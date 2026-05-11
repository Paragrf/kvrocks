/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "cluster_topology.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "common/io_util.h"

// =============================================================================
// FakeServer — minimal loopback TCP server for unit tests.
//
// Accepts exactly one connection per handler in the supplied list, runs each
// handler synchronously in a background thread, then closes the connection.
// Starts listening in the constructor so the port is valid before the caller
// makes any outbound connections.
// =============================================================================
class FakeServer {
 public:
  using Handler = std::function<void(int)>;

  explicit FakeServer(std::vector<Handler> handlers) : handlers_(std::move(handlers)) {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(listen_fd_, 0);
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;  // OS assigns a free port
    EXPECT_EQ(bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)), 0);
    EXPECT_EQ(listen(listen_fd_, static_cast<int>(handlers_.size()) + 1), 0);

    socklen_t len = sizeof(addr);
    getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&addr), &len);
    port_ = ntohs(addr.sin_port);

    thread_ = std::thread([this] { serve(); });
  }

  ~FakeServer() {
    shutdown(listen_fd_, SHUT_RDWR);
    close(listen_fd_);
    if (thread_.joinable()) thread_.join();
  }

  uint16_t port() const { return port_; }

 private:
  void serve() {
    for (auto &handler : handlers_) {
      sockaddr_in client_addr{};
      socklen_t client_len = sizeof(client_addr);
      int conn_fd = accept(listen_fd_, reinterpret_cast<sockaddr *>(&client_addr), &client_len);
      if (conn_fd < 0) break;
      handler(conn_fd);
      close(conn_fd);
    }
  }

  int listen_fd_;
  uint16_t port_;
  std::vector<Handler> handlers_;
  std::thread thread_;
};

// =============================================================================
// Helpers
// =============================================================================

// Discard n CRLF-terminated RESP lines received from fd.
static void drainLines(int fd, int n) {
  for (int i = 0; i < n; i++) util::SockReadLine(fd);
}

// Build a minimal CLUSTER SLOTS RESP response.
// Each node is reported at 127.0.0.1:<ports[i]> and covers an equal slice of
// the 16384 slots (last node gets any remainder).
static std::string MakeSlotsResp(const std::vector<uint16_t> &ports) {
  int n = static_cast<int>(ports.size());
  std::string resp = "*" + std::to_string(n) + "\r\n";
  int slots_per = 16384 / n;
  for (int i = 0; i < n; i++) {
    int start = i * slots_per;
    int end = (i == n - 1) ? 16383 : start + slots_per - 1;
    resp += "*3\r\n";  // range array: start, end, master (no replicas)
    resp += ":" + std::to_string(start) + "\r\n";
    resp += ":" + std::to_string(end) + "\r\n";
    resp += "*3\r\n";  // master sub-array: ip, port, node-id
    resp += "$9\r\n127.0.0.1\r\n";
    resp += ":" + std::to_string(ports[i]) + "\r\n";
    resp += "$2\r\nn" + std::to_string(i) + "\r\n";
  }
  return resp;
}

// Handler: drains the incoming CLUSTER SLOTS command, writes back the response.
// A CLUSTER SLOTS command is *2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n (5 lines).
static FakeServer::Handler SlotsHandler(std::string resp) {
  return [r = std::move(resp)](int fd) {
    drainLines(fd, 5);  // *2 $7 CLUSTER $5 SLOTS
    util::SockSend(fd, r);
  };
}

// Handler: drains AUTH (*2\r\n$4\r\nAUTH\r\n$N\r\n<pw>\r\n = 5 lines), replies
// +OK, then drains CLUSTER SLOTS and writes the response.
static FakeServer::Handler AuthSlotsHandler(std::string resp) {
  return [r = std::move(resp)](int fd) {
    drainLines(fd, 5);  // AUTH command
    util::SockSend(fd, "+OK\r\n");
    drainLines(fd, 5);  // CLUSTER SLOTS command
    util::SockSend(fd, r);
  };
}

// Handler: accept and immediately return (simulates an idle cluster node).
static FakeServer::Handler NodeHandler() { return [](int) {}; }

// Handler: accept, drain AUTH, reply +OK (for authenticated node connections).
static FakeServer::Handler AuthNodeHandler() {
  return [](int fd) {
    drainLines(fd, 5);
    util::SockSend(fd, "+OK\r\n");
  };
}

// =============================================================================
// Tests
// =============================================================================

TEST(ClusterTopology, TwoNodeSlotRouting) {
  FakeServer topo({SlotsHandler(MakeSlotsResp({7000, 7001}))});

  ClusterTopology obj;
  ASSERT_TRUE(obj.refresh("127.0.0.1", topo.port(), "").IsOK());

  EXPECT_EQ(obj.nodeCount(), 2U);
  // MakeSlotsResp splits 16384 evenly: node 0 → [0, 8191], node 1 → [8192, 16383]
  EXPECT_EQ(obj.nodeForSlot(0), 0);
  EXPECT_EQ(obj.nodeForSlot(8191), 0);
  EXPECT_EQ(obj.nodeForSlot(8192), 1);
  EXPECT_EQ(obj.nodeForSlot(16383), 1);
}

TEST(ClusterTopology, SingleNodeCoversAllSlots) {
  FakeServer topo({SlotsHandler(MakeSlotsResp({7000}))});

  ClusterTopology obj;
  ASSERT_TRUE(obj.refresh("127.0.0.1", topo.port(), "").IsOK());

  EXPECT_EQ(obj.nodeCount(), 1U);
  EXPECT_EQ(obj.nodeForSlot(0), 0);
  EXPECT_EQ(obj.nodeForSlot(16383), 0);
}

TEST(ClusterTopology, NodeInfoPreserved) {
  FakeServer topo({SlotsHandler(MakeSlotsResp({7000, 7001}))});

  ClusterTopology obj;
  ASSERT_TRUE(obj.refresh("127.0.0.1", topo.port(), "").IsOK());

  EXPECT_EQ(obj.node(0).host, "127.0.0.1");
  EXPECT_EQ(obj.node(0).port, 7000);
  EXPECT_EQ(obj.node(1).host, "127.0.0.1");
  EXPECT_EQ(obj.node(1).port, 7001);
}

TEST(ClusterTopology, AuthenticatedRefresh) {
  FakeServer topo({AuthSlotsHandler(MakeSlotsResp({7000}))});

  ClusterTopology obj;
  ASSERT_TRUE(obj.refresh("127.0.0.1", topo.port(), "secret").IsOK());
  EXPECT_EQ(obj.nodeCount(), 1U);
}

TEST(ClusterTopology, ReplicasAreIgnored) {
  // inner array count = 4: start, end, master, 1 replica — replica must be skipped
  std::string resp =
      "*1\r\n"
      "*4\r\n"
      ":0\r\n"
      ":16383\r\n"
      "*3\r\n"             // master sub-array
      "$9\r\n127.0.0.1\r\n"
      ":7000\r\n"
      "$2\r\nn0\r\n"
      "*3\r\n"             // replica sub-array — must be consumed and discarded
      "$9\r\n127.0.0.1\r\n"
      ":7001\r\n"
      "$2\r\nn1\r\n";

  FakeServer topo({SlotsHandler(resp)});
  ClusterTopology obj;
  ASSERT_TRUE(obj.refresh("127.0.0.1", topo.port(), "").IsOK());

  EXPECT_EQ(obj.nodeCount(), 1U);          // only the master
  EXPECT_EQ(obj.node(0).port, 7000);       // master port, not replica
}

TEST(ClusterTopology, FdNegativeBeforeConnect) {
  FakeServer topo({SlotsHandler(MakeSlotsResp({7000}))});

  ClusterTopology obj;
  ASSERT_TRUE(obj.refresh("127.0.0.1", topo.port(), "").IsOK());

  EXPECT_EQ(obj.fd(0), -1);  // no connection opened yet
}

TEST(ClusterTopology, EnsureConnectedThenCloseFd) {
  FakeServer node({NodeHandler()});
  FakeServer topo({SlotsHandler(MakeSlotsResp({node.port()}))});

  ClusterTopology obj;
  ASSERT_TRUE(obj.refresh("127.0.0.1", topo.port(), "").IsOK());

  ASSERT_TRUE(obj.ensureConnected(0, "").IsOK());
  EXPECT_GE(obj.fd(0), 0);

  obj.closeFd(0);
  EXPECT_EQ(obj.fd(0), -1);
}

TEST(ClusterTopology, EnsureConnectedIsIdempotent) {
  // Only one accept on the node server — a second ensureConnected must reuse
  // the existing fd and not open a new connection.
  FakeServer node({NodeHandler()});
  FakeServer topo({SlotsHandler(MakeSlotsResp({node.port()}))});

  ClusterTopology obj;
  ASSERT_TRUE(obj.refresh("127.0.0.1", topo.port(), "").IsOK());

  ASSERT_TRUE(obj.ensureConnected(0, "").IsOK());
  int first_fd = obj.fd(0);
  ASSERT_GE(first_fd, 0);

  ASSERT_TRUE(obj.ensureConnected(0, "").IsOK());  // already open → no new connect
  EXPECT_EQ(obj.fd(0), first_fd);
}

TEST(ClusterTopology, FdMigratedAcrossRefresh) {
  // After a topology refresh where a node's host:port is unchanged, the
  // existing open fd must be carried over rather than closed and reopened.
  FakeServer node({NodeHandler()});
  std::string slots = MakeSlotsResp({node.port()});
  FakeServer topo1({SlotsHandler(slots)});
  FakeServer topo2({SlotsHandler(slots)});

  ClusterTopology obj;
  ASSERT_TRUE(obj.refresh("127.0.0.1", topo1.port(), "").IsOK());
  ASSERT_TRUE(obj.ensureConnected(0, "").IsOK());
  int old_fd = obj.fd(0);
  EXPECT_GE(old_fd, 0);

  // Same node host:port appears in the refreshed topology → fd migrated
  ASSERT_TRUE(obj.refresh("127.0.0.1", topo2.port(), "").IsOK());
  EXPECT_EQ(obj.fd(0), old_fd);
}

TEST(ClusterTopology, AuthenticatedNodeConnect) {
  FakeServer node({AuthNodeHandler()});
  FakeServer topo({SlotsHandler(MakeSlotsResp({node.port()}))});

  ClusterTopology obj;
  ASSERT_TRUE(obj.refresh("127.0.0.1", topo.port(), "").IsOK());

  ASSERT_TRUE(obj.ensureConnected(0, "secret").IsOK());
  EXPECT_GE(obj.fd(0), 0);
}
