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

#include <cerrno>
#include <cstring>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "io_util.h"
#include "logging.h"
#include "server/redis_reply.h"

ClusterTopology::~ClusterTopology() {
  for (auto &[idx, fd] : fds_) {
    if (fd >= 0) close(fd);
  }
}

int ClusterTopology::fd(int node_idx) const {
  auto it = fds_.find(node_idx);
  return (it != fds_.end()) ? it->second : -1;
}

void ClusterTopology::closeFd(int node_idx) {
  auto it = fds_.find(node_idx);
  if (it != fds_.end() && it->second >= 0) {
    close(it->second);
    it->second = -1;
  }
  read_bufs_.erase(node_idx);
}

Status ClusterTopology::ensureConnected(int node_idx, const std::string &auth) {
  auto it = fds_.find(node_idx);
  if (it != fds_.end() && it->second >= 0) return Status::OK();
  fds_[node_idx] = GET_OR_RET(connectNode(nodes_[node_idx].host, nodes_[node_idx].port, auth));
  return Status::OK();
}
StatusOr<std::string> ClusterTopology::readLineFromNode(int node_idx) {
  int fd = this->fd(node_idx);
  if (fd < 0) return {Status::NotOK, "readLineFromNode: node not connected"};
  auto &rb = read_bufs_[node_idx];
  while (true) {
    auto pos = rb.data.find("\r\n", rb.offset);
    if (pos != std::string::npos) {
      std::string line = rb.data.substr(rb.offset, pos - rb.offset);
      rb.offset = pos + 2;
      // Compact when consumed more than half — avoids O(n²) erase-per-line at large pipeline sizes.
      if (rb.offset > rb.data.size() / 2) {
        rb.data.erase(0, rb.offset);
        rb.offset = 0;
      }
      return line;
    }
    // Compact before reading more so the incoming data lands at the front.
    if (rb.offset > 0) {
      rb.data.erase(0, rb.offset);
      rb.offset = 0;
    }
    // Use select() with 10-second timeout so a hung server surfaces as an error
    // instead of blocking indefinitely.
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);
    struct timeval tv{10, 0};
    int sel = select(fd + 1, &rfds, nullptr, nullptr, &tv);
    if (sel == 0) {
      return {Status::NotOK, fmt::format(
          "readLineFromNode: 10s timeout waiting for data from node {} fd={} buf_remaining={}B",
          node_idx, fd, rb.data.size() - rb.offset)};
    }
    if (sel < 0) return {Status::NotOK, std::string("select: ") + strerror(errno)};
    char tmp[65536];  // large buffer: drain many pipelined responses in one syscall
    ssize_t n = read(fd, tmp, sizeof(tmp));
    if (n <= 0) return {Status::NotOK, fmt::format(
        "readLineFromNode: read returned {} ({}), node={} fd={}",
        n, strerror(errno), node_idx, fd)};
    rb.data.append(tmp, static_cast<size_t>(n));
  }
}


Status ClusterTopology::drainResponses(int node_idx, int count) {
  if (count == 0) return Status::OK();
  int fd = this->fd(node_idx);
  if (fd < 0) return {Status::NotOK, "drainResponses: node not connected"};
  auto &rb = read_bufs_[node_idx];
  int remaining = count;

  while (remaining > 0) {
    size_t line_start = rb.offset;
    size_t pos = rb.offset;

    // Single-pass scan: find each '\n', check for error, decrement remaining.
    while (pos < rb.data.size() && remaining > 0) {
      if (rb.data[pos] == '\n') {
        if (line_start < rb.data.size() && rb.data[line_start] == '-') {
          size_t end = (pos > 0 && rb.data[pos - 1] == '\r') ? pos - 1 : pos;
          std::string err = rb.data.substr(line_start, end - line_start);
          rb.offset = pos + 1;
          return {Status::NotOK, "cluster: Redis error in pipeline: " + err};
        }
        remaining--;
        rb.offset = pos + 1;
        line_start = rb.offset;
        pos = rb.offset;
      } else {
        pos++;
      }
    }

    if (remaining > 0) {
      // Compact before reading more data.
      if (rb.offset > 0) {
        rb.data.erase(0, rb.offset);
        rb.offset = 0;
      }
      fd_set rfds;
      FD_ZERO(&rfds);
      FD_SET(fd, &rfds);
      struct timeval tv{10, 0};
      int sel = select(fd + 1, &rfds, nullptr, nullptr, &tv);
      if (sel == 0) {
        return {Status::NotOK,
                fmt::format("drainResponses: 10s timeout, node={} remaining={}", node_idx, remaining)};
      }
      if (sel < 0) return {Status::NotOK, std::string("select: ") + strerror(errno)};
      char tmp[65536];
      ssize_t n = read(fd, tmp, sizeof(tmp));
      if (n <= 0) {
        return {Status::NotOK,
                fmt::format("drainResponses: read returned {} ({}), node={}", n, strerror(errno), node_idx)};
      }
      rb.data.append(tmp, static_cast<size_t>(n));
    }
  }

  if (rb.offset > rb.data.size() / 2) {
    rb.data.erase(0, rb.offset);
    rb.offset = 0;
  }
  return Status::OK();
}

Status ClusterTopology::refresh(const std::string &entry_host, uint16_t entry_port,
                                 const std::string &auth) {
  int topo_fd = GET_OR_RET(connectNode(entry_host, entry_port, auth));

  std::vector<Node> new_nodes;
  std::vector<uint16_t> new_slots(16384, 0);
  auto s = parseClusterSlots(topo_fd, new_nodes, new_slots);
  close(topo_fd);
  if (!s.IsOK()) return s;

  // Build a host:port → new-index lookup for fd migration.
  std::map<std::string, int> new_index;
  for (int i = 0; i < static_cast<int>(new_nodes.size()); i++) {
    new_index[new_nodes[i].host + ":" + std::to_string(new_nodes[i].port)] = i;
  }

  // Migrate live fds whose host:port still exists in the new topology.
  // Use a separate map so the cleanup loop below does not close migrated fds.
  std::map<int, int> new_fds;
  for (int i = 0; i < static_cast<int>(nodes_.size()); i++) {
    auto fit = fds_.find(i);
    if (fit == fds_.end() || fit->second < 0) continue;
    std::string key = nodes_[i].host + ":" + std::to_string(nodes_[i].port);
    auto it = new_index.find(key);
    if (it != new_index.end()) {
      new_fds[it->second] = fit->second;
      fit->second = -1;  // prevent double-close below
    }
  }
  for (auto &[idx, fd] : fds_) {
    if (fd >= 0) close(fd);
  }

  nodes_     = std::move(new_nodes);
  slot_nodes_ = std::move(new_slots);
  fds_       = std::move(new_fds);
  read_bufs_.clear();

  INFO("cluster: topology refreshed — {} master nodes", nodes_.size());
  return Status::OK();
}

StatusOr<int> ClusterTopology::connectNode(const std::string &host, uint16_t port,
                                            const std::string &auth) {
  INFO("[topo] SockConnect -> {}:{} ...", host, port);
  int fd = GET_OR_RET(util::SockConnect(host, port).Prefixed("cluster: connect to " + host));
  INFO("[topo] SockConnect -> {}:{} OK fd={}", host, port, fd);
  // Disable Nagle's algorithm: send pipeline data immediately without waiting to coalesce.
  int flag = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
  // Increase socket send/receive buffers to 4 MB to handle large pipeline bursts.
  int bufsize = 4 * 1024 * 1024;
  setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
  setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
  if (!auth.empty()) {
    auto s = util::SockSend(fd, redis::ArrayOfBulkStrings({"AUTH", auth}));
    if (!s.IsOK()) { close(fd); return s.Prefixed("cluster: AUTH send"); }
    auto line = util::SockReadLine(fd);
    if (!line.IsOK()) { close(fd); return line.ToStatus().Prefixed("cluster: AUTH read"); }
    if (line->compare(0, 3, "+OK") != 0) {
      close(fd);
      return {Status::NotOK, "cluster: AUTH failed: " + *line};
    }
  }
  INFO("[topo] AUTH OK for {}:{} fd={}", host, port, fd);
  return fd;
}


struct LineReader {
  int fd_;
  std::string buf_;
  explicit LineReader(int fd) : fd_(fd) {}
  StatusOr<std::string> readLine() {
    while (true) {
      auto pos = buf_.find("\r\n");
      if (pos != std::string::npos) {
        std::string line = buf_.substr(0, pos);
        buf_.erase(0, pos + 2);
        return line;
      }
      char tmp[4096];
      ssize_t n = read(fd_, tmp, sizeof(tmp));
      if (n <= 0) return {Status::NotOK, std::string("read error: ") + strerror(errno)};
      buf_.append(tmp, static_cast<size_t>(n));
    }
  }
};

static StatusOr<int64_t> readRespInt(LineReader &r) {
  auto line = GET_OR_RET(r.readLine());
  if (line.empty() || line[0] != ':')
    return {Status::NotOK, "cluster: expected RESP integer, got: " + line};
  return std::stoll(line.substr(1));
}

static StatusOr<std::string> readRespBulkString(LineReader &r) {
  auto line = GET_OR_RET(r.readLine());
  if (line.empty() || line[0] != '$')
    return {Status::NotOK, "cluster: expected bulk string header, got: " + line};
  int len = std::stoi(line.substr(1));
  if (len < 0) return std::string{};
  return GET_OR_RET(r.readLine());
}

static Status skipRespValue(LineReader &r);

static Status skipRespArray(LineReader &r, int count) {
  for (int i = 0; i < count; i++) GET_OR_RET(skipRespValue(r));
  return Status::OK();
}

static Status skipRespValue(LineReader &r) {
  auto line = GET_OR_RET(r.readLine());
  if (line.empty()) return {Status::NotOK, "cluster: empty line from server"};
  switch (line[0]) {
    case '+': case '-': case ':': return Status::OK();
    case '$': {
      int len = std::stoi(line.substr(1));
      if (len >= 0) GET_OR_RET(r.readLine());
      return Status::OK();
    }
    case '*': return skipRespArray(r, std::stoi(line.substr(1)));
    default:  return {Status::NotOK, "cluster: unknown RESP type: " + line};
  }
}

Status ClusterTopology::parseClusterSlots(int topo_fd, std::vector<Node> &nodes,
                                           std::vector<uint16_t> &slots) {
  GET_OR_RET(util::SockSend(topo_fd, redis::ArrayOfBulkStrings({"CLUSTER", "SLOTS"}))
                 .Prefixed("cluster: CLUSTER SLOTS send"));
  INFO("[topo] CLUSTER SLOTS sent, reading response...");

  LineReader reader(topo_fd);
  auto outer = GET_OR_RET(reader.readLine());
  if (outer.empty() || outer[0] != '*')
    return {Status::NotOK, "cluster: unexpected CLUSTER SLOTS response: " + outer};
  int num_ranges = std::stoi(outer.substr(1));
  INFO("[topo] CLUSTER SLOTS: outer=[{}] num_ranges={}", outer, num_ranges);

  std::map<std::string, int> node_index;
  for (int r = 0; r < num_ranges; r++) {
    auto inner_line = GET_OR_RET(reader.readLine());  // *M (range array header)
    int inner_count = std::stoi(inner_line.substr(1));
    INFO("[topo] range[{}] raw=[{}] inner_count={}", r, inner_line, inner_count);

    auto start = GET_OR_RET(readRespInt(reader));
    auto end   = GET_OR_RET(readRespInt(reader));

    GET_OR_RET(reader.readLine().ToStatus());  // consume "*3" master sub-array header
    auto ip   = GET_OR_RET(readRespBulkString(reader));
    auto port = GET_OR_RET(readRespInt(reader));
    GET_OR_RET(readRespBulkString(reader).ToStatus());  // node_id, ignored

    std::string key = ip + ":" + std::to_string(port);
    if (!node_index.contains(key)) {
      node_index[key] = static_cast<int>(nodes.size());
      nodes.push_back({ip, static_cast<uint16_t>(port)});
    }
    uint16_t idx = static_cast<uint16_t>(node_index[key]);
    for (int64_t slot = start; slot <= end; slot++) slots[slot] = idx;

    // Skip replica sub-arrays: inner_count fields minus the 3 already consumed
    // (start, end, master).
    GET_OR_RET(skipRespArray(reader, inner_count - 3));
    INFO("[topo] range[{}] done, ip={}", r, ip);
  }
  return Status::OK();
}


