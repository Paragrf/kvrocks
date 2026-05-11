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
}

Status ClusterTopology::ensureConnected(int node_idx, const std::string &auth) {
  auto it = fds_.find(node_idx);
  if (it != fds_.end() && it->second >= 0) return Status::OK();
  fds_[node_idx] = GET_OR_RET(connectNode(nodes_[node_idx].host, nodes_[node_idx].port, auth));
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

  INFO("cluster: topology refreshed — {} master nodes", nodes_.size());
  return Status::OK();
}

StatusOr<int> ClusterTopology::connectNode(const std::string &host, uint16_t port,
                                            const std::string &auth) {
  int fd = GET_OR_RET(util::SockConnect(host, port).Prefixed("cluster: connect to " + host));
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
  return fd;
}

Status ClusterTopology::parseClusterSlots(int topo_fd, std::vector<Node> &nodes,
                                           std::vector<uint16_t> &slots) {
  GET_OR_RET(util::SockSend(topo_fd, redis::ArrayOfBulkStrings({"CLUSTER", "SLOTS"}))
                 .Prefixed("cluster: CLUSTER SLOTS send"));

  auto outer = GET_OR_RET(util::SockReadLine(topo_fd));
  if (outer.empty() || outer[0] != '*')
    return {Status::NotOK, "cluster: unexpected CLUSTER SLOTS response: " + outer};
  int num_ranges = std::stoi(outer.substr(1));

  std::map<std::string, int> node_index;
  for (int r = 0; r < num_ranges; r++) {
    auto inner_line = GET_OR_RET(util::SockReadLine(topo_fd));  // *M (range array header)
    int inner_count = std::stoi(inner_line.substr(1));

    auto start = GET_OR_RET(readRespInt(topo_fd));
    auto end   = GET_OR_RET(readRespInt(topo_fd));

    GET_OR_RET(util::SockReadLine(topo_fd).ToStatus());  // consume "*3" master sub-array header
    auto ip   = GET_OR_RET(readRespBulkString(topo_fd));
    auto port = GET_OR_RET(readRespInt(topo_fd));
    GET_OR_RET(readRespBulkString(topo_fd).ToStatus());  // node_id, ignored

    std::string key = ip + ":" + std::to_string(port);
    if (!node_index.contains(key)) {
      node_index[key] = static_cast<int>(nodes.size());
      nodes.push_back({ip, static_cast<uint16_t>(port)});
    }
    uint16_t idx = static_cast<uint16_t>(node_index[key]);
    for (int64_t slot = start; slot <= end; slot++) slots[slot] = idx;

    // Skip replica sub-arrays: inner_count fields minus the 3 already consumed
    // (start, end, master).
    GET_OR_RET(skipRespArray(topo_fd, inner_count - 3));
  }
  return Status::OK();
}

StatusOr<int64_t> ClusterTopology::readRespInt(int fd) {
  auto line = GET_OR_RET(util::SockReadLine(fd));
  if (line.empty() || line[0] != ':')
    return {Status::NotOK, "cluster: expected RESP integer, got: " + line};
  return std::stoll(line.substr(1));
}

StatusOr<std::string> ClusterTopology::readRespBulkString(int fd) {
  auto line = GET_OR_RET(util::SockReadLine(fd));
  if (line.empty() || line[0] != '$')
    return {Status::NotOK, "cluster: expected bulk string header, got: " + line};
  int len = std::stoi(line.substr(1));
  if (len < 0) return std::string{};
  return GET_OR_RET(util::SockReadLine(fd));
}

Status ClusterTopology::skipRespValue(int fd) {
  auto line = GET_OR_RET(util::SockReadLine(fd));
  if (line.empty()) return {Status::NotOK, "cluster: empty line from server"};
  switch (line[0]) {
    case '+': case '-': case ':': return Status::OK();
    case '$': {
      int len = std::stoi(line.substr(1));
      if (len >= 0) GET_OR_RET(util::SockReadLine(fd));
      return Status::OK();
    }
    case '*': return skipRespArray(fd, std::stoi(line.substr(1)));
    default:  return {Status::NotOK, "cluster: unknown RESP type: " + line};
  }
}

Status ClusterTopology::skipRespArray(int fd, int count) {
  for (int i = 0; i < count; i++) GET_OR_RET(skipRespValue(fd));
  return Status::OK();
}
