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

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "common/status.h"

// Manages Redis Cluster topology for one namespace: node list, 16384-entry
// slot→node mapping, and live TCP connections to each master node.
class ClusterTopology {
 public:
  struct Node {
    std::string host;
    uint16_t port = 0;
  };

  ClusterTopology() = default;
  ~ClusterTopology();

  ClusterTopology(const ClusterTopology &) = delete;
  ClusterTopology &operator=(const ClusterTopology &) = delete;

  // Connect to entry_host:entry_port, issue CLUSTER SLOTS, and build the slot
  // map.  Migrates any live connections that survive across the topology change.
  Status refresh(const std::string &entry_host, uint16_t entry_port, const std::string &auth);

  // Open a connection to node_idx if not already open.
  Status ensureConnected(int node_idx, const std::string &auth);

  // Close and invalidate the connection to node_idx.
  void closeFd(int node_idx);

  // Returns the socket fd for node_idx, or -1 if not connected.
  int fd(int node_idx) const;

  // Returns the node index responsible for the given slot (0–16383).
  int nodeForSlot(uint16_t slot) const { return slot_nodes_[slot]; }

  size_t nodeCount() const { return nodes_.size(); }
  const Node &node(int node_idx) const { return nodes_[node_idx]; }

 private:
  std::vector<Node> nodes_;
  std::vector<uint16_t> slot_nodes_;  // 16384-element slot→node-index table
  std::map<int, int> fds_;            // node_index → socket fd

  // Open a TCP connection to host:port and run AUTH if auth is non-empty.
  static StatusOr<int> connectNode(const std::string &host, uint16_t port,
                                   const std::string &auth);

  // Send CLUSTER SLOTS on an already-open fd and parse the response into
  // nodes/slots.  The caller owns topo_fd and must close it afterwards.
  static Status parseClusterSlots(int topo_fd, std::vector<Node> &nodes,
                                  std::vector<uint16_t> &slots);

  // Typed RESP line readers used only inside parseClusterSlots.
  static StatusOr<int64_t> readRespInt(int fd);
  static StatusOr<std::string> readRespBulkString(int fd);
  static Status skipRespValue(int fd);
  static Status skipRespArray(int fd, int count);
};
