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

#include <map>
#include <string>
#include <vector>

#include "cluster_topology.h"
#include "writer.h"

// Cluster-mode writer that routes RESP commands directly to cluster nodes,
// bypassing the AOF intermediate layer entirely.  Write() is synchronous: it
// routes and flushes within the caller's thread so next_seq_ only advances
// after every command in the batch has been confirmed by the target cluster.
class ClusterDirectWriter : public Writer {
 public:
  explicit ClusterDirectWriter(kvrocks2redis::Config *config);

  ClusterDirectWriter(const ClusterDirectWriter &) = delete;
  ClusterDirectWriter &operator=(const ClusterDirectWriter &) = delete;

  ~ClusterDirectWriter() = default;

  Status Write(const std::string &ns, const std::vector<std::string> &commands) override;
  Status Write(const std::string &ns, std::string_view key, const std::string &cmd) override;
  Status FlushDB(const std::string &ns) override;
  Status FlushAll() override;
  std::unique_ptr<Writer> createSibling() override;

 private:
  struct NodeBuffer {
    std::string buf;
    int count = 0;
  };

  std::map<std::string, ClusterTopology> topologies_;  // ns → topology + connections

  std::map<std::string, std::map<int, NodeBuffer>> write_buf_;
  Status flushNodeBuffer(const std::string &ns, int node_idx, NodeBuffer &nb);
  Status flushPending(const std::string &ns);
  Status sendToAllMasters(const std::string &ns, const std::string &cmd);
};
