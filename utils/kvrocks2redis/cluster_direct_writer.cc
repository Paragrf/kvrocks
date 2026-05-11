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

#include "cluster_direct_writer.h"

#include <string_view>

#include "cluster/redis_slot.h"
#include "common/string_util.h"
#include "io_util.h"
#include "logging.h"
#include "server/redis_reply.h"

static constexpr int kPipelineSize = 128;

ClusterDirectWriter::ClusterDirectWriter(kvrocks2redis::Config *config) : Writer(config) {
  for (const auto &[ns, server] : config_->tokens) {
    if (server.db_number != 0) {
      WARN("cluster: namespace '{}' has db_number={} but Redis Cluster only supports db 0 — db_number ignored",
           ns, server.db_number);
    }
    auto s = topologies_[ns].refresh(server.host, server.port, server.auth);
    if (!s.IsOK()) {
      ERROR("ClusterDirectWriter: init failed for namespace '{}': {}", ns, s.Msg());
    }
  }
}

// Route each complete RESP command in `commands` to its CRC16-assigned cluster
// node, flush once the pipeline limit is reached, then flush all remaining
// buffers before returning.  Returns NotOK on any transport or Redis error;
// the caller (Sync::incrementBatchLoop) will not advance next_seq_ and will
// retry this batch from the same WAL position after restart.
Status ClusterDirectWriter::Write(const std::string &ns, const std::vector<std::string> &commands) {
  std::map<int, NodeBuffer> node_bufs;
  auto &topo = topologies_[ns];

  for (const auto &cmd : commands) {
    std::string_view key;
    size_t consumed = 0;
    auto result = util::ExtractRespKeyForRouting(cmd, &key, &consumed);
    if (result == util::RespParseResult::NeedsMore) {
      return {Status::NotOK, "cluster: incomplete RESP command for namespace " + ns};
    }
    if (result == util::RespParseResult::Error) {
      return {Status::NotOK, "cluster: RESP parse error in command for namespace " + ns};
    }

    if (key.empty()) {
      // FLUSHDB: drain all pending buffers first, then broadcast to all masters.
      for (auto &[node_idx, nb] : node_bufs) {
        GET_OR_RET(flushNodeBuffer(ns, node_idx, nb));
      }
      GET_OR_RET(sendToAllMasters(ns, cmd));
    } else {
      uint16_t slot = GetSlotIdFromKey(key);
      int node_idx = topo.nodeForSlot(slot);
      auto &nb = node_bufs[node_idx];
      nb.buf += cmd;
      nb.count++;
      if (nb.count >= kPipelineSize) {
        GET_OR_RET(flushNodeBuffer(ns, node_idx, nb));
      }
    }
  }

  for (auto &[node_idx, nb] : node_bufs) {
    GET_OR_RET(flushNodeBuffer(ns, node_idx, nb));
  }

  return Status::OK();
}

Status ClusterDirectWriter::Write(const std::string &ns, std::string_view key, const std::string &cmd) {
  if (key.empty()) {
    return sendToAllMasters(ns, cmd);
  }
  uint16_t slot = GetSlotIdFromKey(key);
  int node_idx = topologies_[ns].nodeForSlot(slot);
  NodeBuffer nb{cmd, 1};
  return flushNodeBuffer(ns, node_idx, nb);
}

Status ClusterDirectWriter::FlushDB(const std::string &ns) {
  return sendToAllMasters(ns, redis::ArrayOfBulkStrings({"FLUSHDB"}));
}

// Send all buffered commands to node_idx in one pipeline batch, then drain
// exactly nb.count responses.  On transport failure, refreshes the topology
// so the next restart reconnects with an up-to-date node map.
Status ClusterDirectWriter::flushNodeBuffer(const std::string &ns, int node_idx, NodeBuffer &nb) {
  if (nb.buf.empty()) return Status::OK();

  auto &topo = topologies_[ns];
  const auto &server = config_->tokens.at(ns);

  auto refresh_on_error = [&](const Status &err) -> Status {
    WARN("cluster: transport error on node {} — refreshing topology for next restart: {}", node_idx, err.Msg());
    if (auto rs = topo.refresh(server.host, server.port, server.auth); !rs.IsOK()) {
      WARN("cluster: topology refresh also failed: {}", rs.Msg());
    }
    return err;
  };

  auto conn_s = topo.ensureConnected(node_idx, server.auth);
  if (!conn_s.IsOK()) return refresh_on_error(conn_s.Prefixed("cluster: pipeline connect"));

  int fd = topo.fd(node_idx);
  auto send_s = util::SockSend(fd, nb.buf);
  if (!send_s.IsOK()) {
    topo.closeFd(node_idx);
    return refresh_on_error(send_s.Prefixed("cluster: pipeline send"));
  }

  for (int i = 0; i < nb.count; i++) {
    auto line_or = util::SockReadLine(fd);
    if (!line_or.IsOK()) {
      topo.closeFd(node_idx);
      return refresh_on_error(line_or.ToStatus().Prefixed("cluster: pipeline read"));
    }
    if (!line_or->empty() && (*line_or)[0] == '-') {
      return {Status::NotOK, "cluster: Redis error in pipeline: " + *line_or};
    }
  }

  nb.buf.clear();
  nb.count = 0;
  return Status::OK();
}

Status ClusterDirectWriter::sendToAllMasters(const std::string &ns, const std::string &cmd) {
  Status first_err;
  auto &topo = topologies_[ns];
  const auto &server = config_->tokens.at(ns);

  for (int i = 0; i < static_cast<int>(topo.nodeCount()); i++) {
    auto conn_s = topo.ensureConnected(i, server.auth);
    if (!conn_s.IsOK()) {
      const auto &node = topo.node(i);
      ERROR("cluster: sendToAll connect failed on {}:{} — {}", node.host, node.port, conn_s.Msg());
      if (first_err.IsOK()) first_err = conn_s;
      continue;
    }
    int fd = topo.fd(i);
    auto send_s = util::SockSend(fd, cmd);
    if (!send_s.IsOK()) {
      topo.closeFd(i);
      const auto &node = topo.node(i);
      ERROR("cluster: sendToAll send failed on {}:{} — {}", node.host, node.port, send_s.Msg());
      if (first_err.IsOK()) first_err = send_s;
      continue;
    }
    auto line_or = util::SockReadLine(fd);
    if (!line_or.IsOK()) {
      topo.closeFd(i);
      const auto &node = topo.node(i);
      ERROR("cluster: sendToAll read failed on {}:{} — {}", node.host, node.port, line_or.Msg());
      if (first_err.IsOK()) first_err = line_or.ToStatus();
      continue;
    }
    if (!line_or->empty() && (*line_or)[0] == '-') {
      const auto &node = topo.node(i);
      ERROR("cluster: sendToAll Redis error on {}:{} — {}", node.host, node.port, *line_or);
      if (first_err.IsOK()) first_err = {Status::NotOK, *line_or};
    }
  }
  return first_err;
}
