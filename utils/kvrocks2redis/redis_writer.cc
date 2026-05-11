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

#include "redis_writer.h"

#include <assert.h>
#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <thread>

#include "io_util.h"
#include "server/redis_reply.h"
#include "thread_util.h"

static constexpr size_t kAofChunkSize = 4 * 1024 * 1024;

RedisWriter::RedisWriter(kvrocks2redis::Config *config) : Writer(config) {
  auto t = util::CreateThread("redis-writer", [this]() {
    this->sync();
    assert(stop_flag_);
  });
  if (!t.IsOK()) {
    ERROR("Failed to create thread: {}", t.Msg());
    return;
  }
  t_ = std::move(*t);
}

RedisWriter::~RedisWriter() {
  for (const auto &iter : next_offset_fds_) {
    close(iter.second);
  }
  for (const auto &iter : redis_fds_) {
    close(iter.second);
  }
}

Status RedisWriter::Write(const std::string &ns, const std::vector<std::string> &aofs) {
  return Writer::Write(ns, aofs);
}

Status RedisWriter::FlushDB(const std::string &ns) {
  GET_OR_RET(Writer::FlushDB(ns));
  GET_OR_RET(updateNextOffset(ns, 0));
  return Write(ns, {redis::ArrayOfBulkStrings({"FLUSHDB"})});
}

void RedisWriter::Stop() {
  if (stop_flag_) return;

  stop_flag_ = true;  // Stopping procedure is asynchronous,

  if (t_.joinable()) t_.join();
  // handled by sync func
  INFO("RedisWriter Stopped");
}

void RedisWriter::sync() {
  for (const auto &iter : config_->tokens) {
    Status s = readNextOffsetFromFile(iter.first, &next_offsets_[iter.first]);
    if (!s.IsOK()) {
      ERROR("{}", s.Msg());
      return;
    }
  }

  auto buffer = std::make_unique<char[]>(kAofChunkSize);
  while (!stop_flag_) {
    for (const auto &iter : config_->tokens) {
      Status s = GetAofFd(iter.first);
      if (!s.IsOK()) {
        ERROR("{}", s.Msg());
        continue;
      }

      s = getRedisConn(iter.first, iter.second.host, iter.second.port, iter.second.auth, iter.second.db_number);
      if (!s.IsOK()) {
        ERROR("{}", s.Msg());
        continue;
      }

      while (true) {
        auto getted_line_leng = pread(aof_fds_[iter.first], buffer.get(), kAofChunkSize, next_offsets_[iter.first]);
        if (getted_line_leng <= 0) {
          if (getted_line_leng < 0) {
            ERROR("failed to read AOF file: {}", strerror(errno));
          }
          break;
        }

        std::string con = std::string(buffer.get(), getted_line_leng);
        s = util::SockSend(redis_fds_[iter.first], con);
        if (!s.IsOK()) {
          ERROR("Failed to send data to redis err: {}", s.Msg());
          break;
        }

        auto line_state = util::SockReadLine(redis_fds_[iter.first]);
        if (!line_state) {
          ERROR("Failed to read redis response err: {}", line_state.Msg());
          break;
        }

        std::string line = *line_state;
        if (line.compare(0, 1, "-") == 0) {
          // Ooops, something went wrong , sync process has been terminated, administrator should be notified
          // when full sync is needed, please remove last_next_seq config file, and restart kvrocks2redis
          ERROR("CRITICAL - redis sync return error, administrator confirm needed: {}", line);
          Stop();
          return;
        }

        s = updateNextOffset(iter.first, next_offsets_[iter.first] + getted_line_leng);
        if (!s.IsOK()) {
          ERROR("Failed to updating next offset: {}", s.Msg());
          break;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
}

Status RedisWriter::getRedisConn(const std::string &ns, const std::string &host, uint32_t port, const std::string &auth,
                                 int db_index) {
  auto iter = redis_fds_.find(ns);
  if (iter == redis_fds_.end()) {
    redis_fds_[ns] = GET_OR_RET(util::SockConnect(host, port).Prefixed("Failed to connect to redis"));

    if (!auth.empty()) {
      auto s = authRedis(ns, auth);
      if (!s.IsOK()) {
        close(redis_fds_[ns]);
        redis_fds_.erase(ns);
        return s;
      }
    }

    if (db_index != 0) {
      auto s = selectDB(ns, db_index);
      if (!s.IsOK()) {
        close(redis_fds_[ns]);
        redis_fds_.erase(ns);
        return s;
      }
    }
  }

  return Status::OK();
}

Status RedisWriter::authRedis(const std::string &ns, const std::string &auth) {
  auto s = util::SockSend(redis_fds_[ns], redis::ArrayOfBulkStrings({"AUTH", auth}));
  if (!s.IsOK()) return s.Prefixed("[kvrocks2redis] failed to send AUTH command");

  std::string line = GET_OR_RET(util::SockReadLine(redis_fds_[ns]).Prefixed("read redis auth response err"));
  if (line.compare(0, 3, "+OK") != 0) {
    return {Status::NotOK, "[kvrocks2redis] redis Auth failed: " + line};
  }
  return Status::OK();
}

Status RedisWriter::selectDB(const std::string &ns, int db_number) {
  auto s = util::SockSend(redis_fds_[ns], redis::ArrayOfBulkStrings({"SELECT", std::to_string(db_number)}));
  if (!s.IsOK()) return s.Prefixed("failed to send SELECT command to socket");

  std::string line = GET_OR_RET(util::SockReadLine(redis_fds_[ns]).Prefixed("read select db response err"));
  if (line.compare(0, 3, "+OK") != 0) {
    return {Status::NotOK, "[kvrocks2redis] redis select db failed: " + line};
  }
  return Status::OK();
}

Status RedisWriter::updateNextOffset(const std::string &ns, std::istream::off_type offset) {
  next_offsets_[ns] = offset;
  return writeNextOffsetToFile(ns, offset);
}

Status RedisWriter::readNextOffsetFromFile(const std::string &ns, std::istream::off_type *offset) {
  next_offset_fds_[ns] = open(getNextOffsetFilePath(ns).data(), O_RDWR | O_CREAT, 0666);
  if (next_offset_fds_[ns] < 0) {
    return Status::FromErrno("Failed to open next offset file");
  }

  *offset = 0;
  // 256 + 1 byte, extra one byte for the ending \0
  char buf[257];
  memset(buf, '\0', sizeof(buf));
  if (read(next_offset_fds_[ns], buf, sizeof(buf)) > 0) {
    *offset = std::stoll(buf);
  }

  return Status::OK();
}

Status RedisWriter::writeNextOffsetToFile(const std::string &ns, std::istream::off_type offset) {
  std::string offset_string = std::to_string(offset);
  // Pad to 256 bytes so subsequent shorter values fully overwrite the previous one.
  offset_string.resize(256, ' ');
  offset_string += '\0';
  return util::Pwrite(next_offset_fds_[ns], offset_string, 0);
}

std::string RedisWriter::getNextOffsetFilePath(const std::string &ns) {
  return config_->output_dir + ns + "_" + config_->next_offset_file_name;
}

