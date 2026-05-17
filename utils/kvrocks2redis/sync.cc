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

#include "sync.h"

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <fcntl.h>
#include <rocksdb/write_batch.h>
#include <unistd.h>

#include <fstream>
#include <string>

#include "event_util.h"
#include "io_util.h"
#include "logging.h"
#include "server/redis_reply.h"

void SendStringToEvent(bufferevent *bev, const std::string &data) {
  auto output = bufferevent_get_output(bev);
  evbuffer_add(output, data.c_str(), data.length());
}

Sync::Sync(engine::Storage *storage, Writer *writer, Parser *parser, kvrocks2redis::Config *config)
    : storage_(storage), writer_(writer), parser_(parser), config_(config) {}

Sync::~Sync() {
  if (next_seq_fd_) close(next_seq_fd_);
  writer_->Stop();
}

/*
 * 1. Attempt to directly parse the wal.
 * 2. If the attempt fails, then it is necessary to parse the current snapshot,
 *    After completion, repeat the steps of the first phase.
 */
void Sync::Start() {
  auto s = readNextSeqFromFile(&next_seq_);
  if (!s.IsOK()) {
    ERROR("{}", s.Msg());
    return;
  }
  INFO("Start sync the data from kvrocks to redis");
  while (!IsStopped()) {
    s = checkWalBoundary();
    if (!s.IsOK()) {
      if (config_->skip_full_sync) {
        s = advanceToNearestWAL();
        if (!s.IsOK()) {
          ERROR("[sync] skip-full-sync: cannot find any WAL to advance to: {}", s.Msg());
          sleep(5);
        }
      } else {
        parseKVFromLocalStorage();
      }
    }
    s = incrementBatchLoop();
    if (!s.IsOK()) {
      if (!s.Msg().empty()) ERROR("[sync] incrementBatchLoop error: {}", s.Msg());
      sleep(2);
    }
  }
}

void Sync::Stop() {
  if (stop_flag_) return;

  stop_flag_ = true;  // Stopping procedure is asynchronous,
  INFO("Sync Stopped");
}

Status Sync::tryCatchUpWithPrimary() {
  auto s = storage_->GetDB()->TryCatchUpWithPrimary();
  return s.ok() ? Status() : Status::NotOK;
}

Status Sync::checkWalBoundary() {
  auto latest = storage_->LatestSeqNumber();
  DEBUG("[checkWAL] next_seq_={}, latest_seq={}", next_seq_, latest);

  if (next_seq_ == latest + 1) {
    DEBUG("[checkWAL] caught up exactly, proceed to incremental");
    return Status::OK();
  }

  // Upper bound: next_seq_ jumped ahead of the secondary — need full re-scan
  if (next_seq_ > latest + 1) {
    WARN("[checkWAL] next_seq_={} > latest+1={}, next_seq_ is ahead of secondary — forcing full re-scan",
         next_seq_, latest + 1);
    return {Status::NotOK};
  }

  // Lower bound: verify WAL still has seq
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  auto s = storage_->GetWALIter(next_seq_, &iter);
  if (s.IsOK() && iter->Valid()) {
    auto batch = iter->GetBatch();
    if (next_seq_ != batch.sequence) {
      if (next_seq_ > batch.sequence) {
        ERROR("[checkWAL] sequence mismatch: next_seq_={}, WAL returned seq={}", next_seq_, batch.sequence);
      } else {
        WARN("[checkWAL] WAL gap: next_seq_={}, first available WAL seq={} — WAL compacted, forcing full re-scan",
             next_seq_, batch.sequence);
      }
      return {Status::NotOK};
    }
    DEBUG("[checkWAL] WAL boundary OK at seq={}", next_seq_);
    return Status::OK();
  }
  WARN("[checkWAL] GetWALIter(seq={}) returned invalid iter (s={}), forcing full re-scan",
       next_seq_, s.IsOK() ? "ok" : s.Msg());
  return {Status::NotOK};
}

Status Sync::advanceToNearestWAL() {
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  // GetUpdatesSince with next_seq_: if WAL was compacted, RocksDB returns the first available entry
  auto gs = storage_->GetDB()->GetUpdatesSince(next_seq_, &iter);
  if (gs.ok() && iter && iter->Valid()) {
    auto batch = iter->GetBatch();
    if (batch.sequence > next_seq_) {
      WARN("[sync] skip-full-sync: WAL gap detected, advancing next_seq_ {} → {} (skipping {} seqs)",
           next_seq_, batch.sequence, batch.sequence - next_seq_);
      return updateNextSeq(batch.sequence);
    }
    // No gap, next_seq_ is already valid
    return Status::OK();
  }
  // Try from seq=0 to find the absolute first available WAL
  gs = storage_->GetDB()->GetUpdatesSince(0, &iter);
  if (gs.ok() && iter && iter->Valid()) {
    auto batch = iter->GetBatch();
    WARN("[sync] skip-full-sync: advancing next_seq_ {} → first available WAL seq={}",
         next_seq_, batch.sequence);
    return updateNextSeq(batch.sequence);
  }
  return {Status::NotOK, "no WAL entries found"};
}

Status Sync::incrementBatchLoop() {
  INFO("[incr] start: next_seq_={}, secondary_latest={}", next_seq_, storage_->LatestSeqNumber());
  uint64_t batch_count = 0, cmd_count = 0;
  auto last_log_time = std::chrono::steady_clock::now();
  auto last_catchup_log = std::chrono::steady_clock::now();
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  while (!IsStopped()) {
    if (!tryCatchUpWithPrimary().IsOK()) {
      WARN("[incr] TryCatchUpWithPrimary failed (next_seq_={}, latest={}), retrying in 1s",
           next_seq_, storage_->LatestSeqNumber());
      sleep(1);
      continue;
    }

    auto latest = storage_->LatestSeqNumber();
    if (next_seq_ <= latest) {
      DEBUG("[incr] behind: next_seq_={}, latest={}, lag={}", next_seq_, latest, latest - next_seq_);
      auto gs = storage_->GetDB()->GetUpdatesSince(next_seq_, &iter);
      if (!gs.ok()) {
        ERROR("[incr] GetUpdatesSince(seq={}) failed: {} — WAL likely compacted, forcing re-scan",
              next_seq_, gs.ToString());
        return {Status::NotOK, fmt::format("GetUpdatesSince failed: {}", gs.ToString())};
      }
      if (!iter->Valid()) {
        WARN("[incr] GetUpdatesSince(seq={}) returned empty iterator (latest={}) — WAL gap?",
             next_seq_, latest);
        return {Status::NotOK, "GetUpdatesSince returned empty iterator"};
      }
      int batch_this_round = 0;
      for (; iter->Valid(); iter->Next()) {
        auto batch = iter->GetBatch();
        if (batch.sequence != next_seq_) {
          if (next_seq_ > batch.sequence) {
            ERROR("[incr] sequence mismatch: expected={}, got={}", next_seq_, batch.sequence);
            return {Status::NotOK};
          } else {
            WARN("[incr] WAL gap: expected seq={}, first batch seq={}", next_seq_, batch.sequence);
            if (config_->skip_full_sync) {
              WARN("[incr] skip-full-sync: jumping next_seq_ {} → {}", next_seq_, batch.sequence);
              if (auto su = updateNextSeq(batch.sequence); !su.IsOK()) return su;
              break;  // re-enter while loop, will process from new next_seq_ on next iteration
            }
            return {Status::NotOK};
          }
        }
        auto s = parser_->ParseWriteBatch(batch.writeBatchPtr->Data());
        if (!s.IsOK()) {
          return s.Prefixed(
              fmt::format("failed to parse write batch '{}'", util::StringToHex(batch.writeBatchPtr->Data())));
        }
        uint64_t cnt = batch.writeBatchPtr->Count();
        s = updateNextSeq(next_seq_ + cnt);
        batch_count++;
        batch_this_round++;
        cmd_count += cnt;
        { auto _now = std::chrono::steady_clock::now(); if (std::chrono::duration_cast<std::chrono::seconds>(_now - last_log_time).count() >= 30) { auto lag = storage_->LatestSeqNumber() > next_seq_ ? storage_->LatestSeqNumber() - next_seq_ : 0; INFO("[incr] next_seq={}, lag={}, batches/30s={}, cmds/30s={}", next_seq_, lag, batch_count, cmd_count); batch_count = 0; cmd_count = 0; last_log_time = _now; } }
        if (!s.IsOK()) {
          return s.Prefixed("failed to update next sequence");
        }
      }
      DEBUG("[incr] processed {} batches this round, next_seq_={}", batch_this_round, next_seq_);
    } else {
      // Caught up: throttle log to every 30s
      { auto _now = std::chrono::steady_clock::now(); if (std::chrono::duration_cast<std::chrono::seconds>(_now - last_catchup_log).count() >= 30) { INFO("[incr] caught up, next_seq_={}, latest={}", next_seq_, latest); batch_count = 0; cmd_count = 0; last_catchup_log = _now; } }
      usleep(10000);
    }
  }
  return Status::OK();
}

void Sync::parseKVFromLocalStorage() {
  INFO("Start parsing kv from the local storage");
  INFO("[parseKV] FlushDB skipped; starting ParseFullDB");
  INFO("[parseKV] secondary latest_seq before scan: {}", storage_->LatestSeqNumber());
  Status s = parser_->ParseFullDB();
  INFO("[parseKV] ParseFullDB returned: {}", s.IsOK() ? "OK" : s.Msg());
  if (s.IsOK()) {
    s = writer_->FlushAll();
    INFO("[parseKV] FlushAll returned: {}", s.IsOK() ? "OK" : s.Msg());
  }
  if (!s.IsOK()) {
    ERROR("Failed to parse full db, encounter error: {} — backing off 5s before retry", s.Msg());
    sleep(5);
    return;
  }
  auto last_seq = storage_->GetDB()->GetLatestSequenceNumber();
  INFO("[parseKV] full scan done, secondary latest_seq={}, will set next_seq_={}", last_seq, last_seq + 1);
  s = updateNextSeq(last_seq + 1);
  if (!s.IsOK()) {
    ERROR("Failed to update next sequence: {}", s.Msg());
  }
}

Status Sync::updateNextSeq(rocksdb::SequenceNumber seq) {
  next_seq_ = seq;
  return writeNextSeqToFile(seq);
}

Status Sync::readNextSeqFromFile(rocksdb::SequenceNumber *seq) {
  next_seq_fd_ = open(config_->next_seq_file_path.data(), O_RDWR | O_CREAT, 0666);
  if (next_seq_fd_ < 0) {
    return {Status::NotOK, std::string("Failed to open next seq file :") + strerror(errno)};
  }

  *seq = 0;
  // 21 + 1 byte, extra one byte for the ending \0
  char buf[22];
  memset(buf, '\0', sizeof(buf));
  if (read(next_seq_fd_, buf, sizeof(buf)) > 0) {
    *seq = static_cast<rocksdb::SequenceNumber>(std::stoull(buf));
  }

  return Status::OK();
}

Status Sync::writeNextSeqToFile(rocksdb::SequenceNumber seq) const {
  std::string seq_string = std::to_string(seq);
  // append to 21 byte (overwrite entire first 21 byte, aka the largest SequenceNumber size )
  int append_byte = 21 - static_cast<int>(seq_string.size());
  while (append_byte-- > 0) {
    seq_string += " ";
  }
  seq_string += '\0';
  return util::Pwrite(next_seq_fd_, seq_string, 0);
}
