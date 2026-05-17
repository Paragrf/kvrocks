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

#include "parser.h"

#include <rocksdb/write_batch.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>

#include "cluster/redis_slot.h"
#include "db_util.h"
#include "logging.h"
#include "server/redis_reply.h"
#include "storage/redis_metadata.h"
#include "types/redis_string.h"

Status Parser::ParseFullDB() {
  rocksdb::DB *db = storage_->GetDB();
  rocksdb::ColumnFamilyHandle *metadata_cf_handle = storage_->GetCFHandle(ColumnFamilyID::Metadata);

  // Work item: one metadata-CF entry to be processed by a worker.
  struct WorkItem {
    std::string ns_key;
    std::string raw_value;  // full RocksDB value bytes (needed for string type)
    Metadata metadata{kRedisNone};  // must be explicitly initialized; Metadata has no default ctor
  };

  // Try to spin up parallel sibling writers.
  // Each worker gets its own ClusterDirectWriter so TCP connections are independent.
  const int kNumWorkers = (writer_->getConfig() && writer_->getConfig()->parse_workers > 0)
                              ? writer_->getConfig()->parse_workers
                              : 2;
  std::vector<std::unique_ptr<Writer>> siblings;
  for (int i = 0; i < kNumWorkers; i++) {
    auto s = writer_->createSibling();
    if (!s) { siblings.clear(); break; }
    siblings.push_back(std::move(s));
  }
  const bool parallel = !siblings.empty();
  INFO("[parseKV] ParseFullDB: {} workers (parallel={})", parallel ? kNumWorkers : 1, parallel);

  if (!parallel) {
    // Fallback: original single-threaded path.
    rocksdb::ReadOptions ro;
    ro.fill_cache = false;
    ro.readahead_size = 8 * 1024 * 1024;
    std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(ro, metadata_cf_handle));
    uint64_t key_count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      Metadata metadata(kRedisNone);
      if (!metadata.Decode(iter->value()).ok()) continue;
      if (metadata.Expired()) continue;
      key_count++;
      if (key_count % 10000 == 0)
        INFO("[parseKV] progress: {} keys processed", key_count);
      Status s = (metadata.Type() == kRedisString)
                     ? parseSimpleKV(iter->key(), iter->value(), metadata.expire)
                     : parseComplexKV(iter->key(), metadata);
      if (!s.IsOK()) return s;
    }
    INFO("[parseKV] ParseFullDB complete: {} keys (single-threaded)", key_count);
    return Status::OK();
  }

  // --- Parallel path ---
  const size_t kQueueDepth = 512;
  std::deque<WorkItem> queue;
  std::mutex queue_mu;
  std::condition_variable queue_cv;
  bool producer_done = false;
  std::atomic<bool> has_error{false};
  Status first_error;
  std::mutex error_mu;

  // Launch N worker threads, each with its own sibling writer.
  std::vector<std::thread> workers;
  workers.reserve(kNumWorkers);
  for (int i = 0; i < kNumWorkers; i++) {
    workers.emplace_back([&, i, sib = siblings[i].get()]() {
      Parser sub(storage_, sib);
      while (true) {
        WorkItem item;
        {
          std::unique_lock<std::mutex> lk(queue_mu);
          queue_cv.wait(lk, [&] { return !queue.empty() || producer_done || has_error.load(); });
          if (has_error.load() || (queue.empty() && producer_done)) return;
          item = std::move(queue.front());
          queue.pop_front();
        }
        queue_cv.notify_one();  // wake producer: a slot freed up

        rocksdb::Slice ns_key_slice(item.ns_key);
        Status s = (item.metadata.Type() == kRedisString)
                       ? sub.parseSimpleKV(ns_key_slice, rocksdb::Slice(item.raw_value), item.metadata.expire)
                       : sub.parseComplexKV(ns_key_slice, item.metadata);
        if (!s.IsOK()) {
          {
            std::lock_guard<std::mutex> lk(error_mu);
            if (first_error.IsOK()) first_error = s;
          }
          has_error.store(true);
          queue_cv.notify_all();
          return;
        }
      }
    });
  }

  // Producer: scan metadata CF and push work items.
  rocksdb::ReadOptions ro;
  ro.fill_cache = false;
  ro.readahead_size = 8 * 1024 * 1024;
  std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(ro, metadata_cf_handle));
  uint64_t key_count = 0;

  for (iter->SeekToFirst(); iter->Valid() && !has_error.load(); iter->Next()) {
    Metadata metadata(kRedisNone);
    if (!metadata.Decode(iter->value()).ok()) continue;
    if (metadata.Expired()) continue;

    key_count++;
    if (key_count % 50000 == 0)
      INFO("[parseKV] progress: {} keys enqueued", key_count);

    WorkItem item;
    item.ns_key    = iter->key().ToString();
    item.raw_value = iter->value().ToString();
    item.metadata  = metadata;

    {
      std::unique_lock<std::mutex> lk(queue_mu);
      // Back-pressure: block when queue is full so we don't OOM.
      queue_cv.wait(lk, [&] { return queue.size() < kQueueDepth || has_error.load(); });
      if (has_error.load()) break;
      queue.push_back(std::move(item));
    }
    queue_cv.notify_one();
  }

  {
    std::lock_guard<std::mutex> lk(queue_mu);
    producer_done = true;
  }
  queue_cv.notify_all();
  for (auto &w : workers) w.join();

  if (has_error.load()) {
    std::lock_guard<std::mutex> lk(error_mu);
    return first_error;
  }

  // Flush each sibling's pending pipeline buffers.
  for (auto &sib : siblings) {
    GET_OR_RET(sib->FlushAll());
  }

  INFO("[parseKV] ParseFullDB complete: {} keys, {} workers", key_count, kNumWorkers);
  return Status::OK();
}

Status Parser::parseSimpleKV(const Slice &ns_key, const Slice &value, uint64_t expire) {
  auto [ns, user_key] = ExtractNamespaceKey<std::string>(ns_key, slot_id_encoded_);

  auto command =
      redis::ArrayOfBulkStrings({"SET", user_key, value.ToString().substr(Metadata::GetOffsetAfterExpire(value[0]))});
  DEBUG("[parseKV] SET key='{}' ({}B)", user_key, value.size());
  Status s = writer_->Write(ns, user_key, command);
  if (!s.IsOK()) return s.Prefixed(fmt::format("SET key='{}'", user_key));

  if (expire > 0) {
    command = redis::ArrayOfBulkStrings({"EXPIREAT", user_key, std::to_string(expire / 1000)});
    DEBUG("[parseKV] EXPIREAT key='{}'", user_key);
    s = writer_->Write(ns, user_key, command);
  }

  return s;
}

// Maximum sub-keys packed into one batched RESP command (HSET/SADD/RPUSH/ZADD).
// Each batch becomes one pipeline slot; larger batches cut round-trips dramatically
// for wide hashes/lists/sets while staying within Redis's 512MB payload limit.
static constexpr int kSubKeyBatch = 256;

Status Parser::parseComplexKV(const Slice &ns_key, const Metadata &metadata) {
  RedisType type = metadata.Type();
  if (type < kRedisHash || type > kRedisSortedint) {
    return {Status::NotOK, "unknown metadata type: " + std::to_string(type)};
  }

  auto [ns, user_key] = ExtractNamespaceKey<std::string>(ns_key, slot_id_encoded_);
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, slot_id_encoded_).Encode();
  std::string next_version_prefix_key = InternalKey(ns_key, "", metadata.version + 1, slot_id_encoded_).Encode();

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.readahead_size = 8 * 1024 * 1024;
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;

  auto no_txn_ctx = engine::Context::NoTransactionContext(storage_);
  auto iter = util::UniqueIterator(no_txn_ctx, read_options);

  // Determine the command name for batching.
  std::string cmd_name;
  switch (type) {
    case kRedisHash:      cmd_name = "HSET";  break;
    case kRedisSet:       cmd_name = "SADD";  break;
    case kRedisList:      cmd_name = "RPUSH"; break;
    case kRedisZSet:      cmd_name = "ZADD";  break;
    case kRedisSortedint: cmd_name = "ZADD";  break;
    default: break;
  }

  // Accumulator for the current batch: [cmd, key, arg, arg, ...]
  std::vector<std::string> args;
  int item_count = 0;

  auto flush_batch = [&]() -> Status {
    if (args.empty()) return Status::OK();
    auto s = writer_->Write(ns, user_key, redis::ArrayOfBulkStrings(args));
    args.clear();
    item_count = 0;
    return s;
  };

  for (iter->Seek(prefix_key); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(prefix_key)) break;

    InternalKey ikey(iter->key(), slot_id_encoded_);
    std::string sub_key = ikey.GetSubKey().ToString();
    std::string value = iter->value().ToString();

    // Bitmap has its own per-segment logic — handle separately.
    if (type == kRedisBitmap) {
      auto s = Parser::parseBitmapSegment(ns, user_key, std::stoi(sub_key), value);
      if (!s.IsOK()) return s.Prefixed("failed to parse bitmap segment");
      continue;
    }

    // Start a new batch vector if needed.
    if (args.empty()) {
      args.push_back(cmd_name);
      args.push_back(user_key);
    }

    // Append sub-key data to the current batch.
    switch (type) {
      case kRedisHash:
        args.push_back(sub_key);
        args.push_back(value);
        break;
      case kRedisSet:
        args.push_back(sub_key);
        break;
      case kRedisList:
        args.push_back(value);
        break;
      case kRedisZSet: {
        double score = DecodeDouble(value.data());
        args.push_back(util::Float2String(score));
        args.push_back(sub_key);
        break;
      }
      case kRedisSortedint: {
        std::string val = std::to_string(DecodeFixed64(ikey.GetSubKey().data()));
        args.push_back(val);  // score == member for sortedint
        args.push_back(val);
        break;
      }
      default: break;
    }

    if (++item_count >= kSubKeyBatch) {
      GET_OR_RET(flush_batch());
    }
  }

  GET_OR_RET(flush_batch());

  if (metadata.expire > 0) {
    auto output = redis::ArrayOfBulkStrings({"EXPIREAT", user_key, std::to_string(metadata.expire / 1000)});
    GET_OR_RET(writer_->Write(ns, user_key, output).Prefixed("failed to write the EXPIREAT command to AOF"));
  }

  return Status::OK();
}

Status Parser::parseBitmapSegment(const Slice &ns, const Slice &user_key, int index, const Slice &bitmap) {
  Status s;
  for (size_t i = 0; i < bitmap.size(); i++) {
    if (bitmap[i] == 0) continue;  // ignore zero byte

    for (int j = 0; j < 8; j++) {
      if (!(bitmap[i] & (1 << j))) continue;  // ignore zero bit

      s = writer_->Write(
          ns.ToString(), user_key.ToString(),
          redis::ArrayOfBulkStrings({"SETBIT", user_key.ToString(), std::to_string(index * 8 + i * 8 + j), "1"}));
      if (!s.IsOK()) return s.Prefixed("failed to write SETBIT command to AOF");
    }
  }
  return Status::OK();
}

Status Parser::ParseWriteBatch(const std::string &batch_string) {
  rocksdb::WriteBatch write_batch(batch_string);
  WriteBatchExtractor write_batch_extractor(slot_id_encoded_, -1, true);

  auto db_status = write_batch.Iterate(&write_batch_extractor);
  if (!db_status.ok())
    return {Status::NotOK, fmt::format("failed to iterate over the write batch: {}", db_status.ToString())};

  auto resp_commands = write_batch_extractor.GetRESPCommands();
  for (const auto &iter : *resp_commands) {
    auto s = writer_->Write(iter.first, iter.second);
    if (!s.IsOK()) return s;
  }

  return Status::OK();
}
