// tests/cppunit/kvrocks2redis_resp_parser_test.cc
#include <gtest/gtest.h>

#include <string>

#include "common/string_util.h"

using util::ExtractRespKeyForRouting;
using util::RespParseResult;

TEST(ExtractRespKeyForRouting, SetExtractsKeyZeroAlloc) {
  std::string buf = "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
  std::string_view key;
  size_t consumed = 0;
  EXPECT_EQ(ExtractRespKeyForRouting(buf, &key, &consumed), RespParseResult::OK);
  EXPECT_EQ(key, "hello");
  EXPECT_EQ(consumed, buf.size());
}

TEST(ExtractRespKeyForRouting, FlushDbKeyIsEmpty) {
  std::string buf = "*1\r\n$7\r\nFLUSHDB\r\n";
  std::string_view key;
  size_t consumed = 0;
  EXPECT_EQ(ExtractRespKeyForRouting(buf, &key, &consumed), RespParseResult::OK);
  EXPECT_TRUE(key.empty());
  EXPECT_EQ(consumed, buf.size());
}

TEST(ExtractRespKeyForRouting, HSetExtractsKey) {
  std::string buf = "*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
  std::string_view key;
  size_t consumed = 0;
  EXPECT_EQ(ExtractRespKeyForRouting(buf, &key, &consumed), RespParseResult::OK);
  EXPECT_EQ(key, "myhash");
  EXPECT_EQ(consumed, buf.size());
}

TEST(ExtractRespKeyForRouting, NeedsMoreOnTruncatedValue) {
  std::string buf = "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworl";
  std::string_view key;
  size_t consumed = 0;
  EXPECT_EQ(ExtractRespKeyForRouting(buf, &key, &consumed), RespParseResult::NeedsMore);
}

TEST(ExtractRespKeyForRouting, ErrorOnNonArrayPrefix) {
  std::string buf = "+OK\r\n";
  std::string_view key;
  size_t consumed = 0;
  EXPECT_EQ(ExtractRespKeyForRouting(buf, &key, &consumed), RespParseResult::Error);
}

TEST(ExtractRespKeyForRouting, KeyViewPointsIntoOriginalBuffer) {
  std::string buf = "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
  std::string_view key;
  size_t consumed = 0;
  ASSERT_EQ(ExtractRespKeyForRouting(buf, &key, &consumed), RespParseResult::OK);
  // key must point into buf, not a copy
  EXPECT_GE(key.data(), buf.data());
  EXPECT_LE(key.data() + key.size(), buf.data() + buf.size());
}

TEST(ExtractRespKeyForRouting, TwoConsecutiveCommands) {
  std::string buf =
      "*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n"
      "*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
  std::string_view key;
  size_t consumed = 0;

  ASSERT_EQ(ExtractRespKeyForRouting(buf, &key, &consumed), RespParseResult::OK);
  EXPECT_EQ(key, "k1");
  size_t first = consumed;

  ASSERT_EQ(ExtractRespKeyForRouting(std::string_view(buf).substr(first), &key, &consumed),
            RespParseResult::OK);
  EXPECT_EQ(key, "k2");
}
