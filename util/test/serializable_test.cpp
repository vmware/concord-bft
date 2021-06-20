// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "gtest/gtest.h"
#include "Serializable.h"
#include <iostream>
#include <set>
#include <algorithm>
#include "Logger.hpp"
namespace test {
namespace serializable {

using namespace concord::serialize;

struct TestSerializable : public SerializableFactory<TestSerializable> {
  TestSerializable() = default;
  TestSerializable(const std::string& s, std::uint8_t ui8, std::uint16_t ui16, std::uint32_t ui32, std::uint64_t ui64)
      : str(s), ui8_(ui8), ui16_(ui16), ui32_(ui32), ui64_(ui64) {}
  bool operator==(const TestSerializable& other) const {
    return str == other.str && ui8_ == other.ui8_ && ui16_ == other.ui16_ && ui32_ == other.ui32_ &&
           ui64_ == other.ui64_;
  }
  bool operator<(const TestSerializable& other) const { return ui8_ < other.ui8_; }
  virtual const std::string getVersion() const { return "1"; };
  virtual void serializeDataMembers(std::ostream& os) const {
    serialize(os, str);
    serialize(os, ui8_);
    serialize(os, ui16_);
    serialize(os, ui32_);
    serialize(os, ui64_);
  }
  virtual void deserializeDataMembers(std::istream& is) {
    deserialize(is, str);
    deserialize(is, ui8_);
    deserialize(is, ui16_);
    deserialize(is, ui32_);
    deserialize(is, ui64_);
  }

  std::string str;
  std::uint8_t ui8_ = 0;
  std::uint16_t ui16_ = 0;
  std::uint32_t ui32_ = 0;
  std::uint64_t ui64_ = 0;
};

TEST(serializable, Serializable) {
  TestSerializable t{"testSerializable", 1, 2, 3, 4};
  TestSerializable* t1;
  std::stringstream sstream;
  t.serialize(sstream);
  Serializable::deserialize(sstream, t1);

  ASSERT_TRUE(t == *t1);

  delete t1;
}

TEST(serializable, SetInt) {
  std::set<int> s_out, s{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

  std::stringstream sstream;
  Serializable::serialize(sstream, s);
  Serializable::deserialize(sstream, s_out);

  ASSERT_TRUE(s == s_out);
}

TEST(serializable, SetSerializable) {
  std::set<TestSerializable> s, s_out;
  for (int i = 0; i < 10; ++i) {
    s.insert(TestSerializable{"testSerializable" + std::to_string(i),
                              (std::uint8_t)(i + 1),
                              (std::uint16_t)(i + 2),
                              (std::uint32_t)(i + 3),
                              (std::uint64_t)(i + 4)});
  }
  std::stringstream sstream;
  Serializable::serialize(sstream, s);
  Serializable::deserialize(sstream, s_out);
  ASSERT_TRUE(s == s_out);
}

TEST(serializable, VectorInt) {
  std::vector<int> s_out, s{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

  std::stringstream sstream;
  Serializable::serialize(sstream, s);
  Serializable::deserialize(sstream, s_out);

  ASSERT_TRUE(s == s_out);
}

TEST(serializable, Map) {
  std::stringstream sstream;
  std::map<std::string, int> s_out, s{{"k0", 0}, {"k1", 1}, {"k2", 2}, {"k3", 3}, {"k4", 4}, {"k5", 5}};
  Serializable::serialize(sstream, s);
  Serializable::deserialize(sstream, s_out);
  ASSERT_TRUE(s == s_out);
}

TEST(serializable, MapConstString) {
  std::stringstream sstream;
  std::map<const std::string, int> s_out, s{{"k0", 0}, {"k1", 1}, {"k2", 2}, {"k3", 3}, {"k4", 4}, {"k5", 5}};
  Serializable::serialize(sstream, s);
  Serializable::deserialize(sstream, s_out);
  ASSERT_TRUE(s == s_out);
}

TEST(serializable, UnorderedMap) {
  std::stringstream sstream;
  std::unordered_map<std::string, int> s_out, s{{"k0", 0}, {"k1", 1}, {"k2", 2}, {"k3", 3}, {"k4", 4}, {"k5", 5}};
  Serializable::serialize(sstream, s);
  Serializable::deserialize(sstream, s_out);
  ASSERT_TRUE(s == s_out);
}

TEST(serializable, VectorSerializable) {
  std::vector<TestSerializable> s, s_out;
  for (int i = 0; i < 10; ++i) {
    s.push_back(TestSerializable{"testSerializable" + std::to_string(i),
                                 (std::uint8_t)(i + 1),
                                 (std::uint16_t)(i + 2),
                                 (std::uint32_t)(i + 3),
                                 (std::uint64_t)(i + 4)});
  }
  std::stringstream sstream;
  Serializable::serialize(sstream, s);
  Serializable::deserialize(sstream, s_out);
  ASSERT_TRUE(s == s_out);
}

TEST(serializable, VectorSerializablePtr) {
  std::vector<TestSerializable*> s, s_out;
  for (int i = 0; i < 2; ++i) {
    s.push_back(new TestSerializable{"testSerializable" + std::to_string(i),
                                     (std::uint8_t)(i + 1),
                                     (std::uint16_t)(i + 2),
                                     (std::uint32_t)(i + 3),
                                     (std::uint64_t)(i + 4)});
  }
  std::stringstream sstream;
  Serializable::serialize(sstream, s);
  Serializable::deserialize(sstream, s_out);
  ASSERT_TRUE(
      std::equal(s.begin(), s.end(), s_out.begin(), [](const TestSerializable* lhs, const TestSerializable* rhs) {
        return *lhs == *rhs;
      }));

  for (auto* v : s) {
    delete v;
  }
  for (auto* v : s_out) {
    delete v;
  }
}

TEST(serializable, VectorSerializableVectors) {
  std::vector<std::vector<TestSerializable>> vec_of_vecs, vec_of_vecs_out;
  for (int j = 0; j < 10; ++j) {
    std::vector<TestSerializable> v;
    v.reserve(10);
    for (int i = 0; i < 10; ++i)
      v.push_back(TestSerializable{"testSerializable" + std::to_string(i),
                                   (std::uint8_t)(i + 1),
                                   (std::uint16_t)(i + 2),
                                   (std::uint32_t)(i + 3),
                                   (std::uint64_t)(i + 4)});
    vec_of_vecs.push_back(v);
  }
  std::stringstream sstream;
  Serializable::serialize(sstream, vec_of_vecs);
  Serializable::deserialize(sstream, vec_of_vecs_out);
  ASSERT_TRUE(vec_of_vecs == vec_of_vecs_out);
}

TEST(serializable, VectorSerializablePtrVectors) {
  std::vector<std::vector<TestSerializable*>> vec_of_vecs, vec_of_vecs_out;
  for (int j = 0; j < 10; ++j) {
    std::vector<TestSerializable*> v;
    v.reserve(10);
    for (int i = 0; i < 10; ++i)
      v.push_back(new TestSerializable{"testSerializable" + std::to_string(i),
                                       (std::uint8_t)(i + 1),
                                       (std::uint16_t)(i + 2),
                                       (std::uint32_t)(i + 3),
                                       (std::uint64_t)(i + 4)});
    vec_of_vecs.push_back(v);
  }
  std::stringstream sstream;
  Serializable::serialize(sstream, vec_of_vecs);
  Serializable::deserialize(sstream, vec_of_vecs_out);
  for (std::vector<int>::size_type i = 0; i < vec_of_vecs.size(); ++i) {
    ASSERT_TRUE(std::equal(vec_of_vecs[i].begin(),
                           vec_of_vecs[i].end(),
                           vec_of_vecs_out[i].begin(),
                           [](const TestSerializable* lhs, const TestSerializable* rhs) { return *lhs == *rhs; }));
  }

  auto cleanupVec = [](auto& V) {
    for (auto& v : V) {
      for (auto* p : v) {
        delete p;
      }
    }
  };
  cleanupVec(vec_of_vecs);
  cleanupVec(vec_of_vecs_out);
}
}  // namespace serializable
}  // namespace test

int main(int argc, char** argv) {
#ifdef USE_LOG4CPP
  // uncomment if needed
  // log4cplus::Logger::getInstance( LOG4CPLUS_TEXT("serializable")).setLogLevel(log4cplus::TRACE_LOG_LEVEL);
#endif
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
