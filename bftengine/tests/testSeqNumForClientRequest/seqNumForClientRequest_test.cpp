#include "gtest/gtest.h"
#include "SimpleClientImp.cpp"
#include <list>

using namespace std;
using namespace bftEngine;

namespace {

class MockSeqNumGenerator : public SeqNumberGeneratorForClientRequestsImp {
 public:
  uint32_t getLastCountOfUniqueFetchID() { return lastCountOfUniqueFetchID_; }

  void setLastCountOfUniqueFetchID(uint64_t count) { lastCountOfUniqueFetchID_ = count; }
};

TEST(seqNumForClientRequest_test, time_stamp_smaller_SN_monotonicity_preserved) {
  SeqNumberGeneratorForClientRequests *seqNumGen = new MockSeqNumGenerator();
  list<u_int64_t> seqNums;

  // generate bunch of sequence numbers
  for (int i = 0; i < 10; i++) {
    seqNums.push_back(seqNumGen->generateUniqueSequenceNumberForRequest());
    usleep(1000);
  }

  u_int64_t lastSeqNum = seqNums.back();
  std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
  now.operator-=(chrono::milliseconds(1000));
  u_int64_t newSeqNum = seqNumGen->generateUniqueSequenceNumberForRequest(now);

  ASSERT_TRUE(lastSeqNum + 1 == newSeqNum);
  delete seqNumGen;
}

TEST(seqNumForClientRequest_test, monotonicity_check) {
  SeqNumberGeneratorForClientRequests *seqNumGen = new MockSeqNumGenerator();
  std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
  list<u_int64_t> seqNums;

  for (int i = 0; i < 10; i++) {
    seqNums.push_back(seqNumGen->generateUniqueSequenceNumberForRequest(now));
    now.operator+=(chrono::milliseconds(1000));
    usleep(1000);
  }

  // Checks that generated SN is monotonically increasing.
  ConcordAssert(is_sorted(seqNums.begin(), seqNums.end()));
  now.operator+=(std::chrono::hours(24));
  auto newSeqNum = seqNumGen->generateUniqueSequenceNumberForRequest(now);
  auto lastSeqNum = seqNums.back();

  // Two SN generated on same time in different days, stays monotonic.
  ASSERT_TRUE(newSeqNum > lastSeqNum);
  delete seqNumGen;
}

TEST(seqNumForClientRequest_test, send_requests_same_time_uniqueness_preserved) {
  SeqNumberGeneratorForClientRequests *seqNumGen = new MockSeqNumGenerator();
  std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
  set<u_int64_t> seqNums;

  for (int i = 0; i < 10; i++) {
    seqNums.insert(seqNumGen->generateUniqueSequenceNumberForRequest(now));
  }

  // assert 10 unique SN was generated, although practically sent at the same time.
  ConcordAssert(seqNums.size() == 10);
  delete seqNumGen;
}

TEST(seqNumForClientRequest_test, exceed_counter_limit_uniqueness_preserved) {
  SeqNumberGeneratorForClientRequests *seqNumGen = new MockSeqNumGenerator();
  std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
  set<u_int64_t> seqNums;

  seqNums.insert(seqNumGen->generateUniqueSequenceNumberForRequest(now));
  auto mockSeqNumGen = dynamic_cast<MockSeqNumGenerator *>(seqNumGen);

  // Set lastCount biggest value.
  mockSeqNumGen->setLastCountOfUniqueFetchID(0x3FFFFF);
  seqNums.insert(seqNumGen->generateUniqueSequenceNumberForRequest(now));

  // assert unique SN was generated, although counter limit exceeded.
  ASSERT_EQ(seqNums.size(), 2);
  delete seqNumGen;
}

TEST(seqNumForClientRequest_test, invoke_counter_increasement) {
  SeqNumberGeneratorForClientRequests *seqNumGen = new MockSeqNumGenerator();
  set<u_int64_t> seqNums;

  // generate bunch of sequence numbers
  for (int i = 0; i < 10; i++) {
    seqNums.insert(seqNumGen->generateUniqueSequenceNumberForRequest());
    usleep(1000);
  }
  // assert 10 unique sequence numbers was generated.
  ASSERT_EQ(seqNums.size(), 10);

  std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
  now.operator-=(chrono::milliseconds(1000));
  seqNums.insert(seqNumGen->generateUniqueSequenceNumberForRequest(now));
  auto mockSeqNumGen = dynamic_cast<MockSeqNumGenerator *>(seqNumGen);

  // assert 'lastCountOfUniqueFetchID_' variable increased.
  ASSERT_EQ(mockSeqNumGen->getLastCountOfUniqueFetchID(), 1);
  delete seqNumGen;
}
}  // namespace

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
