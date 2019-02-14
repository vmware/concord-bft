#include "gtest/gtest.h"
#include "InMemoryDataStore.hpp"

TEST(in_memory_data_store, initialize) {
    auto store = bftEngine::SimpleBlockchainStateTransfer::impl::InMemoryDataStore(4096);
    store.setAsInitialized();
    ASSERT_TRUE(store.initialized());
}
