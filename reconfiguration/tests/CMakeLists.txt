find_package(GTest REQUIRED)

if (BUILD_ROCKSDB_STORAGE)
    add_executable(pruning_test pruning_test.cpp)
    add_test(pruning_test pruning_test)
    target_link_libraries(pruning_test
            GTest::Main
            concordbft_reconfiguration
            stdc++fs)
endif(BUILD_ROCKSDB_STORAGE)
