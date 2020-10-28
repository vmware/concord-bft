find_package(GTest REQUIRED)

add_executable(db_partitions_test db_partitions_test.cpp)
add_test(db_partitions_test db_partitions_test)
target_link_libraries(db_partitions_test PUBLIC
    GTest::Main
    GTest::GTest
    util
    concordbft_storage
    stdc++fs
)
