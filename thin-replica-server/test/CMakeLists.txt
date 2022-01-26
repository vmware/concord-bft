find_package(GTest REQUIRED)
find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

add_test(NAME thin_replica_server_test COMMAND thin_replica_server_test)
add_test(NAME trs_sub_buffer_test COMMAND trs_sub_buffer_test)
add_test(NAME replica_state_snapshot_service_test COMMAND replica_state_snapshot_service_test)

add_executable(thin_replica_server_test
        thin_replica_server_test.cpp)

target_link_libraries(thin_replica_server_test
        ${Boost_LIBRARIES}
        GTest::Main
        GTest::GTest
        thin-replica-proto
        thin_replica_server
        logging)

add_executable(trs_sub_buffer_test
        trs_sub_buffer_test.cpp)
target_include_directories(trs_sub_buffer_test PUBLIC kvbc/include)
target_link_libraries(trs_sub_buffer_test
        ${Boost_LIBRARIES}
        GTest::Main
        GTest::GTest
        categorized_kvbc_msgs
        thin_replica_server
        logging)

add_executable(replica_state_snapshot_service_test replica_state_snapshot_service_test.cpp)
target_link_libraries(replica_state_snapshot_service_test
        ${Boost_LIBRARIES}
        GTest::Main
        GTest::GTest
        kvbc
        thin_replica_server
        logging)

