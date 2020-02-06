find_package(GTest REQUIRED)
if(Boost_FOUND)
    add_executable(blockchain_view_test blockchain_view_test.cpp $<TARGET_OBJECTS:logging_dev>)
    add_test(blockchain_view_test blockchain_view_test)
    target_include_directories(blockchain_view_test PRIVATE ${Boost_INCLUDE_DIRS})
    target_link_libraries(blockchain_view_test PUBLIC
        GTest::Main
        util
        kvbc
    )
endif()

if(BUILD_ROCKSDB_STORAGE OR USE_OBJECTSTORE_FOR_ROR_TEST)
    add_executable(
        ror_app_state_test
        ror_app_state_test.cpp
        $<TARGET_OBJECTS:logging_dev>)
    add_test(ror_app_state_test ror_app_state_test)
    target_link_libraries(
        ror_app_state_test PUBLIC
        GTest::Main
        object_store_lib
        kvbc
        util)
endif()
