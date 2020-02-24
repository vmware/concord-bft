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
