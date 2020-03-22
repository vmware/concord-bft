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

add_executable(order_test order_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(order_test order_test)
target_include_directories(order_test PUBLIC ${libkvbc_SOURCE_DIR}/include)
target_link_libraries(order_test GTest::Main util)
target_compile_options(order_test PUBLIC -Wno-sign-compare)