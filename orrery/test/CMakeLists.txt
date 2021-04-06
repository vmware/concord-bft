find_package(GTest REQUIRED)
add_executable(orrery_test orrery_test.cpp)
add_test(orrery_test orrery_test)
target_link_libraries(orrery_test GTest::Main orrery util)
