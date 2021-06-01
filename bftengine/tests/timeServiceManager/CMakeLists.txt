find_package(GTest REQUIRED)

add_executable(TimeServiceManager_test TimeServiceManager_test.cpp)

add_test(TimeServiceManager_test TimeServiceManager_test)

target_link_libraries(TimeServiceManager_test PUBLIC
    GTest::Main
    corebft)

