find_package(GTest REQUIRED)

add_executable(TimeServiceResPageClient_test TimeServiceResPageClient_test.cpp)

add_test(TimeServiceResPageClient_test TimeServiceResPageClient_test)

target_link_libraries(TimeServiceResPageClient_test PUBLIC
    GTest::Main
    corebft)

