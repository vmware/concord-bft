find_package(GTest REQUIRED)

add_executable(clientservice-test-yaml_parsing yaml_parsing.cpp)
target_link_libraries(clientservice-test-yaml_parsing PUBLIC
        GTest::Main
        clientservice-lib
        )
add_test(clientservice-test-yaml_parsing clientservice-test-yaml_parsing)
