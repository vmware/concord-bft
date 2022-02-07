find_package(GTest REQUIRED)

add_executable(multiplex_comm_test multiplex_comm_test.cpp )
add_test(multiplex_comm_test multiplex_comm_test)

target_include_directories(multiplex_comm_test PUBLIC ..)

target_link_libraries(multiplex_comm_test PUBLIC
        GTest::Main
        corebft)

