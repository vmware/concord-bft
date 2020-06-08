find_package(GTest REQUIRED)

add_executable(ControllerWithSimpleHistory_test ControllerWithSimpleHistory_test.cpp )
add_test(ControllerWithSimpleHistory_test ControllerWithSimpleHistory_test)

target_link_libraries(ControllerWithSimpleHistory_test PUBLIC
    GTest::Main
    corebft)
