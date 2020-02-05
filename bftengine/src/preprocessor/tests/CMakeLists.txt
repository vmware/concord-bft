find_package(GTest REQUIRED)

add_executable(preprocessor_test preprocessor_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(preprocessor_test preprocessor_test)

target_link_libraries(preprocessor_test PUBLIC
    GTest::Main
    util
    preprocessor)
