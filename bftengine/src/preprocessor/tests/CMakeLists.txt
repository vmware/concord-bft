find_package(GTest REQUIRED)

add_executable(preprocessor_test preprocessor_test.cpp )
add_test(preprocessor_test preprocessor_test)

target_include_directories(preprocessor_test PUBLIC ..)

target_link_libraries(preprocessor_test PUBLIC
    GTest::Main
    util
    bftcommunication
    preprocessor
    corebft)
