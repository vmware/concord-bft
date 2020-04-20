find_package(GTest REQUIRED)
find_package(yaml-cpp REQUIRED)

add_executable(preprocessor_test preprocessor_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(preprocessor_test preprocessor_test)

target_include_directories(preprocessor_test PUBLIC ..)

target_link_libraries(preprocessor_test PUBLIC
    GTest::Main
    util
    bftcommunication
    preprocessor
    corebft)
target_link_libraries(preprocessor_test PRIVATE ${YAML-CPP_LIBRARIES})
