add_executable(bcstatetransfer_tests
    bcstatetransfer_tests.cpp)

add_test(bcstatetransfer_tests bcstatetransfer_tests)

# We are testing implementation details, so must reach into the src hierarchy
# for includes that aren't public in cmake.
target_include_directories(bcstatetransfer_tests
    PRIVATE
    ${bftengine_SOURCE_DIR}/src/bcstatetransfer
    ${bftengine_SOURCE_DIR}/src/bftengine)

target_link_libraries(bcstatetransfer_tests gtest_main)
target_link_libraries(bcstatetransfer_tests corebft)
