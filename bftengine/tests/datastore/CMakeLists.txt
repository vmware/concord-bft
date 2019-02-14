add_executable(datastore_unit_tests
    datastore_unit_tests.cpp)

# We are testing implementation details, so must reach into the src hierarchy
# for includes that aren't public in cmake.
target_include_directories(datastore_unit_tests
    PRIVATE
    ${bftengine_SOURCE_DIR}/src/bcstatetransfer)

target_link_libraries(datastore_unit_tests gtest_main)
target_link_libraries(datastore_unit_tests corebft)
