find_package(GTest REQUIRED)

add_executable(SequenceWithActiveWindow_tests 
                testSequenceWithActiveWindow.cpp)

add_test(SequenceWithActiveWindow_tests SequenceWithActiveWindow_tests)

# We are testing implementation details, so must reach into the src hierarchy
# for includes that aren't public in cmake.
target_include_directories(SequenceWithActiveWindow_tests
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine
      ${concord_bft_tools_SOURCE_DIR})

target_link_libraries(SequenceWithActiveWindow_tests PUBLIC
    GTest::Main
    util
    corebft
    )
