find_package(GTest REQUIRED)

add_executable(ViewChange_tests 
                testViewChange.cpp
                ${bftengine_SOURCE_DIR}/tests/messages/helper.cpp
                ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp)

add_test(ViewChange_tests ViewChange_tests)

# We are testing implementation details, so must reach into the src hierarchy
# for includes that aren't public in cmake.
target_include_directories(ViewChange_tests
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine
      ${bftengine_SOURCE_DIR}/tests/messages
      ${concord_bft_tools_SOURCE_DIR})

target_link_libraries(ViewChange_tests PUBLIC
    GTest::Main
    util
    corebft
    threshsign
    )

add_executable(views_manager_test test_views_manager.cpp ${bftengine_SOURCE_DIR}/tests/messages/helper.cpp)

add_test(views_manager_test views_manager_test)
target_link_libraries(views_manager_test GTest::Main corebft)

# Not using target_link_libraries, because the header is in the src directory.
target_include_directories(views_manager_test PRIVATE 
    ${bftengine_SOURCE_DIR}/src/bftengine 
    ${bftengine_SOURCE_DIR}/tests/messages)