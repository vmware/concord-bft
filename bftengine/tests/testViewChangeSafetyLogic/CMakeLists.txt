find_package(GTest REQUIRED)

add_executable(testViewchangeSafetyLogic_tests 
                testViewChangeSafetyLogic.cpp
                $<TARGET_OBJECTS:logging_dev>
                ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp)

add_test(testViewchangeSafetyLogic_tests testViewchangeSafetyLogic_tests)

target_include_directories(testViewchangeSafetyLogic_tests PRIVATE ..)
target_include_directories(testViewchangeSafetyLogic_tests PRIVATE ../../../tools)

# We are testing implementation details, so must reach into the src hierarchy
# for includes that aren't public in cmake.
#target_include_directories(bcstatetransfer_tests
#      PRIVATE
#      ${bftengine_SOURCE_DIR}/src/bftengine)


target_link_libraries(testViewchangeSafetyLogic_tests PUBLIC
    GTest::Main
    util
    corebft
    threshsign
    )
