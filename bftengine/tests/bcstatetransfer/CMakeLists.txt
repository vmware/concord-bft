add_executable(bcstatetransfer_tests bcstatetransfer_tests.cpp)

add_test(bcstatetransfer_tests bcstatetransfer_tests)
find_package(GTest REQUIRED)

# We are testing implementation details, so must reach into the src hierarchy
# for includes that aren't public in cmake.
target_include_directories(bcstatetransfer_tests
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bcstatetransfer
      ${bftengine_SOURCE_DIR}/src/bftengine)

target_link_libraries(bcstatetransfer_tests GTest::Main)
#TODO [TK] this test uses kvbc and should be moved from bftengine
target_link_libraries(bcstatetransfer_tests corebft kvbc )
target_compile_options(bcstatetransfer_tests PUBLIC "-Wno-sign-compare")

add_executable(source_selector_test source_selector_test.cpp)
add_test(source_selector_test source_selector_test)
target_link_libraries(source_selector_test GTest::Main corebft)
# Not using target_link_libraries, because the header is in the src directory.
target_include_directories(source_selector_test PRIVATE ${bftengine_SOURCE_DIR}/src/bcstatetransfer)

add_executable(RVT_test RVT_test.cpp)
add_test(RVT_test RVT_test)
target_link_libraries(RVT_test GTest::Main ${CRYPTOPP_LIBRARIES} corebft)
target_include_directories(RVT_test PRIVATE ${CRYPTOPP_INCLUDE_DIRS} PRIVATE ${bftengine_SOURCE_DIR}/src/bcstatetransfer)
