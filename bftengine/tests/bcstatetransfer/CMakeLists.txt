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
