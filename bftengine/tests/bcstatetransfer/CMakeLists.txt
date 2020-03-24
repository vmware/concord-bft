add_executable(bcstatetransfer_tests bcstatetransfer_tests.cpp)

add_test(bcstatetransfer_tests bcstatetransfer_tests)
find_package(GTest REQUIRED)

# We are testing implementation details, so must reach into the src hierarchy
# for includes that aren't public in cmake.
target_include_directories(bcstatetransfer_tests
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bcstatetransfer
      ${bftengine_SOURCE_DIR}/src/bftengine)

# TODO [TK] TEMPORARY - WILL BE REMOVED AFTER DBADAPTER REFACTORING !!!
target_include_directories(bcstatetransfer_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../../kvbc/include)

target_link_libraries(bcstatetransfer_tests GTest::Main)
target_link_libraries(bcstatetransfer_tests corebft kvbc $<TARGET_OBJECTS:logging_dev>)
target_compile_options(bcstatetransfer_tests PUBLIC "-Wno-sign-compare")
