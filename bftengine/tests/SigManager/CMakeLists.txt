find_package(GTest REQUIRED)

add_executable(SigManager_test
                SigManager_test.cpp
                ${bftengine_SOURCE_DIR}/tests/messages/helper.cpp)

target_include_directories(SigManager_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine
      ${bftengine_SOURCE_DIR}/tests/messages)

add_test(SigManager_test SigManager_test)

target_link_libraries(SigManager_test PUBLIC
    GTest::Main
    corebft)
