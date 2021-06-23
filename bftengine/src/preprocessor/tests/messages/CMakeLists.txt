add_executable(ClientPreProcessRequestMsg_test
  ClientPreProcessRequestMsg_test.cpp
  ${bftengine_SOURCE_DIR}/tests/messages/helper.cpp)
add_test(ClientPreProcessRequestMsg_test ClientPreProcessRequestMsg_test)
find_package(GTest REQUIRED)
target_include_directories(ClientPreProcessRequestMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine
      ${bftengine_SOURCE_DIR}/tests/messages/
      ../..)
target_link_libraries(ClientPreProcessRequestMsg_test GTest::Main)
target_link_libraries(ClientPreProcessRequestMsg_test corebft )
target_compile_options(ClientPreProcessRequestMsg_test PUBLIC "-Wno-sign-compare")

add_executable(PreProcessReplyMsg_test 
  PreProcessReplyMsg_test.cpp
  ${bftengine_SOURCE_DIR}/tests/messages/helper.cpp)
target_include_directories(PreProcessReplyMsg_test
  PRIVATE
  ${bftengine_SOURCE_DIR}/src/bftengine
  ${bftengine_SOURCE_DIR}/tests/messages/
  ../..)
target_link_libraries(PreProcessReplyMsg_test 
  GTest::Main
  corebft)
target_compile_options(PreProcessReplyMsg_test PUBLIC "-Wno-sign-compare")

add_test(PreProcessReplyMsg_test PreProcessReplyMsg_test)
