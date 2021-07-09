find_package(GTest REQUIRED)

function(add_preexec_msg_test test_name sources extra_link_libs)
  add_executable(${test_name}
  ${sources}
  ${bftengine_SOURCE_DIR}/tests/messages/helper.cpp)
  add_test(${test_name} ${test_name})
  target_include_directories(${test_name}
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine
      ${bftengine_SOURCE_DIR}/tests/messages/
      ../..)
  target_link_libraries(${test_name} GTest::Main)
  target_link_libraries(${test_name} corebft )
  target_compile_options(${test_name} PUBLIC "-Wno-sign-compare")
endfunction(add_preexec_msg_test)

add_preexec_msg_test(ClientPreProcessRequestMsg_test ClientPreProcessRequestMsg_test.cpp "")
add_preexec_msg_test(PreProcessReplyMsg_test PreProcessReplyMsg_test.cpp "")
add_preexec_msg_test(PreProcessResultMsg_test PreProcessResultMsg_test.cpp "")
