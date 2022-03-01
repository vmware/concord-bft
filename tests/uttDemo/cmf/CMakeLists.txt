cmf_generate_cpp(header cpp skvbc::messages utt_messages.cmf)
add_library(utt_messages_cmf ${cpp})
set_target_properties(utt_messages_cmf PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(utt_messages_cmf PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
