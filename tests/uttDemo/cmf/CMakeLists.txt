cmf_generate_cpp(header cpp skvbc::messages skvbc_messages.cmf)
add_library(skvbc_messages_cmf ${cpp})
set_target_properties(skvbc_messages_cmf PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(skvbc_messages_cmf PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
