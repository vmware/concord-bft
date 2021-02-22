cmf_generate_cpp(header cpp concord::messages concord.cmf)
add_library(cmf_messages ${cpp})
set_target_properties(cmf_messages PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(cmf_messages PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
