cmf_generate_cpp(header cpp concord::kvbc::categorization categorized_kvbc_msgs.cmf)
add_library(categorized_kvbc_msgs ${cpp})
set_target_properties(categorized_kvbc_msgs PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(categorized_kvbc_msgs PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
