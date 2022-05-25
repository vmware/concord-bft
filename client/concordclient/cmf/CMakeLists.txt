cmf_generate_cpp(header cpp concord::client::message concord_client_message.cmf)
add_library(concord_client_message ${cpp})
set_target_properties(concord_client_message PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(concord_client_message PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
