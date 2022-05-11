cmf_generate_cpp(header cpp concord::client::request::messages concord_client_request.cmf)
add_library(concord_client_request ${cpp})
set_target_properties(concord_client_request PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(concord_client_request PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
