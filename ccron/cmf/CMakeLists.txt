cmf_generate_cpp(header cpp concord::cron ccron_msgs.cmf)
add_library(ccron_msgs ${cpp})
set_target_properties(ccron_msgs PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(ccron_msgs PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
