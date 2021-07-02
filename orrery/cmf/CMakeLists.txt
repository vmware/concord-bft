cmf_generate_cpp(header cpp concord::orrery orrery_msgs.cmf)
add_library(orrery_msgs ${cpp})
set_target_properties(orrery_msgs PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(orrery_msgs PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
