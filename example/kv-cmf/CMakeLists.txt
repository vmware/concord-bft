cmf_generate_cpp(header cpp kvbc::messages kv_replica_msgs.cmf)
add_library(cmf_kv_replica_msgs ${cpp})
set_target_properties(cmf_kv_replica_msgs PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(cmf_kv_replica_msgs PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
