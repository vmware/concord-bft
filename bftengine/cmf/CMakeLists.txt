cmf_generate_cpp(header cpp concord::messages::keys_and_signatures keys_and_signatures.cmf)
add_library(keys_and_signatures_cmf ${cpp})
set_target_properties(keys_and_signatures_cmf PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(keys_and_signatures_cmf PUBLIC ${CMAKE_CURRENT_BINARY_DIR})


cmf_generate_cpp(header cpp concord::messages::execution_data execution_data.cmf)
add_library(execution_data_cmf ${cpp})
set_target_properties(execution_data_cmf PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(execution_data_cmf PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

cmf_generate_cpp(header cpp concord::messages::db_checkpoint_msg db_checkpoint_msg.cmf)
add_library(db_checkpoint_msg ${cpp})
set_target_properties(db_checkpoint_msg PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(db_checkpoint_msg PUBLIC ${CMAKE_CURRENT_BINARY_DIR})