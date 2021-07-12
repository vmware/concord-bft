add_library(lib_db_editor INTERFACE)
target_link_libraries(lib_db_editor INTERFACE)
target_include_directories(lib_db_editor INTERFACE include)

if(BUILD_ROCKSDB_STORAGE)
add_executable(kv_blockchain_db_editor src/kv_blockchain_db_editor.cpp)
target_include_directories(kv_blockchain_db_editor PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include/kvbc_app_filter/
                                                           ${CMAKE_CURRENT_SOURCE_DIR}/../../../bftengine/src) 
														   
target_link_libraries(kv_blockchain_db_editor PUBLIC lib_db_editor util kvbc stdc++fs execution_data_cmf)
endif()
