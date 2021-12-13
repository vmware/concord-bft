add_library(test_config_lib
        config_file_parser.cpp
        test_comm_config.cpp
        )
target_include_directories(test_config_lib PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${bftengine_SOURCE_DIR}/include
        ${bftengine_SOURCE_DIR}/include/bftengine
        ${PROJECT_SOURCE_DIR}/threshsign/include
        ${concord_bft_tools_SOURCE_DIR})

target_link_libraries(test_config_lib PRIVATE bftcommunication)
if (USE_S3_OBJECT_STORE)
target_link_libraries(test_config_lib PRIVATE concordbft_storage)
endif()