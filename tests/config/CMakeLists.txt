add_library(test_config_lib
        test_comm_config.cpp
        ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp #TODO [TK] separate lib tools
        )
target_include_directories(test_config_lib PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${bftengine_SOURCE_DIR}/include
        ${bftengine_SOURCE_DIR}/include/bftengine
        ${concord_bft_tools_SOURCE_DIR})

target_link_libraries(test_config_lib PRIVATE bftcommunication threshsign)
if (USE_S3_OBJECT_STORE)
    target_link_libraries(test_config_lib PRIVATE concordbft_storage)
endif()