add_library(test_config_lib
        config_file_parser.cpp
        config_file_parser_test.cpp
        test_comm_config.cpp
        )
target_include_directories(test_config_lib PUBLIC
        ${bftengine_SOURCE_DIR}/include/communication
        ${bftengine_SOURCE_DIR}/include
        ${bftengine_SOURCE_DIR}/include/bftengine
        ${PROJECT_SOURCE_DIR}/threshsign/include
        ${concord_bft_tools_SOURCE_DIR})
