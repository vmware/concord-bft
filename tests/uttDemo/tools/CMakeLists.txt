set(gen_utt_cfg_src
    GenerateUTTConfig.cpp
    ../utt_blockchain_app.cpp
    ../transactions.cpp
    ../utt_config.cpp
)

add_executable(gen_utt_cfg ${gen_utt_cfg_src})

target_include_directories(gen_utt_cfg
    PUBLIC
        ..
        ${UTT_DEMO_INCLUDES}
)

target_link_libraries(gen_utt_cfg PUBLIC test_config_lib ${UTT_DEMO_LINKS})