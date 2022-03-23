add_executable(gen_utt_cfg GenerateUTTConfig.cpp)

target_include_directories(gen_utt_cfg PUBLIC ${UTT_DEMO_INCLUDES})

target_link_libraries(gen_utt_cfg PUBLIC ${UTT_DEMO_LINKS})