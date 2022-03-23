
set(gen_utt_config_sources
  GenerateUTTConfig.cpp
)

add_executable(GenerateUTTConfig ${gen_utt_config_sources})

target_include_directories(GenerateUTTConfig
	PUBLIC
		${UTT_DEMO_INCLUDES}
)

target_link_libraries(GenerateUTTConfig PUBLIC ${UTT_DEMO_LINKS})