set(payment_service_src
    main.cpp
	${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp
)

add_executable(payment_service ${payment_service_src})

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(payment_service PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(${USE_COMM_TLS_TCP})
    target_compile_definitions(payment_service PUBLIC USE_COMM_TLS_TCP)
endif()

target_include_directories(payment_service PUBLIC ../..)

target_link_libraries(payment_service PUBLIC corebft test_config_lib utt_messages_cmf)