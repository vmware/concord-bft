add_subdirectory(kv-cmf)
add_subdirectory(replica)
add_subdirectory(client)

add_custom_target(copy_scripts_dir ALL COMMENT "Copying scripts directory")
add_custom_command(TARGET copy_scripts_dir
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/scripts ${CMAKE_CURRENT_BINARY_DIR}/scripts)

add_custom_target(copy_tls_cert_script ALL COMMENT "Copying TLS script")
add_custom_command(TARGET copy_tls_cert_script
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/scripts/linux/create_tls_certs.sh 
        ${CMAKE_CURRENT_BINARY_DIR}/scripts/create_tls_certs.sh)

add_custom_target(copy_tran_sign_keys_script ALL COMMENT "Copying create_concord_clients_transaction_signing_keys script")
add_custom_command(TARGET copy_tran_sign_keys_script
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/scripts/linux/create_concord_clients_transaction_signing_keys.sh 
        ${CMAKE_CURRENT_BINARY_DIR}/scripts/create_concord_clients_transaction_signing_keys.sh)

add_custom_target(copy_msgConfig_dir ALL COMMENT "Copying msg-configs directory")
add_custom_command(TARGET copy_msgConfig_dir
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/msg-configs ${CMAKE_CURRENT_BINARY_DIR}/msg-configs)
