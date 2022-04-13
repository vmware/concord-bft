cmake_minimum_required (VERSION 3.2)
project(utt_demo VERSION 0.1.0.0 LANGUAGES CXX)

add_definitions(
  -DCURVE_BN128
  -DBN_SUPPORT_SNARK=1
)

list(APPEND UTT_DEMO_INCLUDES ${libutt_SOURCE_DIR}/libutt/include)
list(APPEND UTT_DEMO_LINKS utt)

add_subdirectory(cmf)
add_subdirectory(tools)
add_subdirectory(UTTClient)
add_subdirectory(UTTReplica)
add_subdirectory(PaymentService)

add_custom_target(copy_utt_demo_scripts ALL COMMENT "Copying utt demo scripts")

add_custom_command(TARGET copy_utt_demo_scripts
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/scripts ${CMAKE_CURRENT_BINARY_DIR}/scripts
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/scripts/logs
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/scripts/rocksdb
)

