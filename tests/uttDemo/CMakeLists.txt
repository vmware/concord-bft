cmake_minimum_required (VERSION 3.2)
project(utt_demo VERSION 0.1.0.0 LANGUAGES CXX)

add_subdirectory(cmf)
add_subdirectory(UTTClient)
add_subdirectory(UTTReplica)

add_custom_target(copy_utt_demo_scripts ALL COMMENT "Copying utt demo scripts")
add_custom_command(TARGET copy_utt_demo_scripts
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/scripts ${CMAKE_CURRENT_BINARY_DIR}/scripts)

