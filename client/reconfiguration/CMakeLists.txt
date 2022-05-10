project (cre LANGUAGES CXX)

add_library(cre
        src/client_reconfiguration_engine.cpp
        src/poll_based_state_client.cpp
        src/st_based_reconfiguration_client.cpp
        src/default_handlers.cpp
        )

target_include_directories(cre PUBLIC 
        include
        ${bftengine_SOURCE_DIR}/include/bftengine)

target_link_libraries(cre PUBLIC
        corebft
        bftclient_new
        cmf_messages
        util)

if (BUILD_TESTING)
    add_subdirectory(test)
endif()