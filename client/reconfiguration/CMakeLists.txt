project (cre LANGUAGES CXX)

add_library(cre
        src/client_reconfiguration_engine.cpp
        src/poll_based_state_client.cpp
        src/st_based_reconfiguration_client.cpp
        src/default_handlers.cpp
        )

target_include_directories(cre PUBLIC 
        include)

target_link_libraries(cre PUBLIC
        bftclient_new
        cmf_messages
        concord-crypto
        util)

if (BUILD_TESTING)
    add_subdirectory(test)
endif()