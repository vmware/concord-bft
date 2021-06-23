project (cre LANGUAGES CXX)

add_library(cre STATIC
        src/state_client.cpp)

target_include_directories(cre PUBLIC include)

target_link_libraries(cre PUBLIC
        bftclient_new
        cmf_messages)

