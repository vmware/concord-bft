
set(utt-server-api-src
    src/UTTServerApi.cpp
)

add_library(utt_server_api STATIC ${utt-server-api-src})

target_include_directories(utt_server_api PUBLIC include ../utt-common-api/include)

target_link_libraries(utt_server_api PUBLIC utt_api)