
set(utt-client-api-src
    src/ClientApi.cpp
    src/User.cpp
    src/PickCoins.cpp
)

add_library(utt_client_api STATIC ${utt-client-api-src})

target_include_directories(utt_client_api PUBLIC include ../utt-common-api/include ../libutt/include)

target_link_libraries(utt_client_api PUBLIC utt_api)

add_subdirectory(tests)