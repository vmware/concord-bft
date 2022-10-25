add_subdirectory("proto")

set(privacy-admin-cli-src
    src/main.cpp
    src/admin.cpp
)

add_executable(privacy-admin-cli ${privacy-admin-cli-src})

target_include_directories(privacy-admin-cli PUBLIC include/ ../utt-client-api/include ../utt-common-api/include)

target_link_libraries(privacy-admin-cli PUBLIC
  privacy-admin-api-proto utt_client_api
)