add_subdirectory("proto")

set(privacy-wallet-libray-src
    src/main.cpp
    src/wallet.cpp
    src/fileBasedUserStorage.cpp
)
find_package(nlohmann_json REQUIRED)
add_executable(privacy-wallet-libray-service ${privacy-wallet-libray-src})

target_include_directories(privacy-wallet-libray-service PUBLIC include/ ../utt-client-api/include ../utt-common-api/include)

target_link_libraries(privacy-wallet-libray-service PUBLIC
  privacy-wallet-library-proto utt_client_api nlohmann_json::nlohmann_json stdc++fs
)

add_library(privacy-wallet-lib ${privacy-wallet-libray-src})
target_include_directories(privacy-wallet-lib PUBLIC include/ ../utt-client-api/include ../utt-common-api/include)

target_link_libraries(privacy-wallet-lib PUBLIC
privacy-wallet-library-proto utt_client_api nlohmann_json::nlohmann_json stdc++fs
)

add_subdirectory(tests)