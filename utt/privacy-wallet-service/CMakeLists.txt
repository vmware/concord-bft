add_subdirectory("proto")

set(privacy-wallet-library-src
    src/main.cpp
    src/Wallet.cpp
    src/PrivacyService.cpp
    src/FileBasedUserStorage.cpp
)
find_package(nlohmann_json REQUIRED)
add_executable(privacy-wallet-library-service ${privacy-wallet-library-src})

target_include_directories(privacy-wallet-library-service PUBLIC include/ ../utt-client-api/include ../utt-common-api/include)

target_link_libraries(privacy-wallet-library-service PUBLIC
  privacy-wallet-library-proto utt_client_api nlohmann_json::nlohmann_json stdc++fs
)

add_library(privacy-wallet-lib ${privacy-wallet-library-src})
target_include_directories(privacy-wallet-lib PUBLIC include/ ../utt-client-api/include ../utt-common-api/include)

target_link_libraries(privacy-wallet-lib PUBLIC
privacy-wallet-library-proto utt_client_api nlohmann_json::nlohmann_json stdc++fs
)

add_subdirectory(tests)