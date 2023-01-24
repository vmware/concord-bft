add_subdirectory("proto")

set(utt-wallet-cli-src
    src/main.cpp
    src/wallet.cpp
)
find_package(nlohmann_json REQUIRED)
add_executable(utt-wallet-cli ${utt-wallet-cli-src})

target_include_directories(utt-wallet-cli PUBLIC
 include/ ../utt-client-api/include ../utt-common-api/include ../privacy-wallet-lib/include
 )

target_link_libraries(utt-wallet-cli PUBLIC
  utt-wallet-api-proto utt_client_api privacy_wallet_lib nlohmann_json::nlohmann_json stdc++fs
)

add_library(utt-wallet-lib ${utt-wallet-cli-src})
target_include_directories(utt-wallet-lib PUBLIC
 include/ ../utt-client-api/include ../utt-common-api/include ../privacy-wallet-lib/include
 )

target_link_libraries(utt-wallet-lib PUBLIC
  utt-wallet-api-proto utt_client_api privacy_wallet_lib nlohmann_json::nlohmann_json stdc++fs
)

add_subdirectory(tests)