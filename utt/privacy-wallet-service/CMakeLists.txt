add_subdirectory("proto")

set(privacy-wallet-service-src
    src/main.cpp
    src/Wallet.cpp
    src/PrivacyService.cpp
)
find_package(nlohmann_json REQUIRED)
add_executable(privacy-wallet-service ${privacy-wallet-service-src})

target_include_directories(privacy-wallet-service PUBLIC 
  include/ ../utt-client-api/include ../utt-common-api/include ../privacy-wallet-lib/include
)

target_link_libraries(privacy-wallet-service PUBLIC
  privacy-wallet-service-proto utt_client_api privacy_wallet_lib nlohmann_json::nlohmann_json stdc++fs
)

#add_library(privacy-wallet-service-lib ${privacy-wallet-service-src})
#target_link_libraries(privacy-wallet-service-lib PUBLIC
#  privacy-wallet-library-proto utt_client_api privacy_wallet_lib nlohmann_json::nlohmann_json stdc++fs
#)
# fixme
#add_subdirectory(tests)