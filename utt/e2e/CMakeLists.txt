set(e2e-src
    src/main.cpp
    src/test_scenario.cpp
)

add_executable(privacy-e2e ${e2e-src})

target_link_libraries(privacy-e2e PUBLIC
utt_test_utils privacy-admin-api-proto utt-wallet-api-proto utt-wallet-lib privacy-admin-lib utt_client_api
)

target_include_directories(privacy-e2e PUBLIC include/ 
../wallet-api/include ../admin-cli/include ../utt-client-api/include ../utt-common-api/include)
