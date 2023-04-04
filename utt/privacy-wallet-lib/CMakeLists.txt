
set(privacy-wallet-library-src
    src/storage/FileBasedUserStorage.cpp
    src/utils/crypto.cpp
)
find_package(OpenSSL REQUIRED)

add_library(privacy_wallet_lib STATIC ${privacy-wallet-library-src})

target_include_directories(privacy_wallet_lib PUBLIC
    include ../utt-common-api/include ../libutt/include ../utt-client-api/include
)

target_link_libraries(privacy_wallet_lib PUBLIC utt_api OpenSSL::Crypto)

if (BUILD_TESTING)
	#add_subdirectory(tests) #TODO[TK] - reenable after ubuntu-22
endif ()