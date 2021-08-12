# OpenSSL Library

ExternalProject_Add(openssl
                    PREFIX openssl
                    GIT_REPOSITORY "https://github.com/openssl/openssl.git"
                    GIT_TAG "OpenSSL_1_1_1-stable"
                    GIT_PROGRESS TRUE
                    CONFIGURE_COMMAND ./config --prefix=${THIRDPARTY_INSTALL_DIR} 
                                               --openssldir=${THIRDPARTY_INSTALL_DIR} 
                                               shared zlib
                    BUILD_COMMAND   ${CMAKE_MAKE_PROGRAM} -j${NPROC}
                    BUILD_IN_SOURCE 1
                    INSTALL_COMMAND ""
                    LOG_DOWNLOAD 1
                    LOG_BUILD 1
)

ExternalProject_Get_Property(openssl BINARY_DIR)

# Reslut variables as set by FindOpenSSL.cmake
set(OPENSSL_CRYPTO_LIBRARY  "${BINARY_DIR}/libcrypto.so")
set(OPENSSL_SSL_LIBRARY     "${BINARY_DIR}/libssl.so")
set(OPENSSL_LIBRARIES       "${OPENSSL_CRYPTO_LIBRARY} ${OPENSSL_SSL_LIBRARY}")
set(OPENSSL_INCLUDE_DIR     "${BINARY_DIR}")

set(OPENSSL_CRYPTO_LIBRARY  ${OPENSSL_CRYPTO_LIBRARY}   PARENT_SCOPE)
set(OPENSSL_SSL_LIBRARY     ${OPENSSL_SSL_LIBRARY}      PARENT_SCOPE)
set(OPENSSL_LIBRARIES       ${OPENSSL_LIBRARIES}        PARENT_SCOPE)
set(OPENSSL_INCLUDE_DIR     ${OPENSSL_INCLUDE_DIR}      PARENT_SCOPE)

message(STATUS "OPENSSL_CRYPTO_LIBRARY ${OPENSSL_CRYPTO_LIBRARY}")
message(STATUS "OPENSSL_SSL_LIBRARY    ${OPENSSL_SSL_LIBRARY}")
message(STATUS "OPENSSL_LIBRARIES      ${OPENSSL_LIBRARIES}")
message(STATUS "OPENSSL_INCLUDE_DIR    ${OPENSSL_INCLUDE_DIR}")

# Imported targets as defined by FindOpenSSL.cmake
add_library(OpenSSL::Crypto UNKNOWN IMPORTED GLOBAL)
set_target_properties(OpenSSL::Crypto
                      PROPERTIES
                        INTERFACE_INCLUDE_DIRECTORIES ${OPENSSL_INCLUDE_DIR}
                        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                        IMPORTED_LOCATION ${OPENSSL_CRYPTO_LIBRARY} )

add_library(OpenSSL::SSL UNKNOWN IMPORTED GLOBAL)
set_target_properties(OpenSSL::SSL
                      PROPERTIES
                        INTERFACE_INCLUDE_DIRECTORIES ${OPENSSL_INCLUDE_DIR}
                        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                        IMPORTED_LOCATION ${OPENSSL_SSL_LIBRARY}
                        INTERFACE_LINK_LIBRARIES OpenSSL::Crypto)

add_dependencies(OpenSSL::Crypto openssl)
