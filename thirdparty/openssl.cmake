include(ExternalProject)

ExternalProject_Add( OpenSSL
                     PREFIX openssl
                     GIT_REPOSITORY https://github.com/openssl/openssl.git
                     GIT_TAG OpenSSL_1_1_1-stable
                     GIT_PROGRESS TRUE
                     UPDATE_COMMAND ""
                     CONFIGURE_COMMAND ./config
                     BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -j${NPROC}
                     BUILD_IN_SOURCE 1
                     TEST_COMMAND ""
                     INSTALL_COMMAND ""
)

# We cannot use find_library because ExternalProject_Add() is performed at build time.
# And to please the property INTERFACE_INCLUDE_DIRECTORIES,
# we make the include directory in advance.
ExternalProject_Get_Property(OpenSSL SOURCE_DIR BINARY_DIR)
message (STATUS "Source: ${SOURCE_DIR}")

#set(OPENSSL_ROOT_DIR ${SOURCE_DIR} CACHE PATH "openssl root dir" FORCE)
set(OPENSSL_FOUND TRUE CACHE BOOL "openssl found" FORCE)
set(OPENSSL_USE_STATIC_LIBS TRUE)

set(OPENSSL_INCLUDE_DIR ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${OPENSSL_INCLUDE_DIR})

add_library(OpenSSL::SSL STATIC IMPORTED GLOBAL)
set_property(TARGET OpenSSL::SSL PROPERTY IMPORTED_LOCATION ${SOURCE_DIR}/libssl${CMAKE_STATIC_LIBRARY_SUFFIX})
set_property(TARGET OpenSSL::SSL PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${OPENSSL_INCLUDE_DIR})
add_dependencies(OpenSSL::SSL OpenSSL)

add_library(OpenSSL::Crypto STATIC IMPORTED GLOBAL)
set_property(TARGET OpenSSL::Crypto PROPERTY IMPORTED_LOCATION ${SOURCE_DIR}/libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX})
set_property(TARGET OpenSSL::Crypto PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${OPENSSL_INCLUDE_DIR})
add_dependencies(OpenSSL::Crypto OpenSSL)
