# Crypto++® Library

ExternalProject_Add(cryptopp-pem
                    PREFIX cryptopp-pem
                    GIT_REPOSITORY "https://github.com/noloader/cryptopp-pem.git"
                    GIT_TAG "CRYPTOPP_8_2_0"
                    CONFIGURE_COMMAND ""
                    BUILD_COMMAND ""
                    INSTALL_COMMAND ""
                    LOG_DOWNLOAD 1
)

ExternalProject_Get_Property(cryptopp-pem SOURCE_DIR)
set(CRYPTOPP_PEM_SOURCE_DIR ${SOURCE_DIR})

ExternalProject_Add(cryptopp
                    PREFIX cryptopp
                    URL "https://github.com/weidai11/cryptopp/archive/CRYPTOPP_8_2_0.tar.gz"
                    PATCH_COMMAND ${CMAKE_COMMAND} -E copy ${CRYPTOPP_PEM_SOURCE_DIR}/pem_common.h
                                                           ${CRYPTOPP_PEM_SOURCE_DIR}/pem.h
                                                           ${CRYPTOPP_PEM_SOURCE_DIR}/pem_common.cpp
                                                           ${CRYPTOPP_PEM_SOURCE_DIR}/pem_read.cpp
                                                           ${CRYPTOPP_PEM_SOURCE_DIR}/pem_write.cpp 
                                                           <SOURCE_DIR>
                    CONFIGURE_COMMAND ""
                    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -j${NPROC}
                    INSTALL_COMMAND ""
                    BUILD_IN_SOURCE 1
                    LOG_DOWNLOAD 1
                    LOG_BUILD 1
                    DEPENDS cryptopp-pem
)

ExternalProject_Get_Property(cryptopp BINARY_DIR)
ExternalProject_Get_Property(cryptopp DOWNLOAD_DIR)
set(CRYPTOPP_LIBRARIES ${BINARY_DIR}/libcryptopp.a PARENT_SCOPE)
set(CRYPTOPP_INCLUDE_DIRS ${DOWNLOAD_DIR} PARENT_SCOPE)
message(STATUS "CRYPTOPP_LIBRARIES ${BINARY_DIR}/libcryptopp.a")
message(STATUS "CRYPTOPP_INCLUDE_DIRS ${DOWNLOAD_DIR}/")
