message(STATUS "Build third parites")
include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckIncludeFiles)
include(ExternalProject)
include(ProcessorCount)

ProcessorCount(NPROC)

set(THIRDPARTY_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR})
file(MAKE_DIRECTORY ${THIRDPARTY_INSTALL_DIR}/include)


ExternalProject_Add(libsodium
                    PREFIX libsodium
                    URL "https://download.libsodium.org/libsodium/releases/libsodium-1.0.18.tar.gz"
                    CONFIGURE_COMMAND ./configure --prefix=${THIRDPARTY_INSTALL_DIR}
                    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -j${NPROC}
                    BUILD_IN_SOURCE 1
                    LOG_DOWNLOAD 1
                    LOG_BUILD 1
)

ExternalProject_Add(ntl
                    PREFIX ntl
                    URL "https://libntl.org/ntl-11.5.1.tar.gz"
                    CONFIGURE_COMMAND cd src && ./configure PREFIX=${THIRDPARTY_INSTALL_DIR}
                    BUILD_IN_SOURCE 1
                    LOG_DOWNLOAD 1
                    LOG_BUILD 1
                    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -C src -j${NPROC}
                    INSTALL_COMMAND ${CMAKE_MAKE_PROGRAM} -C src install
)
set(NTL_INCLUDE_DIR ${THIRDPARTY_INSTALL_DIR}/include)
set(NTL_LIBRARY ${THIRDPARTY_INSTALL_DIR}/lib/libntl.a)
message(STATUS "NTL_INCLUDE_DIR ${NTL_INCLUDE_DIR}")
message(STATUS "NTL_LIBRARY ${NTL_LIBRARY}")


ExternalProject_Add(ate_pairing
                    PREFIX ate_pairing
                    GIT_REPOSITORY "https://github.com/herumi/ate-pairing.git"
                    GIT_TAG "530223d7502e95f6141be19addf1e24d27a14d50"
                    CONFIGURE_COMMAND ""
                    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -C src -j${NPROC} DBG=on SUPPORT_SNAKR=1 
                    BUILD_IN_SOURCE 1
                    LOG_DOWNLOAD 1
                    LOG_BUILD 1
                    INSTALL_COMMAND install -p -D -t ${THIRDPARTY_INSTALL_DIR}/include/ate-pairing/include 
                                                  include/bn.h include/zm.h include/zm2.h &&
                                    install -p -D -t ${THIRDPARTY_INSTALL_DIR}/lib lib/libzm.a
)

set(ZM_INCLUDE_DIR ${THIRDPARTY_INSTALL_DIR}/include)
set(ZM_LIBRARY ${THIRDPARTY_INSTALL_DIR}/lib/libzm.a)
message(STATUS "ZM_INCLUDE_DIR ${ZM_INCLUDE_DIR}")
message(STATUS "ZM_LIBRARY ${ZM_LIBRARY}")

ExternalProject_Add(libff
                    PREFIX libff
                    GIT_REPOSITORY "https://github.com/scipr-lab/libff.git"
                    GIT_TAG "a152abfcef21b7778cece96fe77f5e0f819ba79e"
                    GIT_PROGRESS TRUE
                    LOG_DOWNLOAD 1
                    LOG_BUILD 1
                    # WARNING: Does not link correctly with -DPERFORMANCE=ON
                    CMAKE_ARGS  -DCMAKE_BUILD_TYPE=RelWithDebInfo
                                -DCMAKE_INSTALL_PREFIX:FILEPATH=${THIRDPARTY_INSTALL_DIR}
                                -DIS_LIBFF_PARENT=OFF
                                -DBINARY_OUTPUT=OFF
                                -DNO_PT_COMPRESSION=ON
                                -DCMAKE_CXX_FLAGS="-Wno-unused-parameter -Wno-unused-value -Wno-unused-variable"
                                -DUSE_ASM=ON
                                -DPERFORMANCE=OFF
                                -DMULTICORE=OFF
                                -DCURVE=BN128
                                -DWITH_PROCPS=OFF
                    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -j${NPROC}
                    DEPENDS libsodium
                    DEPENDS ate_pairing
)

set(LIBFF_INCLUDE_DIR ${THIRDPARTY_INSTALL_DIR}/include)
set(LIBFF_LIBRARY ${THIRDPARTY_INSTALL_DIR}/lib/libff.a)
message(STATUS "LIBFF_INCLUDE_DIR ${LIBFF_INCLUDE_DIR}")
message(STATUS "LIBFF_LIBRARY ${LIBFF_LIBRARY}")

# Fast Fourier transforms in finite fields (used in libutt)
ExternalProject_Add(libfqfft
                    PREFIX libfqfft
                    GIT_REPOSITORY "https://github.com/alinush/libfqfft.git"
                    GIT_TAG "1ebd069d2a00254558998c93767efbbbd51f250a"
                    CONFIGURE_COMMAND ""
                    BUILD_COMMAND "" 
                    LOG_DOWNLOAD 1
                    INSTALL_COMMAND cp -rp <SOURCE_DIR>/libfqfft ${THIRDPARTY_INSTALL_DIR}/include
)
