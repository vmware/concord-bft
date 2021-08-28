message(STATUS "Build third parites")
include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckIncludeFiles)
include(ExternalProject)
include(ProcessorCount)

ProcessorCount(NPROC)

set(THIRDPARTY_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR})
file(MAKE_DIRECTORY ${THIRDPARTY_INSTALL_DIR}/include)

include(gmp.cmake)
include(relic.cmake)
include(cryptopp.cmake)
include(openssl.cmake)
include(hdrhistogram.cmake)
include(rocksdb.cmake)
