message(STATUS "Build third parites")
include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckIncludeFiles)
include(ExternalProject)
include(ProcessorCount)

ProcessorCount(NPROC)

set(THIRDPARTY_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR})

include(gmp.cmake)
include(relic.cmake)
include(cryptopp.cmake)
include(openssl.cmake)
