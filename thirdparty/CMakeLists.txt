message(STATUS "Build third parites")
include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckIncludeFiles)
include(ExternalProject)
include(ProcessorCount)

ProcessorCount(NPROC)

include(gmp.cmake)
include(relic.cmake)
include(cryptopp.cmake)
