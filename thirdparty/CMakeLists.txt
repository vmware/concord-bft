message(STATUS "Build third parites")
include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckIncludeFiles)
include(ExternalProject)
include(ProcessorCount)
include(FetchContent)


include(thirdparty/boost.cmake)
include(thirdparty/yaml-cpp.cmake)
include(thirdparty/openssl.cmake)
