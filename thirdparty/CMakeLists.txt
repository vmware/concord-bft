message(STATUS "Build third parites")
include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckIncludeFiles)
include(ExternalProject)
include(ProcessorCount)
include(FetchContent)


include(boost.cmake)
include(yaml-cpp.cmake)
