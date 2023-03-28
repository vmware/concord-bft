cmake_minimum_required(VERSION 3.24)
# Boost library

set(BOOST_INCLUDE_LIBRARIES thread program_options lockfree bimap asio algorithm histogram predef)
set(BOOST_ENABLE_CMAKE ON)

set(FETCHCONTENT_QUIET FALSE)
FetchContent_Declare(
    Boost
    GIT_REPOSITORY "https://github.com/boostorg/boost.git"
    GIT_TAG boost-1.80.0
    GIT_SHALLOW TRUE
    GIT_PROGRESS TRUE
    OVERRIDE_FIND_PACKAGE
)
FetchContent_MakeAvailable(Boost)
set(Boost_NO_SYSTEM_PATHS ON CACHE BOOL "don't search boost in a system location" FORCE)
set(Boost_INCLUDE_DIR ${BOOST_LIBRARY_INCLUDES} CACHE PATH "boost include dirs" FORCE)
