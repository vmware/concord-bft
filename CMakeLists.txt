cmake_minimum_required(VERSION 3.2)

# Default to building with clang on Linux,OSX,BSD etc...
# This must come before the project definition or it won't be picked up.
if(NOT ENV{CXX} AND UNIX)
    set(ENV{CXX} clang++)
endif()

project(concord-bft VERSION 0.1.0.0 LANGUAGES CXX)

#
# C++ options
#   TODO: change to set_target_properties?
#   https://crascit.com/2015/03/28/enabling-cxx11-in-cmake/
#
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


set(IS_LEAKCHECK FALSE)
set(BUILD_COMM_TCP_TLS FALSE)
set(SLEEP_FOR_DBG FALSE)

# Default to debug builds
# Release builds can be enabled by running cmake with -DCMAKE_BUILD_TYPE=Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "ENABLE DEBUG BUILDS" FORCE)
endif()

#if not set to TRUE by the app, set from here
if(NOT USE_LOG4CPP)
    set(USE_LOG4CPP FALSE)
endif()
if(NOT BUILD_COMM_TCP_PLAIN)
    set(BUILD_COMM_TCP_PLAIN FALSE)
endif()

if(NOT CONCORD_LOGGER_NAME)
    set(CONCORD_LOGGER_NAME "concord")
endif()

if(SLEEP_FOR_DBG)
    add_definitions(-DSLEEP_DBG)
endif()

#
# Compiler options
#
#

string(APPEND CXX_FLAGS " -Wall")
string(APPEND CXX_FLAGS " -Wbuiltin-macro-redefined")
if(IS_LEAKCHECK)
string(APPEND CXX_FLAGS " -fsanitize=leak")
endif()
#string(APPEND CXX_FLAGS " -Werror")
#string(APPEND CXX_FLAGS " -Wextra")

# TODO: Figure out right way to deal with -fstrict-overflow / -Wstrict-overflow related errors
#string(APPEND CXX_FLAGS
#    " -fno-strict-overflow")
# Prevents some buffer overflows: https://access.redhat.com/blogs/766093/posts/1976213
string(APPEND CXX_FLAGS_DEBUG
    " -D_FORTIFY_SOURCE=2")

# GCC and clang-specific options
string(APPEND CMAKE_CXX_FLAGS
    " ${CXX_FLAGS}")
string(APPEND CMAKE_CXX_FLAGS_DEBUG
    " ${CXX_FLAGS_DEBUG}")

# using Clang
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")

    # Clang-specific options
    string(APPEND CMAKE_CXX_FLAGS
        " -pedantic")
    string(APPEND CMAKE_CXX_FLAGS
        " -ferror-limit=3")
    # Our RELIC library used in threshsign is in the habit of picking generic
    # macro names like HASH and ALIGNED, which conflicts with our own code or
    # other libraries. Even worse, compilers don't show 'macro redefined' warnings
    # for system header files such as our installed RELIC library. So we do this:
    string(APPEND CMAKE_CXX_FLAGS
        " --no-system-header-prefix relic")
    string(APPEND CMAKE_CXX_FLAGS
        " -Wmacro-redefined")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG
        " -fstack-protector-all")

# using GCC
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")

    # GCC-specific options
    string(APPEND CMAKE_CXX_FLAGS
        " -pedantic")
    string(APPEND CMAKE_CXX_FLAGS
        " -fmax-errors=3")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG
        " -fstack-protector-all")

# TODO: using Intel C++
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Intel")

# TODO: using Visual Studio C++
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")

endif()


#
# External dependencies
#
#find_package(cryptopp 5.6.5 EXACT REQUIRED)
#find_package(relic REQUIRED)
#find_package(threshsign REQUIRED)

#
# Subdirectories
#
add_subdirectory(logging)
add_subdirectory(threshsign)
add_subdirectory(bftengine)
add_subdirectory(tools)
add_subdirectory(bftengine/tests)
