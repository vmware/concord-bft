cmake_minimum_required (VERSION 3.2)
# project(<name> VERSION <ver> LANGUAGES CXX)
project(libxutils VERSION 0.1.0.0 LANGUAGES CXX)

#
# Configuration options
#

#
# Configure CCache if available
#
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

#
# Dependencies
#
find_package(xassert QUIET)

if(xassert_FOUND)
    message("xassert library is installed!")
else()
    message("xassert library not installed. Please download, build and install from https://github.com/alinush/xassert")
endif()

#find_package(Threads REQUIRED)
#find_package(Boost 1.65 COMPONENTS program_options REQUIRED)
#include_directories(${Boost_INCLUDE_DIR})

# TODO: there should be better ways of dealing with this
include_directories(SYSTEM "/usr/local/include") # because OS X Catalina is a pain in the ***

#
# C++ options
#   TODO: change to set_target_properties?
#   https://crascit.com/2015/03/28/enabling-cxx11-in-cmake/
#
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

#
# Compiler flags
#

# When you do 'a > b in 'C/C++, if a is unsigned and b is signed and equal to -1, C/C++
# actually casts b to unsigned (probably because casting unsigned to signed would require a bigger data type)
# Thus, 1 > -1 will evaluate to false because during the cast -1 will be set to to 2^32 - 1
#
# WARNING: For the love of god, do not remove this flag or you will regret it. Instead,
# just use signed types everywhere and cast your unsigned to signed when mixing unsigned
# variables with signed ones. See: http://soundsoftware.ac.uk/c-pitfall-unsigned
set(CXX_FLAGS_INTEGER_CORRECTNESS 
    "-Wconversion -Wsign-conversion")
set(CXX_FLAGS_FORMAT 
    "-Wformat-y2k -Wno-format-extra-args -Wno-format-zero-length -Wformat-nonliteral -Wformat-security -Wformat=2")
set(CXX_FLAGS_OPTIMIZATIONS "-O3")

string(APPEND CXX_FLAGS " ${CXX_FLAGS_OPTIMIZATIONS}")
string(APPEND CXX_FLAGS " ${CXX_FLAGS_FORMAT}")
string(APPEND CXX_FLAGS " ${CXX_FLAGS_INTEGER_CORRECTNESS}")
# TODO: use target_compile_features instead: 
#   https://cmake.org/cmake/help/v3.1/command/target_compile_features.html#command:target_compile_features
#   https://cmake.org/cmake/help/v3.1/prop_gbl/CMAKE_CXX_KNOWN_FEATURES.html#prop_gbl:CMAKE_CXX_KNOWN_FEATURES
string(APPEND CXX_FLAGS " -Wall")
string(APPEND CXX_FLAGS " -Werror")
string(APPEND CXX_FLAGS " -Wextra")


# TODO: Figure out right way to deal with -fstrict-overflow / -Wstrict-overflow related errors 
string(APPEND CXX_FLAGS 
    " -fno-strict-overflow")
string(APPEND CXX_FLAGS_DEBUG 
    " -D_FORTIFY_SOURCE=2")

# GNU and Clang-specific flags
string(APPEND CMAKE_CXX_FLAGS 
    " ${CXX_FLAGS}")
string(APPEND CMAKE_CXX_FLAGS_DEBUG 
    " ${CXX_FLAGS_DEBUG}")
# When building with 'cmake -DCMAKE_BUILD_TYPE=Trace'
string(APPEND CMAKE_CXX_FLAGS_TRACE 
    " ${CXX_FLAGS_DEBUG} -DTRACE")

# using Clang
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # Clang-specific options
    string(APPEND CMAKE_CXX_FLAGS 
        " -ferror-limit=3")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG
        " -fstack-protector-all")

# using GCC
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # GCC-specific options
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
# Testing flags
#
enable_testing()

add_subdirectory(libxutils)
