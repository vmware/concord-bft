cmake_minimum_required (VERSION 3.2)
# project(<name> VERSION <ver> LANGUAGES CXX)
project(libutt VERSION 0.1.0.0 LANGUAGES CXX)

#
# Configuration options
#
set(
  CURVE
  "BN128"
  CACHE
  STRING
  "Default curve: one of ALT_BN128, BN128, EDWARDS, MNT4, MNT6"
)

option(
  BINARY_OUTPUT
  "In serialization of elliptic curve points, output raw binary data (instead of decimal), which is smaller and faster."
  OFF
)

add_definitions(
  -DCURVE_${CURVE}
)

option(
  USE_MULTITHREADING
  "Enable parallelized execution of DKG protocols using OpenMP"
  OFF
)

if(${CURVE} STREQUAL "BN128")
  add_definitions(
    -DBN_SUPPORT_SNARK=1
  )
endif()

#
# Configure CCache if available
#
# find_program(CCACHE_FOUND ccache)
# if(CCACHE_FOUND)
#         set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
#         set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
# endif(CCACHE_FOUND)

#find_package(Threads REQUIRED)
#find_package(Boost 1.65 COMPONENTS program_options REQUIRED)
#include_directories(${Boost_INCLUDE_DIR})

# OS X Catalina fix
include_directories(SYSTEM "/usr/local/include")
link_directories("/usr/local/lib")

# Note: xassert and xutils are part of utt source code
# set(utt_packages
#     xassert
#     xutils
# )

#
# Dependencies
#
# TODO: Find ate-pairing, libff, libfqfft too or issue error with pointer to install script
# foreach(package ${utt_packages})
#     find_package(${package} QUIET)

#     if(${package}_FOUND)
#         message("${package} library is installed!")
#     else()
#         message("${package} library not installed locally, please download from https//github.com/alinush/lib${package}")
#     endif()
# endforeach()

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
    "-Wconversion -Wsign-conversion -Wsign-compare")
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
    " -O1")
# "It turns out that some gcc builds (depends on the distro) set _FORTIFY_SOURCE internally, so you need to undefine it first. So if you used CFLAGS="-U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=2", it might go better."
# https://github.com/neovim/neovim/issues/2557
string(APPEND CXX_FLAGS_DEBUG 
    " -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=2")

if("${USE_MULTITHREADING}")
  add_definitions(-DUSE_MULTITHREADING)
  string(APPEND CMAKE_CXX_FLAGS " -fopenmp")
endif()

# GNU and Clang-specific flags
string(APPEND CMAKE_CXX_FLAGS 
    " ${CXX_FLAGS}")
string(APPEND CMAKE_CXX_FLAGS_DEBUG 
    " ${CXX_FLAGS_DEBUG} -DUTT_LOG_DEBUG")
# When building with 'cmake -DCMAKE_BUILD_TYPE=Trace'
string(APPEND CMAKE_CXX_FLAGS_TRACE 
    " ${CXX_FLAGS_DEBUG} -DUTT_LOG_TRACE")

# using Clang
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # Clang-specific options
    string(APPEND CMAKE_CXX_FLAGS 
        " -ferror-limit=3")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG
        # " -fstack-protector-all -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls")
        " -fstack-protector-all -fno-omit-frame-pointer -fno-optimize-sibling-calls -ggdb -O0")
    # TODO: doesn't seem to work on MacOS, getting strange linking errors
    #string(APPEND CMAKE_CXX_FLAGS_DEBUG 
    #    " -D_LIBCPP_DEBUG=0")
    #string(APPEND CMAKE_CXX_FLAGS_DEBUG 
    #    " -D_LIBCPP_DEBUG=1")

# using GCC
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # GCC-specific options
    string(APPEND CMAKE_CXX_FLAGS 
        " -fmax-errors=3")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG 
        " -fstack-protector-all")
    # NOTE: define _GLIBCXX_DEBUG if you want bounds checking for std::vector::operator[] (but it seems to cause a segfault at the end of execution)
    # only works for libstdc++ from the GNU compiler, I think
    string(APPEND CMAKE_CXX_FLAGS_DEBUG 
        " -D_GLIBCXX_DEBUG")

# TODO: using Intel C++
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Intel")

# TODO: using Visual Studio C++
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")

endif()

#
# Testing flags
#
enable_testing()

# Subdirectories
add_subdirectory(libutt)
add_subdirectory(libxassert)
add_subdirectory(libxutils)

set(newutt_src 
    libutt/src/details.cpp
    libutt/src/nullifier.cpp
    libutt/src/commitment.cpp
    libutt/src/clientIdentity.cpp
    libutt/src/bankIdentity.cpp
    libutt/src/mint.cpp
    libutt/src/coin.cpp
    libutt/src/registratorIdentity.cpp
    libutt/src/common.cpp
    )
add_library(newutt STATIC ${newutt_src})
target_link_libraries(newutt PUBLIC utt)
target_include_directories(newutt PUBLIC include)
add_subdirectory(tests)



