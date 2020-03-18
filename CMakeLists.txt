cmake_minimum_required(VERSION 3.2)
project(concord-bft VERSION 0.1.0.0 LANGUAGES CXX)

#
# C++ options
#   TODO: change to set_target_properties?
#   https://crascit.com/2015/03/28/enabling-cxx11-in-cmake/
#
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(IS_LEAKCHECK FALSE)
set(SLEEP_FOR_DBG FALSE)

set(MIN_BOOST_VERSION 1.64)
option(USE_CONAN "use conan package manager" ON)

# Default to debug builds
# Release builds can be enabled by running cmake with -DCMAKE_BUILD_TYPE=Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Enable debug or release builds" FORCE)
endif()

# Default USE_LOG4CPP to FALSE
if (NOT DEFINED USE_LOG4CPP)
  option(USE_LOG4CPP "Enable LOG4CPP" FALSE)
endif()

# Default BUILD_COMM_TCP_PLAIN to FALSE
option(BUILD_COMM_TCP_PLAIN "Enable TCP communication" FALSE)

# Default BUILD_COMM_TCP_TLS to FALSE
option(BUILD_COMM_TCP_TLS "Enable TCP TLS communication" FALSE)

# This requires the rocksdb dependencies to be installed, so defaults to FALSE
option(BUILD_ROCKSDB_STORAGE "Enable building of RocksDB storage library" FALSE)

set(COMM_MODULES 0)
if(BUILD_COMM_TCP_PLAIN)
    math(EXPR COMM_MODULES "${COMM_MODULES}+1")
endif()

if(BUILD_COMM_TCP_TLS)
    math(EXPR COMM_MODULES "${COMM_MODULES}+1")
endif()

# UDP module is not part of the CMake configuration, it will be used by default
# if neither of plain TCP or TLS will be chosen
if(${COMM_MODULES} GREATER 1)
    message(FATAL_ERROR "Only one comm module can be chosen")
endif()

if(SLEEP_FOR_DBG)
    add_definitions(-DSLEEP_DBG)
endif()

#
# Compiler options
#
#

string(APPEND CMAKE_CXX_FLAGS " -Wall")
string(APPEND CMAKE_CXX_FLAGS " -Wbuiltin-macro-redefined")
string(APPEND CMAKE_CXX_FLAGS " -pedantic")
string(APPEND CMAKE_CXX_FLAGS " -Werror")
if(IS_LEAKCHECK)
    string(APPEND CMAKE_CXX_FLAGS " -fsanitize=leak")
endif()

# TODO: Figure out right way to deal with -fstrict-overflow / -Wstrict-overflow related errors
# string(APPEND CXX_FLAGS " -fno-strict-overflow")
# Prevents some buffer overflows: https://access.redhat.com/blogs/766093/posts/1976213
string(APPEND CMAKE_CXX_FLAGS_RELEASE " -D_FORTIFY_SOURCE=2")

string(APPEND CMAKE_CXX_FLAGS_DEBUG " -fstack-protector-all")


if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")

    string(APPEND CMAKE_CXX_FLAGS " -ferror-limit=3")

    # Our RELIC library used in threshsign is in the habit of picking generic
    # macro names like HASH and ALIGNED, which conflicts with our own code or
    # other libraries. Even worse, compilers don't show 'macro redefined' warnings
    # for system header files such as our installed RELIC library. So we do this:
    # TODO: [TK] move to the threshsign module
    string(APPEND CMAKE_CXX_FLAGS " --no-system-header-prefix relic")

    string(APPEND CMAKE_CXX_FLAGS " -Wmacro-redefined")
    string(APPEND CMAKE_CXX_FLAGS " -Wsign-compare")

    # Export a compile database for use by semantic analysis tools
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    string(APPEND CMAKE_CXX_FLAGS " -fmax-errors=3")
endif()

if (USE_CONAN)
    message("Using conan package manager")
    set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/.conan/cmake_helpers ${CMAKE_MODULE_PATH})
    include(${CMAKE_SOURCE_DIR}/.conan/cmake_helpers/conan.cmake)
    execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/.conan/install_conan_pkgs.sh)
    conan_cmake_run(CONANFILE conanfile.txt
            BASIC_SETUP CMAKE_TARGETS NO_OUTPUT_DIRS
            BUILD missing)
endif()

include(CTest)
#
# Subdirectories
#
add_subdirectory(logging)
add_subdirectory(util)
add_subdirectory(threshsign)
add_subdirectory(bftengine)
add_subdirectory(tools)
add_subdirectory(storage)
add_subdirectory(scripts)
add_subdirectory(kvbc)

#
# Setup testing
#
if(BUILD_TESTING)
    add_subdirectory(bftengine/tests)
    add_subdirectory(tests)
endif()
