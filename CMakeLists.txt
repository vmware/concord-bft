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
set(SLEEP_FOR_DBG FALSE)

set(MIN_BOOST_VERSION 1.64)

# Default to debug builds
# Release builds can be enabled by running cmake with -DCMAKE_BUILD_TYPE=Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Enable debug or release builds" FORCE)
endif()

if(NOT DEFINED USE_LOG4CPP)
    option(USE_LOG4CPP "Enable LOG4CPP" ON)
endif()

option(USE_RELIC "Enable usage of Relic library for BLS threshold signature generation/verification" OFF)
option(RUN_APOLLO_TESTS "Enable Apollo tests run" ON)
option(KEEP_APOLLO_LOGS "Retains logs from replicas in separate folder for each test in build/tests/apollo/logs" ON)
option(TXN_SIGNING_ENABLED "Enable External concord client transcattion signing" ON)
option(BUILD_COMM_TCP_PLAIN "Enable TCP communication" OFF)
option(LEAKCHECK "Enable Address and Leak Sanitizers" OFF)
option(THREADCHECK "Enable Thread Sanitizer" OFF)
option(BUILD_COMM_TCP_TLS "Enable TCP TLS communication" OFF)
# Rocksdb is required for storage now. Consider removing this flag.
option(BUILD_ROCKSDB_STORAGE "Enable building of RocksDB storage library" ON)
option(USE_S3_OBJECT_STORE "Enable S3 Object Store" ON)
option(BUILD_SLOWDOWN "Build Slowdown framework" OFF)
option(USE_FAKE_CLOCK_IN_TIME_SERVICE "BUILD TimeService Using Fake Clock" OFF)
option(USE_OPENTRACING "Enable OpenTracing" ON)
option(USE_PROMETHEUS "Enable Prometheus" ON)
option(USE_JAEGER "Enable Jaeger" ON)
option(USE_JSON "Enable use of JSON library" ON)
option(USE_HTTPLIB "Enable use of httplib" ON)
option(USE_GRPC "Enable GRPC and Protobuf" ON)
option(USE_OPENSSL "Enable use of OpenSSL" ON)
option(USE_EDDSA_OPENSSL "Enable use of EdDSA's OpenSSL implementation for signature generation and verification" ON)
option(BUILD_THIRDPARTY "Wheter to build third party librarie or use preinstalled ones" OFF)
option(CODECOVERAGE "Enable Code Coverage Metrics in Clang" OFF)
option(ENABLE_RESTART_RECOVERY_TESTS "Enable tests for restart recovery" OFF)

if(NOT (USE_EDDSA_OPENSSL OR USE_RELIC))
    message(FATAL_ERROR "At least one threshold/multisig implementation needs to be chosen, "
            "Define at least one of the following cmake options: [USE_EDDSA_OPENSSL, USE_RELIC]")
endif()

if((USE_OPENSSL OR USE_EDDSA_OPENSSL) AND NOT BUILD_THIRDPARTY)
    set(OPENSSL_ROOT_DIR /usr/local/ssl) # not to confuse with system ssl libs
endif()

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
string(APPEND CMAKE_CXX_FLAGS " -fno-omit-frame-pointer")
if(LEAKCHECK AND THREADCHECK)
    message(FATAL_ERROR "Cannot have both Thread and Address/Leak Sanitizers Enabled")
endif()
if(LEAKCHECK)
    string(APPEND CMAKE_CXX_FLAGS " -fsanitize=leak -fsanitize=address")
    message("-- Address and Leak Sanitizers Enabled")
endif()
if(THREADCHECK)
    string(APPEND CMAKE_CXX_FLAGS " -fsanitize=thread")
    message("-- Thread Sanitizer Enabled")
endif()

if(OMIT_TEST_OUTPUT)
    message("-- OMIT_TEST_OUTPUT Enabled")
endif()
if(KEEP_APOLLO_LOGS)
    message("-- KEEP_APOLLO_LOGS Enabled")
endif()
if(RUN_APOLLO_TESTS)
    message("-- RUN_APOLLO_TESTS Enabled")
endif()

if(CODECOVERAGE AND THREADCHECK)
    message(FATAL_ERROR "Cannot have both Thread and Code Coverage Enabled")
endif()
if(LEAKCHECK AND CODECOVERAGE)
    message(FATAL_ERROR "Cannot have both Code Coverage and Address/Leak Sanitizers Enabled")
endif()
if(CODECOVERAGE)
    string(APPEND CMAKE_CXX_FLAGS " -fprofile-instr-generate -fcoverage-mapping")
    string(APPEND CMAKE_EXE_LINKER_FLAGS " -fprofile-instr-generate")
    message( "-- Building with llvm Code Coverage Tools")
endif()

if(USE_S3_OBJECT_STORE)
    add_compile_definitions(USE_S3_OBJECT_STORE=1)
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

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

include(CTest)
#
# Subdirectories
#
if(BUILD_THIRDPARTY)
	add_subdirectory(thirdparty)
endif()

add_subdirectory(logging)
add_subdirectory(util)
add_subdirectory(secretsmanager)
add_subdirectory(threshsign)
add_subdirectory(communication)
add_subdirectory(kvbc)
add_subdirectory(performance)
add_subdirectory(bftengine)
add_subdirectory(tools)
add_subdirectory(storage)
add_subdirectory(scripts)
add_subdirectory(diagnostics)
add_subdirectory(reconfiguration)
add_subdirectory(client)
if(USE_GRPC)
	add_subdirectory(thin-replica-server)
endif()
add_subdirectory(ccron)

#
# Setup testing
#

option(OMIT_TEST_OUTPUT "Forwards output stdout and stdin to /dev/null" OFF)

if(BUILD_TESTING)
    add_subdirectory(bftengine/tests)
    add_subdirectory(tests)
    add_subdirectory(messages)
endif()
