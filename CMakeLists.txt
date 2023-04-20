cmake_minimum_required(VERSION 3.24)
project(concord-bft VERSION 0.1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(SLEEP_FOR_DBG FALSE)
# targets with generic names like "format" may already exist in an imported libs
cmake_policy(SET CMP0002 OLD)
set(ALLOW_DUPLICATE_CUSTOM_TARGETS TRUE)
set(MIN_BOOST_VERSION 1.80)
set(YAML_CPP_VERSION 0.7.0)

# Default to debug builds
# Release builds can be enabled by running cmake with -DCMAKE_BUILD_TYPE=Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Enable debug or release builds" FORCE)
endif()

option(USE_LOG4CPP "Enable LOG4CPP" ON)
option(RUN_APOLLO_TESTS "Enable Apollo tests run" ON)
option(KEEP_APOLLO_LOGS "Retains logs from replicas in separate folder for each test in build/tests/apollo/logs" ON)
option(TXN_SIGNING_ENABLED "Enable External concord client transcattion signing" ON)
option(LEAKCHECK "Enable Address and Leak Sanitizers" OFF)
option(HEAPTRACK "Enable Heaptrack - a heap memory profiler for Linux" OFF)
option(THREADCHECK "Enable Thread Sanitizer" OFF)
option(UNDEFINED_BEHAVIOR_CHECK "Enable Undefined Behavior Sanitizer" OFF)
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
option(BUILD_THIRDPARTY "Whether to build third party libraries or use preinstalled ones" OFF)
option(ENABLE_RESTART_RECOVERY_TESTS "Enable tests for restart recovery" OFF)
option(BUILD_UTT "Build UTT library" ON)
option(BUILD_SHARED_LIBS "whether to create shared libraires" OFF)
if((USE_OPENSSL) AND NOT BUILD_THIRDPARTY)
    set(OPENSSL_ROOT_DIR /usr/local/ssl) # not to confuse with system ssl libs
endif()

if(SLEEP_FOR_DBG)
    add_definitions(-DSLEEP_DBG)
endif()

# include compiler specific options
include(cmake/${CMAKE_CXX_COMPILER_ID}.cmake)
include(cmake/cppcheck.cmake)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
include(cmake/grpc_utils.cmake)

if(BUILD_TESTING)
	include(CTest)
endif()

if(USE_S3_OBJECT_STORE)
    add_compile_definitions(USE_S3_OBJECT_STORE=1)
endif()

#
# Subdirectories
#
# TODO [TK] uncomment when dependencies are compiled by this CMake
#set (CMAKE_FIND_USE_CMAKE_SYSTEM_PATH FALSE)
if(BUILD_THIRDPARTY)
    include(thirdparty/CMakeLists.txt)
endif()

#link all libraries against logging by default
add_subdirectory(libs/log)
link_libraries(logging)
add_subdirectory(libs)
add_subdirectory(kvbc)
add_subdirectory(performance)
add_subdirectory(bftengine)
add_subdirectory(tools)
add_subdirectory(storage)
add_subdirectory(scripts)
add_subdirectory(client)
if(USE_GRPC)
	add_subdirectory(thin-replica-server)
endif()
add_subdirectory(ccron)
if (BUILD_UTT)
    add_subdirectory(utt)
    add_subdirectory(utt-replica)
endif()

#
# Setup testing
#
if(BUILD_TESTING)
	option(OMIT_TEST_OUTPUT "Forwards output stdout and stdin to /dev/null" OFF)
	if(OMIT_TEST_OUTPUT)
	    message("-- OMIT_TEST_OUTPUT Enabled")
	endif()
	if(KEEP_APOLLO_LOGS)
	    message("-- KEEP_APOLLO_LOGS Enabled")
	endif()
	if(RUN_APOLLO_TESTS)
	    message("-- RUN_APOLLO_TESTS Enabled")
	endif()
	
    add_subdirectory(bftengine/tests)
    add_subdirectory(tests)
    add_subdirectory(messages)
    add_subdirectory(examples)
endif()

