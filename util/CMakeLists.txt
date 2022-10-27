# pthread dependency
find_package(Threads REQUIRED)
find_package(Boost ${MIN_BOOST_VERSION} REQUIRED)
set(util_source_files
    src/Metrics.cpp
    src/MetricsServer.cpp
    src/SimpleThreadPool.cpp
    src/histogram.cpp
    src/status.cpp
    src/sliver.cpp
    src/hex_tools.cpp
    src/OpenTracing.cpp
    src/throughput.cpp
    src/crypto/crypto.cpp
    src/openssl/certificates.cpp
    src/RawMemoryPool.cpp
    src/config_file_parser.cpp
    src/io.cpp)
add_library(util STATIC ${util_source_files} src/crypto/factory.cpp)

if(USE_OPENSSL)
    if(NOT BUILD_THIRDPARTY)
        find_package(OpenSSL REQUIRED)
    endif()
    find_library(CRYPTO_STATIC_LIBRARY NAMES libcrypto.a HINTS ${OPENSSL_ROOT_DIR}/lib/  REQUIRED)
    message(STATUS "Using ${CRYPTO_STATIC_LIBRARY} for util library")
    target_sources(util PRIVATE src/openssl/crypto.cpp)
    target_link_libraries(util PUBLIC ${CRYPTO_STATIC_LIBRARY} dl z pthread)
    target_compile_definitions(util PUBLIC USE_OPENSSL)
endif()

target_link_libraries(util PUBLIC
        Threads::Threads
        Boost::headers
        stdc++fs)

target_include_directories(util PUBLIC include include/openssl)

if(USE_OPENTRACING)
    if(NOT DEFINED OPENTRACING_INCLUDE_DIR)
        find_path(OPENTRACING_INCLUDE_DIR NAMES opentracing/tracer.h)
    endif()
    if(NOT DEFINED OPENTRACING_STATIC_LIBRARY)
        find_library(OPENTRACING_STATIC_LIBRARY libopentracing.a)
    endif()
    target_include_directories(util PUBLIC ${OPENTRACING_INCLUDE_DIR})
    target_link_libraries(util PUBLIC ${OPENTRACING_STATIC_LIBRARY})
    target_compile_definitions(util PUBLIC USE_OPENTRACING=1)
endif()

if(BUILD_TESTING)
    add_subdirectory(pyclient)
    add_subdirectory(test)
endif()

set(util_header_files
    include/histogram.hpp
    include/Metrics.hpp
    include/OpenTracing.hpp
    include/SimpleThreadPool.hpp
    include/sliver.hpp
    include/status.hpp
    include/string.hpp)
install(FILES ${util_header_files} DESTINATION include/util)

set_property(DIRECTORY .. APPEND PROPERTY INCLUDE_DIRECTORIES
        ${CMAKE_CURRENT_SOURCE_DIR}/include/)

set_property(GLOBAL PROPERTY UTIL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)

if(USE_PROMETHEUS)
    find_package(prometheus-cpp CONFIG REQUIRED)
    set(util_prometheus_src
            ${util_source_files}
            src/concord_prometheus_metrics.cpp
            )
    add_library(util_prometheus STATIC ${util_prometheus_src})
    target_link_libraries(util_prometheus PUBLIC
            prometheus-cpp::core
            prometheus-cpp::pull
            util
            )
endif()

add_subdirectory(bench)
