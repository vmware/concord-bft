# pthread dependency
find_package(Threads REQUIRED)

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
        src/crypto_utils.cpp
        src/RawMemoryPool.cpp
        src/config_file_parser.cpp
        src/Digest.cpp
        )


add_library(util STATIC ${util_source_files})
add_library(util_shared SHARED ${util_source_files})

if(USE_OPENSSL)
    if(NOT BUILD_THIRDPARTY)
        find_package(OpenSSL REQUIRED)
    endif()
    target_sources(util PRIVATE src/openssl_crypto.cpp)
    target_sources(util_shared PRIVATE src/openssl_crypto.cpp)
    target_link_libraries(util PUBLIC OpenSSL::Crypto)
    target_link_libraries(util_shared PUBLIC OpenSSL::Crypto)
    target_compile_definitions(util PUBLIC USE_OPENSSL)
    target_compile_definitions(util_shared PUBLIC USE_OPENSSL)
endif()

if(BUILD_THIRDPARTY)
    add_dependencies(util cryptopp)
    add_dependencies(util_shared cryptopp)
else()
    find_package(cryptopp REQUIRED)
endif()

target_link_libraries(util PUBLIC Threads::Threads ${CRYPTOPP_LIBRARIES})
target_include_directories(util PUBLIC include ${CRYPTOPP_INCLUDE_DIRS})

target_link_libraries(util_shared PUBLIC Threads::Threads ${CRYPTOPP_LIBRARIES})
target_include_directories(util_shared PUBLIC include ${CRYPTOPP_INCLUDE_DIRS})

if(USE_OPENTRACING)
    if(NOT DEFINED OPENTRACING_INCLUDE_DIR)
        find_path(OPENTRACING_INCLUDE_DIR NAMES opentracing/tracer.h)
    endif()
    if(NOT DEFINED OPENTRACING_LIBRARY)
        find_library(OPENTRACING_LIBRARY opentracing)
    endif()
    target_include_directories(util PUBLIC ${OPENTRACING_INCLUDE_DIR})
    target_link_libraries(util PUBLIC ${OPENTRACING_LIBRARY})
    target_compile_definitions(util PUBLIC USE_OPENTRACING=1)
    target_include_directories(util_shared PUBLIC ${OPENTRACING_INCLUDE_DIR})
    target_link_libraries(util_shared PUBLIC ${OPENTRACING_LIBRARY})
    target_compile_definitions(util_shared PUBLIC USE_OPENTRACING=1)
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
        include/string.hpp
        include/openssl_crypto.hpp
        include/Digest.hpp
        include/DigestType.hpp)
install(FILES ${util_header_files} DESTINATION include/util)

set_property(DIRECTORY .. APPEND PROPERTY INCLUDE_DIRECTORIES
        ${CMAKE_CURRENT_SOURCE_DIR}/include/)
install(TARGETS util_shared DESTINATION lib${LIB_SUFFIX})

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
