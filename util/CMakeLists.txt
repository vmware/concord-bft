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
    src/OpenTracing.cpp)

add_library(util STATIC
    ${util_source_files}
    )

add_library(util_shared SHARED
    ${util_source_files})

if(BUILD_ROCKSDB_STORAGE)
    target_compile_definitions(util PUBLIC "USE_ROCKSDB=1")
    target_compile_definitions(util_shared PUBLIC "USE_ROCKSDB=1")
endif()

find_package(prometheus-cpp CONFIG REQUIRED)
target_link_libraries(util PUBLIC prometheus-cpp::core)
target_link_libraries(util_shared PUBLIC prometheus-cpp::core)

target_link_libraries(util PUBLIC Threads::Threads)
target_include_directories(util PUBLIC include)

target_link_libraries(util_shared PUBLIC Threads::Threads)
target_include_directories(util_shared PUBLIC include)
target_include_directories(util_shared PUBLIC ${CRYPTOPP_INCLUDE_DIRS})

if (USE_OPENTRACING)
  if (NOT DEFINED OPENTRACING_INCLUDE_DIR)
        find_path(OPENTRACING_INCLUDE_DIR NAMES opentracing/tracer.h)
    endif()
    if (NOT DEFINED OPENTRACING_LIBRARY)
        find_library(OPENTRACING_LIBRARY opentracing)
    endif()
    target_include_directories(util PUBLIC ${OPENTRACING_INCLUDE_DIR})
    target_link_libraries(util PUBLIC ${OPENTRACING_LIBRARY})
    target_compile_definitions(util PUBLIC USE_OPENTRACING=1)
    target_include_directories(util_shared PUBLIC ${OPENTRACING_INCLUDE_DIR})
    target_link_libraries(util_shared PUBLIC ${OPENTRACING_LIBRARY})
    target_compile_definitions(util_shared PUBLIC USE_OPENTRACING=1)
endif()

if (BUILD_TESTING)
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
install (TARGETS util_shared DESTINATION lib${LIB_SUFFIX}) 
