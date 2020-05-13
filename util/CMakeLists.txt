# pthread dependency
find_package(Threads REQUIRED)

add_library(util STATIC
    src/Metrics.cpp
    src/MetricsServer.cpp
    src/SimpleThreadPool.cpp
    src/histogram.cpp
    src/status.cpp
    src/sliver.cpp
    src/hex_tools.cpp)

if(BUILD_ROCKSDB_STORAGE)
    target_compile_definitions(util PUBLIC "USE_ROCKSDB=1")
endif()

target_link_libraries(util PUBLIC Threads::Threads)
target_include_directories(util PUBLIC include)

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
endif()

if (BUILD_TESTING)
    add_subdirectory(pyclient)
    add_subdirectory(test)
endif()

set_property(DIRECTORY .. APPEND PROPERTY INCLUDE_DIRECTORIES
    ${CMAKE_CURRENT_SOURCE_DIR}/include/)
