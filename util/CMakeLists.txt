# pthread dependency
find_package(Threads REQUIRED)

add_library(util STATIC src/Metrics.cpp src/MetricsServer.cpp src/SimpleThreadPool.cpp src/Serializable.cpp)

TARGET_COMPILE_DEFINITIONS(util PUBLIC DEFAULT_LOGGER_NAME="${CONCORD_LOGGER_NAME}")

target_link_libraries(util PUBLIC Threads::Threads)
target_include_directories(util PUBLIC include)

if (BUILD_TESTING)
    add_subdirectory(pyclient)
    add_subdirectory(test)
endif()

set_property(DIRECTORY .. APPEND PROPERTY INCLUDE_DIRECTORIES
    ${CMAKE_CURRENT_SOURCE_DIR}/include/)
