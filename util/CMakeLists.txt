# pthread dependency
find_package(Threads REQUIRED)

add_library(util STATIC src/Metrics.cpp src/MetricsServer.cpp)

target_link_libraries(util PUBLIC logging Threads::Threads)
target_include_directories(util PUBLIC include)

if (BUILD_TESTING)
    add_subdirectory(pyclient)
    add_subdirectory(test)
endif()

if(${USE_LOG4CPP})
    TARGET_COMPILE_DEFINITIONS(util PUBLIC USE_LOG4CPP)
    target_link_libraries(util PUBLIC log4cplus)
endif()
