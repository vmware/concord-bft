add_library(util STATIC src/Metrics.cpp)

target_include_directories(util PUBLIC include)

if (BUILD_TESTING)
    add_subdirectory(pyclient)
    add_subdirectory(test)
endif()
