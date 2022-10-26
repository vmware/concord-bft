cmake_minimum_required (VERSION 3.2)
project(secretsmanager VERSION 0.1.0.0 LANGUAGES CXX)

set(secretsmanager_source_files
    src/secrets_manager_enc.cpp
    src/secrets_manager_plain.cpp
    src/secrets_manager_impl.cpp
    src/base64.cpp
    src/aes.cpp)

add_library(secretsmanager STATIC ${secretsmanager_source_files})

target_include_directories(secretsmanager PRIVATE include src)
target_link_libraries(secretsmanager PUBLIC util)

if (USE_JSON AND USE_HTTPLIB)
    add_subdirectory(secretretriever)
else ()
    message(STATUS "Not building secretretriever, usage of JSON library and/or httplib is disabled")
endif()

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
