cmake_minimum_required (VERSION 3.2)
project(secretsmanager VERSION 0.1.0.0 LANGUAGES CXX)

set(secretsmanager_source_files
    src/secrets_manager_enc.cpp
    src/secrets_manager_plain.cpp
    src/secrets_manager_impl.cpp
    src/base64.cpp
    src/aes.cpp
    src/key_params.cpp)

add_library(secretsmanager STATIC 
    ${secretsmanager_source_files})
add_library(secretsmanager_shared SHARED
    ${secretsmanager_source_files})

if(BUILD_THIRDPARTY)
    add_dependencies(secretsmanager cryptopp)
    add_dependencies(secretsmanager_shared cryptopp)
else()
    find_package(cryptopp REQUIRED)
endif()

target_link_libraries(secretsmanager PUBLIC ${CRYPTOPP_LIBRARIES})
target_include_directories(secretsmanager PUBLIC include ${CRYPTOPP_INCLUDE_DIRS})
target_include_directories(secretsmanager PRIVATE src)

target_link_libraries(secretsmanager_shared PUBLIC ${CRYPTOPP_LIBRARIES})
target_include_directories(secretsmanager_shared PUBLIC include ${CRYPTOPP_INCLUDE_DIRS})

add_subdirectory(secretretriever)
if (BUILD_TESTING)
    add_subdirectory(test)
endif()