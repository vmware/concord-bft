# pthread dependency
find_package(nlohmann_json REQUIRED)
find_package(cryptopp REQUIRED)

set(secretsmanager_source_files
    src/secrets_manager_enc.cpp
    src/secrets_manager_plain.cpp
    src/secrets_manager_impl.cpp
    src/base64.cpp
    src/aes.cpp
    src/openssl_pass.cpp)

add_library(secretsmanager STATIC 
    ${secretsmanager_source_files})
add_library(secretsmanager_shared SHARED
    ${secretsmanager_source_files})

target_link_libraries(secretsmanager PUBLIC ${CRYPTOPP_LIBRARIES})
target_include_directories(secretsmanager PUBLIC include ${CRYPTOPP_INCLUDE_DIRS})

target_link_libraries(secretsmanager_shared PUBLIC ${CRYPTOPP_LIBRARIES})
target_include_directories(secretsmanager_shared PUBLIC include ${CRYPTOPP_INCLUDE_DIRS})

if (BUILD_TESTING)
    add_subdirectory(test)
endif()