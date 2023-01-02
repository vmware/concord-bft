cmake_minimum_required (VERSION 3.2)

project(secretsmanager VERSION 0.1.0.0 LANGUAGES CXX)

add_library(secretsmanager STATIC
    src/secrets_manager_enc.cpp
    src/secrets_manager_plain.cpp
    src/secrets_manager_impl.cpp
    src/base64.cpp
    src/aes.cpp
    )

set_target_properties(secretsmanager PROPERTIES PUBLIC_HEADER "secrets_manager_impl.h;secrets_manager_enc.h;secrets_manager_plain.h;secret_data.h")

target_include_directories(secretsmanager PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
                                                 $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/secrets>
                                          PRIVATE src
)

target_link_libraries(secretsmanager PUBLIC concord-crypto)

install(TARGETS secretsmanager
#       EXPORT  secretsmanager-targets #TODO [TK] generate secretsmanager-config.cmake
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/secrets
        )

if (USE_JSON AND USE_HTTPLIB)
    find_package(nlohmann_json REQUIRED)
    if(NOT BUILD_THIRDPARTY)
        find_package(OpenSSL COMPONENTS SSL REQUIRED)
    endif()
    set(HTTPLIB_IS_USING_OPENSSL TRUE)

    add_library(secret_retriever STATIC src/secret_retriever.cpp)
    set_target_properties(secret_retriever PROPERTIES PUBLIC_HEADER "secret_retriever.hpp;secret_data.h")
    target_include_directories(secret_retriever PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..> 
                                                        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/secrets>
                                                PRIVATE src
    )
    target_compile_definitions(secret_retriever PUBLIC $<$<BOOL:${HTTPLIB_IS_USING_OPENSSL}>:CPPHTTPLIB_OPENSSL_SUPPORT>)
    target_link_libraries(secret_retriever PUBLIC nlohmann_json::nlohmann_json OpenSSL::SSL concord-crypto)

    install(TARGETS secret_retriever
#           EXPORT  secret_retriever-targets #TODO [TK] generate secret_retriever-config.cmake
            PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/secrets)
else ()
    message(STATUS "Not building secretretriever, usage of JSON library and/or httplib is disabled")
endif()

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
