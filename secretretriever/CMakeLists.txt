project("Secret Retriever")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
find_package(nlohmann_json REQUIRED)
add_library(secret_retriever STATIC
        "src/secret_retriever.cpp"
        )

add_library(secret_retriever_shared SHARED
        "src/secret_retriever.cpp"
        )
add_subdirectory(httplib)
target_include_directories(secret_retriever PUBLIC include/ httplib/)
target_include_directories(secret_retriever PRIVATE ../secretsmanager/include)
target_link_libraries(secret_retriever PUBLIC
        nlohmann_json::nlohmann_json
        )

target_link_libraries(secret_retriever_shared PUBLIC
        nlohmann_json::nlohmann_json
        )
target_include_directories(secret_retriever_shared PUBLIC include/ httplib/)
target_include_directories(secret_retriever_shared PRIVATE ../secretsmanager/include)
find_package(OpenSSL REQUIRED)
if(OPENSSL_FOUND)
    set(HTTPLIB_IS_USING_OPENSSL TRUE)
endif()

target_compile_definitions(secret_retriever PUBLIC
        $<$<BOOL:${HTTPLIB_IS_USING_OPENSSL}>:CPPHTTPLIB_OPENSSL_SUPPORT>
        )

target_compile_definitions(secret_retriever_shared PUBLIC
        $<$<BOOL:${HTTPLIB_IS_USING_OPENSSL}>:CPPHTTPLIB_OPENSSL_SUPPORT>
        )

install (TARGETS secret_retriever DESTINATION l${LIB_SUFFIX})
install (TARGETS secret_retriever_shared DESTINATION l${LIB_SUFFIX})