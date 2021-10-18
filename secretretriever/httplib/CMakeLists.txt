project("Httplib")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(httplib STATIC
        "include/httplib.h")
target_include_directories(httplib PUBLIC include/)
find_package(OpenSSL REQUIRED)
if(OPENSSL_FOUND)
    set(HTTPLIB_IS_USING_OPENSSL TRUE)
endif()

target_compile_definitions(httplib PUBLIC
        $<$<BOOL:${HTTPLIB_IS_USING_OPENSSL}>:CPPHTTPLIB_OPENSSL_SUPPORT>
        )

install (TARGETS httplib DESTINATION l${LIB_SUFFIX})
