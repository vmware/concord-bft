project (libbftcommunication LANGUAGES CXX)

if(BUILD_COMM_TCP_PLAIN AND BUILD_COMM_TCP_TLS)
    message(FATAL_ERROR "Both BUILD_COMM_TCP_PLAIN and BUILD_COMM_TCP_TLS are enabled, while only one can be chosen.")
endif()

add_library(bftcommunication src/CommFactory.cpp) 

if(BUILD_COMM_TCP_PLAIN)
    target_sources(bftcommunication PRIVATE src/PlainTcpCommunication.cpp)
target_compile_definitions(bftcommunication PUBLIC USE_COMM_PLAIN_TCP)
elseif(BUILD_COMM_TCP_TLS)
    target_sources(bftcommunication PRIVATE src/TlsTCPCommunication.cpp
                                            src/TlsMultiplexCommunication.cpp
                                            src/TlsRunner.cpp
                                            src/TlsConnectionManager.cpp
                                            src/AsyncTlsConnection.cpp
    )
    target_compile_definitions(bftcommunication PUBLIC USE_COMM_TLS_TCP)
    target_link_libraries(bftcommunication PUBLIC secretsmanager)
    if(NOT USE_OPENSSL)
        message(FATAL_ERROR "-DUSE_OPENSSL should be specified if building with -DBUILD_COMM_TCP_TLS")
    endif()
    find_package(OpenSSL REQUIRED)
    target_link_libraries(bftcommunication PUBLIC OpenSSL::SSL diagnostics)
    
else()
    # UDP module is not part of the CMake configuration, it will be used by default
    # if neither of plain TCP or TLS will be chosen
    target_sources(bftcommunication PRIVATE src/PlainUDPCommunication.cpp)
    target_compile_definitions(bftcommunication PUBLIC USE_COMM_UDP)
endif()

target_link_libraries(bftcommunication PUBLIC util concord-crypto)

target_include_directories(bftcommunication PUBLIC ../bftengine/include/bftengine)
target_include_directories(bftcommunication PUBLIC include)
target_include_directories(bftcommunication PUBLIC ${util}/include)


install(DIRECTORY include/communication DESTINATION include)

if (BUILD_TESTING)
    add_subdirectory(tests)
endif()
