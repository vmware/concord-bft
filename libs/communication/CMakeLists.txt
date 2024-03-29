project (libbftcommunication LANGUAGES CXX)

option(BUILD_COMM_TCP_PLAIN "Enable plain TCP communication" ON)
option(BUILD_COMM_TCP_TLS "Enable TCP TLS communication" OFF)
option(BUILD_COMM_UDP "Enable plain UDP communication" OFF)

if(BUILD_COMM_TCP_PLAIN AND BUILD_COMM_TCP_TLS)
    message(FATAL_ERROR "Both BUILD_COMM_TCP_PLAIN and BUILD_COMM_TCP_TLS are enabled, while only one can be chosen.")
endif()

add_library(bftcommunication src/CommFactory.cpp) 

if(BUILD_COMM_TCP_PLAIN)
	message(STATUS BUILD_COMM_TCP_PLAIN)
    target_sources(bftcommunication PRIVATE src/PlainTcpCommunication.cpp)
	target_compile_definitions(bftcommunication PUBLIC USE_COMM_PLAIN_TCP)
elseif(BUILD_COMM_TCP_TLS)
	message(STATUS BUILD_COMM_TCP_TLS)
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
    if(NOT BUILD_THIRDPARTY)
        find_package(OpenSSL COMPONENTS SSL REQUIRED)
    endif()
    target_link_libraries(bftcommunication PUBLIC OpenSSL::SSL diagnostics)
    
elseif(BUILD_COMM_UDP)
	message(STATUS BUILD_COMM_UDP)
    # UDP module is not part of the CMake configuration, it will be used by default
    # if neither of plain TCP or TLS will be chosen
    target_sources(bftcommunication PRIVATE src/PlainUDPCommunication.cpp)
    target_compile_definitions(bftcommunication PUBLIC USE_COMM_UDP)
else()
	message(FATAL_ERROR "At least one build option should be chosen for libbftcommunication library [BUILD_COMM_TCP_TLS, BUILD_COMM_TCP_PLAIN, BUILD_COMM_UDP]")
endif()

target_link_libraries(bftcommunication PUBLIC util concord-crypto)

target_include_directories(bftcommunication PUBLIC . ../../bftengine/include)

target_sources(bftcommunication PUBLIC FILE_SET comm_pub_hdrs
                                       TYPE HEADERS
                                       FILES CommDefs.hpp
                                             CommFactory.hpp
                                             ICommunication.hpp
                                             StateControl.hpp
                                             StatusInfo.hpp
             )
install(TARGETS bftcommunication FILE_SET comm_pub_hdrs DESTINATION communication)

if (BUILD_TESTING)
    add_subdirectory(tests)
endif()
