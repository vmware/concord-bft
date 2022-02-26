project (libbftcommunication LANGUAGES CXX)

set(bftcommunication_src
  src/CommFactory.cpp
  src/PlainUDPCommunication.cpp
)

if(BUILD_COMM_TCP_PLAIN)
    set(bftcommunication_src ${bftcommunication_src} src/PlainTcpCommunication.cpp)
endif()
if(BUILD_COMM_TCP_TLS)
    set(bftcommunication_src ${bftcommunication_src}
        src/TlsTCPCommunication.cpp
        src/TlsMultiplexCommunication.cpp
        src/TlsRunner.cpp
        src/TlsConnectionManager.cpp
        src/AsyncTlsConnection.cpp
    )
endif()

add_library(bftcommunication ${bftcommunication_src})
add_library(bftcommunication_shared SHARED ${bftcommunication_src})
set_property(TARGET bftcommunication_shared PROPERTY POSITION_INDEPENDENT_CODE ON)

target_include_directories(bftcommunication PUBLIC include)
target_include_directories(bftcommunication PUBLIC ${util}/include)
target_include_directories(bftcommunication PUBLIC ../secretsmanager/include)
target_link_libraries(bftcommunication PUBLIC util)

target_include_directories(bftcommunication_shared PUBLIC include)
target_include_directories(bftcommunication_shared PUBLIC ${util}/include)
target_include_directories(bftcommunication_shared PUBLIC ../secretsmanager/include)
target_link_libraries(bftcommunication_shared PUBLIC util_shared)

if(BUILD_COMM_TCP_TLS)
    target_link_libraries(bftcommunication PUBLIC secretsmanager)
    target_link_libraries(bftcommunication_shared PUBLIC secretsmanager_shared)
endif()

set(Boost_USE_STATIC_LIBS OFF) # find all kind of libs
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS system filesystem REQUIRED)
target_include_directories(bftcommunication PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(bftcommunication PUBLIC ${Boost_LIBRARIES})
target_include_directories(bftcommunication_shared PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(bftcommunication_shared PUBLIC ${Boost_LIBRARIES})
if(BUILD_COMM_TCP_PLAIN)
    target_compile_definitions(bftcommunication PUBLIC USE_COMM_PLAIN_TCP)
    target_compile_definitions(bftcommunication_shared PUBLIC USE_COMM_PLAIN_TCP)
elseif(BUILD_COMM_TCP_TLS)
    if(NOT USE_OPENSSL)
        message(FATAL_ERROR "-DUSE_OPENSSL should be specified if building with -DBUILD_COMM_TCP_TLS")
    endif()
    if(NOT BUILD_THIRDPARTY)
        find_package(OpenSSL REQUIRED)
    endif()
    target_link_libraries(bftcommunication        PUBLIC OpenSSL::SSL diagnostics)
    target_link_libraries(bftcommunication_shared PUBLIC OpenSSL::SSL diagnostics)
    
    target_compile_definitions(bftcommunication        PUBLIC USE_COMM_TLS_TCP)
    target_compile_definitions(bftcommunication_shared PUBLIC USE_COMM_TLS_TCP)

endif()

install(DIRECTORY include/communication DESTINATION include)
install (TARGETS bftcommunication_shared DESTINATION lib${LIB_SUFFIX})

if (BUILD_TESTING)
    add_subdirectory(tests)
endif()
