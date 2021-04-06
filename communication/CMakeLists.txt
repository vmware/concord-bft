project (libbftcommunication LANGUAGES CXX)

set(bftcommunication_src
  src/CommFactory.cpp
  src/PlainUDPCommunication.cpp
)

if(${BUILD_COMM_TCP_PLAIN})
    set(bftcommunication_src ${bftcommunication_src} src/PlainTcpCommunication.cpp)
endif()
if(${BUILD_COMM_TCP_TLS})
    set(bftcommunication_src ${bftcommunication_src}
        src/TlsTCPCommunication.cpp
        src/TlsTcpImpl.cpp
        src/AsyncTlsConnection.cpp
    )
endif()

add_library(bftcommunication ${bftcommunication_src})
add_library(bftcommunication_shared SHARED ${bftcommunication_src})
set_property(TARGET bftcommunication_shared PROPERTY POSITION_INDEPENDENT_CODE ON)
target_include_directories(bftcommunication PUBLIC include)
target_link_libraries(bftcommunication PUBLIC util)
target_include_directories(bftcommunication_shared PUBLIC include)
target_link_libraries(bftcommunication_shared PUBLIC util_shared)

target_include_directories(bftcommunication PUBLIC ../secretsmanager/include)
target_include_directories(bftcommunication_shared PUBLIC ../secretsmanager/include)
if(${BUILD_COMM_TCP_TLS})
    target_link_libraries(bftcommunication PUBLIC secretsmanager)
    target_link_libraries(bftcommunication_shared PUBLIC secretsmanager_shared)
endif()

if(${BUILD_COMM_TCP_PLAIN} OR ${BUILD_COMM_TCP_TLS})
    set(Boost_USE_STATIC_LIBS OFF) # find all kind of libs
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_USE_STATIC_RUNTIME OFF)
    find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS system filesystem REQUIRED)
    target_include_directories(bftcommunication PUBLIC ${Boost_INCLUDE_DIRS})
    target_link_libraries(bftcommunication PUBLIC ${Boost_LIBRARIES})
    target_include_directories(bftcommunication_shared PUBLIC ${Boost_INCLUDE_DIRS})
    target_link_libraries(bftcommunication_shared PUBLIC ${Boost_LIBRARIES})
    if(${BUILD_COMM_TCP_PLAIN})
        target_compile_definitions(bftcommunication PUBLIC USE_COMM_PLAIN_TCP)
        target_compile_definitions(bftcommunication_shared PUBLIC USE_COMM_PLAIN_TCP)
    elseif(${BUILD_COMM_TCP_TLS})
        find_package(OpenSSL REQUIRED)
        target_include_directories(bftcommunication PUBLIC ${OPENSSL_INCLUDE_DIR})
        target_link_libraries(bftcommunication PUBLIC ${OPENSSL_LIBRARIES} diagnostics)
        target_compile_definitions(bftcommunication PUBLIC USE_COMM_TLS_TCP)
        target_include_directories(bftcommunication_shared PUBLIC ${OPENSSL_INCLUDE_DIR})
        target_link_libraries(bftcommunication_shared PUBLIC ${OPENSSL_LIBRARIES} diagnostics)
        target_compile_definitions(bftcommunication_shared PUBLIC USE_COMM_TLS_TCP)
        if(LEAKCHECK OR THREADCHECK)
            # Workaround for the issue SSL socket gets stuck on shutdown:
            # Usually, Apollo tests kill skvbc_replica process with `SIGKILL` but to receive
            # reports from sanitizers, the replicas have to be stopped with `SIGTERM`. The
            # problem is that sometimes SSL socket shutdown gets stuck forever. While the
            # issue is not fully investigated skvbc_replicas for ASAN/TSAN tests are built
            # with the flag NO_SSL_SOCKET_SHUTDOWN on.
            target_compile_definitions(bftcommunication_shared PUBLIC NO_SSL_SOCKET_SHUTDOWN)
            target_compile_definitions(bftcommunication PUBLIC NO_SSL_SOCKET_SHUTDOWN)
        endif()
    endif()
endif()
install(DIRECTORY include/communication DESTINATION include)
install (TARGETS bftcommunication_shared DESTINATION lib${LIB_SUFFIX})
