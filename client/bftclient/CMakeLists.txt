project (bftclient LANGUAGES CXX)

add_library(bftclient_new STATIC
  src/bft_client.cpp
  src/msg_receiver.cpp
  src/matcher.cpp
  src/quorums.cpp
)

target_include_directories(bftclient_new PUBLIC include)
target_include_directories(bftclient_new PUBLIC src)
target_include_directories(bftclient_new PUBLIC ${bftengine_SOURCE_DIR}/src)
target_include_directories(bftclient_new PUBLIC ${bftengine_SOURCE_DIR}/include)
target_include_directories(bftclient_new PUBLIC ${libbftcommunication_SOURCE_DIR}/include)

target_link_libraries(bftclient_new PRIVATE 
    bftcommunication 
    bftheaders
    diagnostics
    secretsmanager
    util
    concord-crypto
)

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
