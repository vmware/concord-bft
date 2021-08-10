project (bftclient LANGUAGES CXX)

add_library(bftclient_new STATIC
  src/bft_client.cpp
  src/msg_receiver.cpp
  src/matcher.cpp
  src/quorums.cpp
  ${bftengine_SOURCE_DIR}/src/bftengine/Crypto.cpp
)

target_include_directories(bftclient_new PUBLIC include)
target_include_directories(bftclient_new PUBLIC src)
target_include_directories(bftclient_new PUBLIC ${bftengine_SOURCE_DIR}/src)
target_include_directories(bftclient_new PUBLIC ${threshsign_SOURCE_DIR}/include)
target_include_directories(bftclient_new PUBLIC ${libdiagnostics_SOURCE_DIR}/include)
target_include_directories(bftclient_new PUBLIC ${secretsmanager_SOURCE_DIR}/include)
target_link_libraries(bftclient_new PUBLIC 
    bftcommunication 
    bftheaders
    diagnostics
    secretsmanager
)

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
