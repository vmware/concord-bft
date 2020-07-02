project (bftclient LANGUAGES CXX)

add_library(bftclient2 STATIC
  src/bft_client.cpp
  src/msg_receiver.cpp
  src/matcher.cpp
  src/quorums.cpp
)

target_include_directories(bftclient2 PUBLIC include)
target_include_directories(bftclient2 PRIVATE src)
target_link_libraries(bftclient2 PUBLIC bftcommunication bftheaders)

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
