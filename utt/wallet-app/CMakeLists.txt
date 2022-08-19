add_subdirectory("proto")

set(src
    main.cpp
)

add_executable(wallet-app ${src})

target_link_libraries(wallet-app PUBLIC
  utt-wallet-api-proto
)