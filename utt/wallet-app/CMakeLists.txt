add_subdirectory("proto")

set(src
    main.cpp
)

add_executable(utt-wallet ${src})

target_link_libraries(utt-wallet PUBLIC
  utt-wallet-api-proto
)