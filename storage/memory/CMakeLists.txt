add_library(concordbft_storage_memory STATIC
    in_memory_db_client.cpp
)
target_include_directories(concordbft_storage_memory PUBLIC .)
target_link_libraries(concordbft_storage_memory PUBLIC storage)
