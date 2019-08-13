
add_library(storage STATIC src/db_metadata_storage.cpp)

target_include_directories(storage PUBLIC include)
target_link_libraries(storage corebft)

if (BUILD_ROCKSDB_STORAGE)
  add_subdirectory(rocksdb)
  if (BUILD_TESTING)
    add_subdirectory(test)
  endif()
else()
  add_subdirectory(memory)
endif()

add_subdirectory(blockchain)
