set(simple_file_storage_source_files_lib
    ObjectsMetadataHandler.cpp
    FileStorage.cpp)

set(simple_file_storage_source_files
    ObjectsMetadataHandler.cpp
    FileStorage.cpp
    main.cpp)

add_executable(simple_file_storage ${simple_file_storage_source_files})

add_library(simple_file_storage_lib ${simple_file_storage_source_files_lib})

target_link_libraries(simple_file_storage PUBLIC corebft  )

target_link_libraries(simple_file_storage_lib PUBLIC corebft)
