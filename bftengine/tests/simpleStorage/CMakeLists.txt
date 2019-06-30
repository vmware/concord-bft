set(simple_file_storage_source_files
    ObjectsMetadataHandler.cpp
    FileStorage.cpp
    main.cpp
)

add_executable(simple_file_storage
    ${simple_file_storage_source_files}
)

target_link_libraries(simple_file_storage PUBLIC corebft)
