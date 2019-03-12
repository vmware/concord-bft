set(simple_file_storage_source_files
    ObjectsMetadataHandler.cpp
    FileStorage.cpp
    main.cpp
)

add_executable(simple_file_storage
    ${simple_file_storage_source_files}
)

if(USE_LOG4CPP)
    target_compile_definitions(simple_file_storage PUBLIC USE_LOG4CPP=1)
endif()

target_link_libraries(simple_file_storage PUBLIC corebft)
