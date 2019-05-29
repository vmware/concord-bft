set(test_serialization_source_files
    TestSerialization.cpp)

add_executable(test_serialization
    ${test_serialization_source_files})

target_include_directories(test_serialization PUBLIC
        ${bftengine_SOURCE_DIR}/src/bftengine)

target_link_libraries(test_serialization PUBLIC corebft)
