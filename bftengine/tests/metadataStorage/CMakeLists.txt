find_package(GTest REQUIRED)
add_executable(metadataStorage_test metadataStorage_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(metadataStorage_test metadataStorage_test)

target_link_libraries(metadataStorage_test PUBLIC
    GTest::Main
    GTest::GTest
    util
    kvbc
    corebft
)
