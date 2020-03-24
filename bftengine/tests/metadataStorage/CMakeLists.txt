find_package(GTest REQUIRED)
add_executable(metadataStorage_test metadataStorage_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(metadataStorage_test metadataStorage_test)
# TODO [TK] TEMPORARY - WILL BE REMOVED AFTER DBADAPTER REFACTORING !!!
target_include_directories(metadataStorage_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../../kvbc/include)

target_link_libraries(metadataStorage_test PUBLIC
    GTest::Main
    GTest::GTest
    util
    kvbc
    corebft
)
