find_package(GTest REQUIRED)

add_executable(seqNumForClientRequest_test seqNumForClientRequest_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(seqNumForClientRequest_test seqNumForClientRequest_test)


target_link_libraries(seqNumForClientRequest_test PUBLIC
        GTest::Main
        corebft
        )