find_package(GTest REQUIRED)

add_executable(seqNumForClientRequest_test seqNumForClientRequest_test.cpp )
add_test(seqNumForClientRequest_test seqNumForClientRequest_test)

target_include_directories(seqNumForClientRequest_test
        PRIVATE
        ${bftengine_SOURCE_DIR}/src/preprocessor

        )

target_link_libraries(seqNumForClientRequest_test PUBLIC
        GTest::Main
        corebft
        )