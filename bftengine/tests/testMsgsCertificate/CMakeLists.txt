find_package(GTest REQUIRED)

add_executable(msgsCertificate_test msgsCertificate_test.cpp ${bftengine_SOURCE_DIR}/tests/messages/helper.cpp)
add_test(msgsCertificate_test msgsCertificate_test)

target_include_directories(msgsCertificate_test
        PRIVATE
        ${bftengine_SOURCE_DIR}/src/preprocessor
        ${bftengine_SOURCE_DIR}/tests/messages/
        )

target_link_libraries(msgsCertificate_test PUBLIC
        GTest::Main
        corebft
        ${bftengine_SOURCE_DIR}/src/bftengine
        )
