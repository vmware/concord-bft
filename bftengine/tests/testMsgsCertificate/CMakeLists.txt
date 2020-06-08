find_package(GTest REQUIRED)

add_executable(msgsCertificate_test msgsCertificate_test.cpp )
add_test(msgsCertificate_test msgsCertificate_test)

target_include_directories(msgsCertificate_test
        PRIVATE
        ${bftengine_SOURCE_DIR}/src/preprocessor

        )

target_link_libraries(msgsCertificate_test PUBLIC
        GTest::Main
        corebft
        )