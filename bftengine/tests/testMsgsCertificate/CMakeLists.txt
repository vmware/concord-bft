find_package(GTest REQUIRED)

add_executable(msgsCertificate_test msgsCertificate_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(msgsCertificate_test msgsCertificate_test)

target_link_libraries(msgsCertificate_test PUBLIC
        GTest::Main
        corebft
        )