find_package(GTest REQUIRED)

add_executable(KeyStore_test KeyStore_test.cpp )
add_test(KeyStore_test KeyStore_test)

target_link_libraries(KeyStore_test PUBLIC
    GTest::Main
    corebft)
