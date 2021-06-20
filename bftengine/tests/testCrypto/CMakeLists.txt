find_package(GTest REQUIRED)

add_executable(crypto_test crypto_test.cpp )
add_test(crypto_test crypto_test)

target_link_libraries(crypto_test PUBLIC
    GTest::Main
    corebft)
