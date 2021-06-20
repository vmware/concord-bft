find_package(GTest REQUIRED)

add_executable(ClientsManager_test ClientsManager_test.cpp )
add_test(ClientsManager_test ClientsManager_test)

target_link_libraries(ClientsManager_test PUBLIC
    GTest::Main
    corebft)
