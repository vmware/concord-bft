find_package(GTest REQUIRED)

add_executable(secrets_manager_test secrets_manager_test.cpp)
add_test(secrets_manager_test secrets_manager_test)
target_link_libraries(secrets_manager_test PUBLIC GTest::Main secretsmanager)
target_include_directories(secrets_manager_test PRIVATE ${PROJECT_SOURCE_DIR} ../src)