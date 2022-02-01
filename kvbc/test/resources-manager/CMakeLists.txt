find_package(GTest REQUIRED)

add_executable(resource_manager_test ResourceManagerTest.cpp)
target_link_libraries(resource_manager_test GTest::Main kvbc)

add_test(resource_manager_test resource_manager_test)

target_include_directories(resource_manager_test PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../../include/resources-manager)
