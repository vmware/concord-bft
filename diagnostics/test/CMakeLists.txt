find_package(GTest REQUIRED)

add_executable(diagnostics_tests diagnostics_tests.cpp)
add_test(diagnostics_tests diagnostics_tests)
target_link_libraries(diagnostics_tests PRIVATE GTest::Main diagnostics)

