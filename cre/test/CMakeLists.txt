find_package(GTest REQUIRED)

add_executable(cre_test cre_test.cpp)
add_test(cre_test cre_test)
target_include_directories(cre_test PRIVATE ../src)
target_link_libraries(cre_test PRIVATE
  GTest::Main
  GTest::GTest
  cre
    bftclient_new
)