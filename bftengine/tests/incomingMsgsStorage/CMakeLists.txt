find_package(GTest REQUIRED)

add_executable(incomingMsgsStorage_test incomingMsgsStorage_test.cpp )
add_test(incomingMsgsStorage_test incomingMsgsStorage_test)

target_link_libraries(incomingMsgsStorage_test PUBLIC
   GTest::Main
   corebft)
