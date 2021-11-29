find_package(GTest REQUIRED)

add_executable(RequestThreadPool_test testRequestThreadPool.cpp)

add_test(RequestThreadPool_test RequestThreadPool_test)

target_link_libraries(RequestThreadPool_test PUBLIC
        GTest::Main
        corebft)

