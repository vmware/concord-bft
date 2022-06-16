find_package(GTest REQUIRED)

add_executable(client-pool-timer-test client_pool_timer_test.cpp)
target_link_libraries(client-pool-timer-test PUBLIC
  GTest::Main
  concord_client_pool
)

# Disabled due to instability
# add_test(client-pool-timer-test client-pool-timer-test)
