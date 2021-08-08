find_package(GTest REQUIRED)

add_executable(cre_test_api cre_test_api.cpp)
add_test(cre_test_api cre_test_api)
target_link_libraries(cre_test_api PRIVATE
  GTest::Main
  GTest::GTest
  cre
    bftclient_new
)

add_executable(poll_based_state_client_test poll_based_state_client_test.cpp)
add_test(poll_based_state_client_test poll_based_state_client_test)
target_link_libraries(poll_based_state_client_test PRIVATE
        GTest::Main
        GTest::GTest
        cre
        bftclient_new
        cmf_messages
        threshsign
        secretsmanager
        )