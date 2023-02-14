add_test(NAME pyclient_tests COMMAND python3 -m unittest
        test_msgs
        test_metrics_client
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
