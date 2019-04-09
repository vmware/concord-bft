add_test(NAME pyclient_tests COMMAND python3 -m unittest
    test_client
    test_msgs
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
