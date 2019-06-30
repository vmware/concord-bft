add_test(NAME skvbc_tests COMMAND python3 -m unittest
    test_skvbc
    test_skvbc_chaos
    test_skvbc_linearizability
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
