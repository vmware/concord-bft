add_test(NAME bft_sys_tests COMMAND python3 -m unittest
    test_skvbc
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_linearizability COMMAND python3 -m unittest
    test_skvbc_linearizability
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
