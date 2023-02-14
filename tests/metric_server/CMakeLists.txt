add_test(   NAME metric_server_tests 
            COMMAND env METRIC_SERVER_PATH=$<TARGET_FILE:metric_server> python3 -m unittest test_metric_server
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )
