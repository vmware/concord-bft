get_cmake_property(_variableNames VARIABLES)
list (SORT _variableNames)
set(ALL_ENV_VARS "")
foreach (_variableName ${_variableNames})
    set(ALL_ENV_VARS "${ALL_ENV_VARS} ${_variableName}='${${_variableName}}'")
endforeach()

add_test(NAME skvbc_basic_tests COMMAND sh -c
        "env ${ALL_ENV_VARS} python3 -m unittest test_skvbc 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_linearizability_tests COMMAND sudo sh -c
        "env ${ALL_ENV_VARS} python3 -m unittest test_skvbc_history_tracker test_skvbc_linearizability 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_fast_path_tests COMMAND sh -c
        "env ${ALL_ENV_VARS} python3 -m unittest test_skvbc_fast_path 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_slow_path_tests COMMAND sh -c
        "env ${ALL_ENV_VARS} python3 -m unittest test_skvbc_slow_path 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_view_change_tests COMMAND sudo sh -c
        "env ${ALL_ENV_VARS} python3 -m unittest test_skvbc_view_change 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_auto_view_change_tests COMMAND sh -c
        "env ${ALL_ENV_VARS} python3 -m unittest test_skvbc_auto_view_change 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

if (BUILD_ROCKSDB_STORAGE)
    add_test(NAME skvbc_persistence_tests COMMAND sh -c
            "python3 -m unittest test_skvbc_persistence 2>&1 > /dev/null"
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
            
add_test(NAME skvbc_ro_replica_tests COMMAND sh -c
        "python3 -m unittest test_skvbc_ro_replica 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()   
