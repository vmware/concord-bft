set(test-client-api-src
    TestUTTClientApi.cpp
)

foreach(testSrc ${test-client-api-src})
    get_filename_component(testName ${testSrc} NAME_WE)
    set(testDir ../bin/test)

    add_executable(${testName} ${testSrc})
    target_link_libraries(${testName} PRIVATE utt_test_utils utt_client_api)

    add_test(NAME ${testName} COMMAND ${testName})
    set_target_properties(${testName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${testDir})
endforeach()