set(newutt_test_sources
    TestDistributedRegistration.cpp
    TestMint.cpp
    TestBurn.cpp
    TestBudget.cpp
    TestTransaction.cpp
    TestTransactionWithRsaEncryption.cpp
)

foreach(appSrc ${newutt_test_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    set(appDir ../bin/test)

    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName} PRIVATE utt_api)

    add_test(NAME ${appName} COMMAND ${appName})
    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endforeach()