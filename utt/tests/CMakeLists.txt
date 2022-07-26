set(newutt_test_sources
    TestDistributedRegistration.cpp
    TestMint.cpp
    TestBurn.cpp
    TestBudget.cpp
    TestTransaction.cpp
)

foreach(appSrc ${newutt_test_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    set(appDir ../bin/test)

    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName} PRIVATE newutt)

    add_test(NAME ${appName} COMMAND ${appName})
    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endforeach()

target_compile_definitions(TestParallelPairing PRIVATE -DUSE_MULTITHREADING)
