set(utt_test_sources
    TestCoin.cpp
    TestIBE.cpp
    #TestKatePublicParams.cpp
    TestPolyOps.cpp
    TestLibff.cpp
    TestMultiPairing.cpp
    TestParallelPairing.cpp
    TestParams.cpp
    TestRandSig.cpp
    TestRandSigDKG.cpp
    TestNullifier.cpp
    #TestRootsOfUnity.cpp
    TestTxn.cpp
    #TestTxnFlat.cpp
    TestTxnSimulateAll.cpp
    #TestTxnSimulateUTTDemo.cpp
    TestTxnSimulateUTTDemo_v2.cpp
    #TestXGCD.cpp
)

foreach(appSrc ${utt_test_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    set(appDir ../bin/test)

    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName} PRIVATE utt)

    add_test(NAME ${appName} COMMAND ${appName})
    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endforeach()

target_compile_definitions(TestParallelPairing PRIVATE -DUSE_MULTITHREADING)
