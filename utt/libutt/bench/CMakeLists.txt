set(utt_bench_sources
    #BenchConvertAndMultiexp.cpp
    BenchExp.cpp
    BenchFFT.cpp
    BenchMultiexp.cpp
    BenchMultipairing.cpp
    #BenchNtlConversion.cpp
    BenchPairing.cpp
    #BenchPolynomialOps.cpp
    BenchTxn.cpp
)

foreach(appSrc ${utt_bench_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    set(appDir ../bin/bench)

    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName} PRIVATE utt)

    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endforeach()
