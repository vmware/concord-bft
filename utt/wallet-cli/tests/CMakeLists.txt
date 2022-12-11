find_package(GTest REQUIRED)
set(wallet_cli_tests_src
    storage/FileBasedStorageTests.cpp
)

foreach(appSrc ${wallet_cli_tests_src})
    get_filename_component(appName ${appSrc} NAME_WE)
    set(appDir ${CMAKE_CURRENT_BINARY_DIR}/bin)

    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName} PRIVATE utt-wallet-api-proto utt-wallet-lib GTest::Main GTest::GTest)

    add_test(NAME ${appName} COMMAND ${appName})
    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endforeach()