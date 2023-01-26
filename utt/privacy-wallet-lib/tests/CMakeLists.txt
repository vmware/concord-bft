find_package(GTest REQUIRED)

set(privacy_wallet_lib_storage_tests_src
    storage/FileBasedStorageTests.cpp
    utils/crypto_tests.cpp
)

foreach(appSrc ${privacy_wallet_lib_storage_tests_src})
    get_filename_component(appName ${appSrc} NAME_WE)
    set(appDir ${CMAKE_CURRENT_BINARY_DIR}/bin)

    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName} PRIVATE privacy_wallet_lib GTest::Main GTest::GTest)

    add_test(NAME ${appName} COMMAND ${appName})
    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endforeach()