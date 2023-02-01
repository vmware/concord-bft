find_package(GTest REQUIRED)
set(privacy_wallet_service_tests_src
    grpc-service/grpc-server-test.cpp
)

foreach(appSrc ${privacy_wallet_service_tests_src})
    get_filename_component(appName ${appSrc} NAME_WE)
    set(appDir ${CMAKE_CURRENT_BINARY_DIR}/bin)

    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName} PRIVATE utt_test_utils privacy-wallet-service-lib GTest::Main GTest::GTest)

    add_test(NAME ${appName} COMMAND ${appName})
    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endforeach()


    #target_include_directories(${appName} PUBLIC
    #    ../include ../../include ../../libutt/include
    #    ${LIBFF_INCLUDE_DIR}
    #    ${NTL_INCLUDE_DIR}
    #    ${ZM_INCLUDE_DIR}
    #)
