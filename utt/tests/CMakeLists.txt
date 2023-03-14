find_package(GTest REQUIRED)

set(utt-tests-utils-src
    testUtils/testUtils.cpp
)

add_library(utt_test_utils STATIC ${utt-tests-utils-src})

target_include_directories(utt_test_utils PUBLIC include .)

target_link_libraries(utt_test_utils PUBLIC utt_api logging GTest::Main GTest::GTest)


set(newutt_test_sources
    TestUttNewApi.cpp
)

foreach(appSrc ${newutt_test_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    set(appDir ../bin/test)

    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName} PRIVATE  utt_test_utils)

    add_test(NAME ${appName} COMMAND ${appName})
    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endforeach()