set(xutils_test_sources
  AutoBufTest.cpp
  UtilsTest.cpp
)

foreach(appSrc ${xutils_test_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName} PRIVATE xutils xassert)
    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ../bin/test)

    add_test(NAME ${appName} COMMAND ${appName})
endforeach()
