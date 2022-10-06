#
# Targets
#

set(utt_example_sources
)

foreach(appSrc ${utt_example_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    set(appDir ../bin/examples)

    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName} PRIVATE utt)

    add_test(NAME ${appName} COMMAND ${appName})

    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endforeach()
