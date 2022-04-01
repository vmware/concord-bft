set(utt_app_sources
    KeygenThreshPs16.cpp
    ParamsGenTrapdoors.cpp
    ParamsGenPowers.cpp
    ParamsValidate.cpp
)

foreach(appSrc ${utt_app_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    set(appDir ../bin/app)

    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName} PUBLIC utt)

    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endforeach()

#install(TARGETS PlayApp DESTINATION bin)
