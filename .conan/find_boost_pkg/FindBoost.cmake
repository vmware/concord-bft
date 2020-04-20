# Copied from https://github.com/bincrafters/community/issues/26

function(add_cloned_imported_target dst src)
    add_library(${dst} INTERFACE IMPORTED)
    foreach(name INTERFACE_LINK_LIBRARIES INTERFACE_INCLUDE_DIRECTORIES INTERFACE_COMPILE_DEFINITIONS INTERFACE_COMPILE_OPTIONS)
        get_property(value TARGET ${src} PROPERTY ${name} )
        set_property(TARGET ${dst} PROPERTY ${name} ${value})
    endforeach()
endfunction()

function(first_character_to_upper dst src)
    string(SUBSTRING ${src} 0 1 first)
    string(SUBSTRING ${src} 1 -1 remaining)
    string(TOUPPER ${first} FIRST)
    set(${dst} ${FIRST}${remaining} PARENT_SCOPE)
endfunction()

foreach(component ${Boost_FIND_COMPONENTS})
    if(NOT TARGET Boost::${component})
        first_character_to_upper(Compontent ${component})
        add_cloned_imported_target(Boost::${component} CONAN_PKG::Boost.${Compontent})
    endif()
endforeach()
set(Boost_FOUND TRUE)
