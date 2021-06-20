# Generate C++ code from a CMF file
#
# cmf_generate_cpp(<LIST_OF_GENERATED_HEADER_FILES> <LIST_OF_GENERATED_CPP_FILES> <CPP_NAMESPACE> <CMFs> ...)
#
# LIST_OF_GENERATED_HEADER_FILES - Will be populated with generated header files
# LIST_OF_GENERATED_CPP_FILES - Will be populated with generated cpp files
# CPP_NAMESPACE - C++ namespace to use for the generated code
# CMFs - List of CMF files
function(CMF_GENERATE_CPP CPP_HEADER CPP_IMPL CPP_NAMESPACE)
  foreach(FIL ${ARGN})
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${FIL}")
      message(FATAL_ERROR "CMF doesn't exist: ${CMAKE_CURRENT_SOURCE_DIR}/${FIL}")
    endif()
    set(IN_FILE_ABS "${CMAKE_CURRENT_SOURCE_DIR}/${FIL}")
    add_custom_command(
      OUTPUT "${FIL}.hpp" "${FIL}.cpp"
      COMMAND ${CMF_COMPILER}
      ARGS --input ${IN_FILE_ABS}
           --output ${FIL}
           --language cpp
           --namespace ${CPP_NAMESPACE}
      DEPENDS ${FIL} ${CMF_COMPILER}
      COMMENT "CMFC: Generate C++ code for ${FIL}"
      VERBATIM
    )
    list(APPEND CPP_HEADER "${FIL}.hpp")
    list(APPEND CPP_IMPL "${FIL}.cpp")
  endforeach()
  set_source_files_properties(${CPP_HEADER} PROPERTIES GENERATED TRUE)
  set(${CPP_HEADER} ${${CPP_HEADER}} PARENT_SCOPE)
  set_source_files_properties(${CPP_IMPL} PROPERTIES GENERATED TRUE)
  set(${CPP_IMPL} ${${CPP_IMPL}} PARENT_SCOPE)
endfunction()

function(CMF_GENERATE_PYTHON OUTPUT_NAME)
  foreach(FIL ${ARGN})
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${FIL}")
      message(FATAL_ERROR "CMF doesn't exist: ${CMAKE_CURRENT_SOURCE_DIR}/${FIL}")
    endif()
    set(IN_FILE_ABS "${CMAKE_CURRENT_SOURCE_DIR}/${FIL}")
    execute_process(
            COMMAND ${CMF_COMPILER}
            --input ${IN_FILE_ABS}
            --output "${CMAKE_CURRENT_BINARY_DIR}/${OUTPUT_NAME}"
            --language python
    )
  endforeach()
endfunction()

# Find CMFC
find_program(CMF_COMPILER cmfc.py
  HINTS "../messages/compiler/"
)
if(NOT CMF_COMPILER)
  message(FATAL_ERROR "Couldn't find CMF compiler")
endif()
message(STATUS "cmfc.py found at ${CMF_COMPILER}")
