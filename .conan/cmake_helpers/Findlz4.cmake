find_path(LZ4_INCLUDE_DIR lz4.h ${CONAN_INCLUDE_DIRS_LZ4})
find_library(LZ4_LIBRARY NAMES ${CONAN_LIBS_LZ4} PATHS ${CONAN_LIB_DIRS_LZ4})

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args(LZ4 DEFAULT_MSG LZ4_INCLUDE_DIR LZ4_LIBRARY)

if(LZ4_FOUND)
    set(LZ4_LIBRARIES ${LZ4_LIBRARY})
    set(LZ4_INCLUDE_DIRS ${LZ4_INCLUDE_DIR})
endif()
