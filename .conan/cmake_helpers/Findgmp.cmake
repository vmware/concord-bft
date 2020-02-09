find_path(GMP_INCLUDE_DIR gmp.h ${CONAN_INCLUDE_DIRS_GMP})
find_library(GMP_LIBRARY NAMES ${CONAN_LIBS_GMP} PATHS ${CONAN_LIB_DIRS_GMP})

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args(GMP DEFAULT_MSG GMP_INCLUDE_DIR GMP_LIBRARY)

if(GMP_FOUND)
    set(GMP_LIBRARIES ${GMP_LIBRARY})
    set(GMP_INCLUDE_DIRS ${GMP_INCLUDE_DIR})
endif()
