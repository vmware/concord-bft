find_path(RAPIDCHECK_INCLUDE_DIR rapidcheck/rapidcheck.h ${CONAN_INCLUDE_DIRS_RAPIDCHECK})
find_library(RAPIDCHECK_LIBRARY NAMES ${CONAN_LIBS_RAPIDCHECK} PATHS ${CONAN_LIB_DIRS_RAPIDCHECK})

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args(RAPIDCHECK DEFAULT_MSG RAPIDCHECK_INCLUDE_DIR RAPIDCHECK_LIBRARY)

if(RAPIDCHECK_FOUND)
    set(RAPIDCHECK_LIBRARIES ${RAPIDCHECK_LIBRARY})
    # Support includes in the form of:
    #  * #include "rapidcheck/rapidcheck.h"
    #  * #include "rapidcheck/extras/gtest.h"
    #  * #include "rapidcheck/extras/gmock.h"
    # In order to do so, set both the include dir returned by Conan and its /rapidcheck subdirectory.
    # The latter is needed, because rapidcheck internally includes as #include "rapidcheck/xyz.h" .
    set(RAPIDCHECK_INCLUDE_DIRS ${RAPIDCHECK_INCLUDE_DIR} ${RAPIDCHECK_INCLUDE_DIR}/rapidcheck)
endif()
