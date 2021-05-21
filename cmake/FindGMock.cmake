find_path(GMOCK_INCLUDE_DIR gmock/gmock.h
        HINTS ${CMAKE_CURRENT_SOURCE_DIR}/../googletest/googlemock/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../../googletest/googlemock/include)

if(DEFINED GMOCK_INCLUDE_DIR_NOTFOUND)
  message(SEND_ERROR "GMOCK not found")
else(DEFINED GMOCK_INCLUDE_DIR_FOUND)
  message(STATUS "GMOCK found at " ${GMOCK_INCLUDE_DIR})
  set(GMOCK_FOUND 1)

  set(GMOCK_DIR "${GTEST_INCLUDE_DIR}/../../_build/googlemock")

  find_library(GMOCK_LIBRARY
          NAMES gmock
          libgmock
          libgmock.a
          PATHS "${GMOCK_DIR}")

endif(DEFINED GMOCK_INCLUDE_DIR_NOTFOUND)
