find_path(CRYPTOPP_INCLUDE_DIR keccak.h
        HINTS /usr/local/include/cryptopp
        ${CMAKE_CURRENT_SOURCE_DIR}/../cryptopp)

if(CRYPTOPP_INCLUDE_DIR EQUAL CRYPTOPP_INCLUDE_DIR-NOTFOUND)
    message(SEND_ERROR "cryptopp not found")
else()
    message(STATUS "cryptopp found at " ${CRYPTOPP_INCLUDE_DIR})
    set(CRYPTOPP_INCLUDE_DIRS ${CRYPTOPP_INCLUDE_DIR})

    find_path(CRYPTOPP_LIB_DIR libcryptopp.a
            HINTS ${CRYPTOPP_INCLUDE_DIR}/../../lib/
            ${CRYPTOPP_INCLDUE_DIR}/build/)
    if(NOT CRYPTOPP_LIB_DIR)
        message(SEND_ERROR "cryptopp lib not found")
    else(NOT CRYPTOPP_LIB_DIR)
        link_directories(${CRYPTOPP_LIB_DIR})
        set(CRYPTOPP_LIBRARIES cryptopp)
        set(CRYPTOPP_FOUND 1)
    endif(NOT CRYPTOPP_LIB_DIR)
endif(CRYPTOPP_INCLUDE_DIR EQUAL CRYPTOPP_INCLUDE_DIR-NOTFOUND)
