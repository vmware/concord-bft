#
# Targets
#

add_library(utt 
    Address.cpp
    BudgetProof.cpp
    Coin.cpp
    Comm.cpp
    IBE.cpp
    Kzg.cpp
    Nullifier.cpp
    Params.cpp
    PolyOps.cpp
    PolyCrypto.cpp
    RandSig.cpp
    RandSigDKG.cpp
    RangeProof.cpp
    RegAuth.cpp
    SplitProof.cpp
    Tx.cpp
    TxIn.cpp
    TxOut.cpp
    Utils.cpp
    Wallet.cpp
    ZKPoK.cpp
    Simulation.cpp
    Client.cpp
    Replica.cpp
    BurnOp.cpp
    MintOp.cpp
    DataUtils.cpp
)

find_package(OpenSSL REQUIRED)
add_dependencies(utt libfqfft ntl libff)

target_include_directories(utt PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>"
    "$<INSTALL_INTERFACE:${INSTALL_CMAKE_DIR}/include>"
)

target_include_directories(utt SYSTEM PUBLIC
    ${LIBFF_INCLUDE_DIR}
    ${NTL_INCLUDE_DIR}
    ${ZM_INCLUDE_DIR}
)

if(APPLE)
    target_link_libraries(utt PUBLIC
        ${NTL_LIBRARY} ${LIBFF_LIBRARY} pthread ${ZM_LIBRARY} ${GMP_LIBRARY} ${GMPXX_LIBRARY} xutils xassert OpenSSL::Crypto z
    )
else()
    target_link_libraries(utt PUBLIC
    ${NTL_LIBRARY} ${LIBFF_LIBRARY} pthread ${ZM_LIBRARY} ${GMPXX_LIBRARY} ${GMP_LIBRARY} xutils xassert stdc++fs OpenSSL::Crypto z
    )
endif()

include(FindOpenMP)
if(OPENMP_FOUND)
    # Out of nowhere, this started failing on OS X, so I disabled it since we don't use it anyway
    if(NOT APPLE)
        target_link_libraries(utt PUBLIC gomp)
    endif()
endif()

#target_link_libraries(utt PUBLIC Threads::Threads)

#
# Installation
# TODO: Add <Package>Config[Version].cmake files so this package can be easily imported?
# (See https://cmake.org/cmake/help/git-master/manual/cmake-packages.7.html#creating-packages)
#

# This creates the <Package>Config.cmake file and installs it
# install(TARGETS utt EXPORT uttConfig 
#    ARCHIVE DESTINATION lib)
# install(EXPORT uttConfig DESTINATION lib/cmake/utt)

# This installs the static or (/and?) dynamic library
install(TARGETS utt 
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)

# This installs the headers
# WARNING: Don't add / at the end. No slash means utt/ directory is created in the destination path
install(DIRECTORY include/utt DESTINATION include)
