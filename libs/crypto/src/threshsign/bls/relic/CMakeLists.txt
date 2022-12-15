include(ExternalProject)

# Relic is a cryptographic "meta-toolkit" which can be used with different
# concrete implementations. The user has to make sure that the latter is
# available at compile-/runtime.

string(APPEND CXX_FLAGS " -Wextra")
## TODO: Figure out right way to deal with -fstrict-overflow / -Wstrict-overflow related errors
string(APPEND CXX_FLAGS " -fno-strict-overflow")
string(APPEND CXX_FLAGS_DEBUG " -fstack-protector-all")
string(APPEND CXX_FLAGS_DEBUG " -D_FORTIFY_SOURCE=2")
#
## When you do 'a > b in 'C/C++, if a is unsigned and b is signed and equal to -1, C/C++
## actually casts b to unsigned (probably because casting unsigned to signed would require a bigger data type)
## Thus, 1 > -1 will evaluate to false because during the cast -1 will be set to to 2^32 - 1
##
## WARNING: For the love of god, do not remove this flag or you will regret it. Instead,
## just use signed types everywhere and cast your unsigned to signed when mixing unsigned
## variables with signed ones. See: http://soundsoftware.ac.uk/c-pitfall-unsigned
string(APPEND CXX_FLAGS " -Wconversion -Wsign-conversion")


# The GNU Multiple Precision Arithmetic Library
# Needed by relic
ExternalProject_Add(gmp
                    PREFIX gmp
                    URL "https://gmplib.org/download/gmp/gmp-6.1.2.tar.lz"
                    LOG_DOWNLOAD 1
                    LOG_BUILD 1
                    BUILD_IN_SOURCE 1
                    CONFIGURE_COMMAND ./configure --with-pic --enable-cxx --disable-fat
                    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -j${NPROC}
                    INSTALL_COMMAND ""
)

ExternalProject_Get_Property(gmp BINARY_DIR)
set(GMP_STATIC_LIBRARY ${BINARY_DIR}/.libs/libgmp.a)
message(STATUS "GMP_STATIC_LIBRARY ${GMP_STATIC_LIBRARY}")

#
# https://www.scivision.dev/cmake-externalproject-list-arguments/
# to pass strings with semicolons via CMAKE_CACHE_ARGS
#
set(FP_METHD_FLAGS "INTEG;INTEG;INTEG;MONTY;LOWER;SLIDE" )
set(FPX_METHD_FLAGS "INTEG;INTEG;LAZYR")
set(PP_METHD_FLAGS "LAZYR;OATEP")

set(COMP_FLAGS "-O3 -funroll-loops -fomit-frame-pointer -finline-small-functions -march=x86-64 -mtune=generic -fPIC")
set(INSTALL_DIR "install")
ExternalProject_Add(relic
                    PREFIX relic
                    GIT_REPOSITORY "https://github.com/relic-toolkit/relic"
                    GIT_TAG "0998bfcb6b00aec85cf8d755d2a70d19ea3051fd"
                    GIT_PROGRESS TRUE
                    LOG_DOWNLOAD 1
                    LOG_BUILD 1
                    CMAKE_ARGS  -DCOMP=${COMP_FLAGS}
                                -DALLOC=AUTO 
                                -DWSIZE=64 
                                -DWORD=64 
                                -DRAND=UDEV 
                                -DSHLIB=ON 
                                -DSTLIB=ON 
                                -DSTBIN=OFF 
                                -DTIMER=HREAL   
                                -DCHECK=on 
                                -DVERBS=on 
                                -DARITH=x64-asm-254 
                                -DFP_PRIME=254  
                                -DFP_PMERS=off 
                                -DFP_QNRES=on 
                                -DTESTS=0
                                -DBENCH=0
                                -DCMAKE_INSTALL_PREFIX:PATH=${INSTALL_DIR}
                    CMAKE_CACHE_ARGS    
                                -DFP_METHD:STRING=${FP_METHD_FLAGS}
                                -DFPX_METHD:STRING=${FPX_METHD_FLAGS}
                                -DPP_METHD:STRING=${PP_METHD_FLAGS}
                    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -j${NPROC}
                    INSTALL_COMMAND ""
                    DEPENDS gmp
)

ExternalProject_Get_Property(relic BINARY_DIR)
ExternalProject_Get_Property(relic SOURCE_DIR)
set(RELIC_STATIC_LIBRARY "${BINARY_DIR}/lib/librelic_s.a")
set(RELIC_INCLUDE_DIRS "${SOURCE_DIR}/include;${BINARY_DIR}/include")
message(STATUS "RELIC_STATIC_LIBRARY ${RELIC_STATIC_LIBRARY}")
message(STATUS "RELIC_INCLUDE_DIRS ${RELIC_INCLUDE_DIRS}")

set(bls_relic_source_files
        BlsAccumulatorBase.cpp
        BlsAlmostMultisigAccumulator.cpp
        BlsAlmostMultisigCoefficients.cpp
        BlsBatchVerifier.cpp
        BlsMultisigAccumulator.cpp
        BlsMultisigKeygen.cpp
        BlsMultisigVerifier.cpp
        BlsNumTypes.cpp
        BlsPolynomial.cpp
        BlsPublicParameters.cpp
        BlsThresholdAccumulator.cpp
        BlsThresholdAccumulatorBaseExports.cpp
        BlsThresholdFactory.cpp
        BlsThresholdKeygen.cpp
        BlsThresholdSigner.cpp
        BlsThresholdVerifier.cpp
        FastMultExp.cpp
        LagrangeInterpolation.cpp
        Library.cpp
        )

add_library(bls_relic STATIC ${bls_relic_source_files})
add_dependencies(bls_relic relic)
target_include_directories(bls_relic PUBLIC . .. ${RELIC_INCLUDE_DIRS})
target_link_libraries(bls_relic PUBLIC ${RELIC_STATIC_LIBRARY} ${GMP_STATIC_LIBRARY})
target_link_libraries(threshsign PUBLIC bls_relic)

if(BUILD_TESTING)
    add_subdirectory(app)
    add_subdirectory(test)
    add_subdirectory(bench)
endif()

