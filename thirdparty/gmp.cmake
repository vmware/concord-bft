#The GNU Multiple Precision Arithmetic Library

ExternalProject_Add(gmp
                    PREFIX gmp
                    URL "https://gmplib.org/download/gmp/gmp-6.1.2.tar.lz"
                    LOG_DOWNLOAD 1
                    LOG_BUILD 1
                    BUILD_IN_SOURCE 1
                    CONFIGURE_COMMAND ./configure --with-pic --enable-cxx --disable-fat --build x86_64-linux-gnu
                    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -j${NPROC}
                    INSTALL_COMMAND ""
                    COMMAND make check
)

ExternalProject_Get_Property(gmp BINARY_DIR)
set(GMP_STATIC_LIBRARY ${BINARY_DIR}/.libs/libgmp.a PARENT_SCOPE)
message(STATUS "GMP_STATIC_LIBRARY ${BINARY_DIR}/.libs/libgmp.a")
