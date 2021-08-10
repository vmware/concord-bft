#
# https://www.scivision.dev/cmake-externalproject-list-arguments/
# to pass strings with semicolons via CMAKE_CACHE_ARGS
#
set(FP_METHD_FLAGS "INTEG;INTEG;INTEG;MONTY;LOWER;SLIDE" )
set(FPX_METHD_FLAGS "INTEG;INTEG;LAZYR")
set(PP_METHD_FLAGS "LAZYR;OATEP")

set(COMP_FLAGS "-O3 -funroll-loops -fomit-frame-pointer -finline-small-functions -march=x86-64 -mtune=generic -fPIC")

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
                    CMAKE_CACHE_ARGS    
                                -DFP_METHD:STRING=${FP_METHD_FLAGS}
                                -DFPX_METHD:STRING=${FPX_METHD_FLAGS}
                                -DPP_METHD:STRING=${PP_METHD_FLAGS}
                    INSTALL_COMMAND ""
                    DEPENDS gmp
)

ExternalProject_Get_Property(relic BINARY_DIR)
set(RELIC_STATIC_LIBRARY ${BINARY_DIR}/lib/librelic_s.a PARENT_SCOPE)
message(STATUS "RELIC_STATIC_LIBRARY ${BINARY_DIR}/lib/librelic_s.a")
