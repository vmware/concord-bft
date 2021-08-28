# HdrHistogram_c Library

ExternalProject_Add(hdrhistogram
                    PREFIX hdrhistogram
                    GIT_REPOSITORY "https://github.com/HdrHistogram/HdrHistogram_c"
                    GIT_TAG "0.9.12"
                    GIT_PROGRESS TRUE
                    CMAKE_ARGS cmake -DCMAKE_INSTALL_PREFIX=${THIRDPARTY_INSTALL_DIR}
                    LOG_DOWNLOAD 1
                    LOG_BUILD 1
                    )

ExternalProject_Get_Property(hdrhistogram BINARY_DIR)

set(HDR_HISTOGRAM_INCLUDE_DIR ${THIRDPARTY_INSTALL_DIR}/include)
set(HDR_HISTOGRAM_LIBRARY ${THIRDPARTY_INSTALL_DIR}/lib/libhdr_histogram${CMAKE_SHARED_LIBRARY_SUFFIX})
set(HDR_HISTOGRAM_INCLUDE_DIR ${HDR_HISTOGRAM_INCLUDE_DIR} PARENT_SCOPE)
set(HDR_HISTOGRAM_LIBRARY ${HDR_HISTOGRAM_LIBRARY} PARENT_SCOPE)

message(STATUS "HDR_HISTOGRAM_INCLUDE_DIR ${HDR_HISTOGRAM_INCLUDE_DIR}")
message(STATUS "HDR_HISTOGRAM_LIBRARY ${HDR_HISTOGRAM_LIBRARY}")    
