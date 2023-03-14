# HdrHistogram_c Library

ExternalProject_Add(rocksdb
                    PREFIX rocksdb
                    GIT_REPOSITORY "https://github.com/facebook/rocksdb.git"
                    GIT_TAG "v6.29.3"
                    GIT_PROGRESS TRUE
                    LOG_DOWNLOAD 1
                    LOG_BUILD 1
                    CMAKE_ARGS -DWITH_GFLAGS=OFF -DFAIL_ON_WARNINGS=OFF -DCMAKE_INSTALL_PREFIX:FILEPATH=${THIRDPARTY_INSTALL_DIR}
                    )

set(ROCKSDB_INCLUDE_DIR ${THIRDPARTY_INSTALL_DIR}/include)
set(ROCKSDB_LIBRARY ${THIRDPARTY_INSTALL_DIR}/lib/librocksdb${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ROCKSDB_INCLUDE_DIR ${ROCKSDB_INCLUDE_DIR} PARENT_SCOPE)
set(ROCKSDB_LIBRARY ${ROCKSDB_LIBRARY} PARENT_SCOPE)

message(STATUS "ROCKSDB_INCLUDE_DIR ${ROCKSDB_INCLUDE_DIR}")
message(STATUS "ROCKSDB_LIBRARY ${ROCKSDB_LIBRARY}")
