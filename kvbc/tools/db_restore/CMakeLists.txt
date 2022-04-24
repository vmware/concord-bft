if (USE_S3_OBJECT_STORE)
find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

add_executable(db_restore main.cpp db_restore.cpp)
target_link_libraries(db_restore PUBLIC libdb_integrity_check libdb_integrity_check)
endif(USE_S3_OBJECT_STORE)
