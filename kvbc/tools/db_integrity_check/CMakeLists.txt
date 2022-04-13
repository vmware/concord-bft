if (USE_S3_OBJECT_STORE)

project(db_integrity_check LANGUAGES CXX )
find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

add_library(libdb_integrity_check integrity_checker.cpp ../../../tools/KeyfileIOUtils.cpp)
target_include_directories(libdb_integrity_check PUBLIC ../../../tools)
target_link_libraries(libdb_integrity_check PUBLIC kvbc threshsign util Boost::program_options)

add_executable(s3_integrity_check main.cpp) 
set_property(TARGET s3_integrity_check PROPERTY CXX_STANDARD 17)
target_link_libraries(s3_integrity_check PRIVATE libdb_integrity_check)

endif(USE_S3_OBJECT_STORE)
