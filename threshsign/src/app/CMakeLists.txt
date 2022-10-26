set(mainapp_source_files Main.cpp)

# Link against this when you want an AppMain() without RELIC support (i.e., initialized RELIC library)
# For example, generic non-RELIC tests and benchmarks will use this to avoid
# repeating the same boilerplate initialization code.
add_library(mainapp STATIC ${mainapp_source_files})
target_include_directories(mainapp PRIVATE ../../include ../../src)
