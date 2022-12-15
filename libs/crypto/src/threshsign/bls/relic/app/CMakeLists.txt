
# Link against this when you want an AppMain() without RELIC support (i.e., initialized RELIC library)
# For example, generic non-RELIC tests and benchmarks will use this to avoid
# repeating the same boilerplate initialization code.
add_library(mainapp STATIC Main.cpp)
target_include_directories(mainapp PRIVATE ../../)

# Link against this when you want a RelicAppMain() with RELIC support (i.e., initialized RELIC library)
# For example, BLS threshold signature tests and benchmarks will use this to avoid
# repeating the same boilerplate initialization code.
add_library(relic_mainapp STATIC RelicMain.cpp)
target_include_directories(relic_mainapp PRIVATE ../../include ../../src ${RELIC_INCLUDE_DIRS})
