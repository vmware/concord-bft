cmake_minimum_required (VERSION 3.2)
project(libccron LANGUAGES CXX)

# Set the minumum version to 3.13 as we want the NEW CMP0079 behavior:
# https://cmake.org/cmake/help/latest/policy/CMP0079.html
# We need it in order to be able to call target_link_libraries() for corebft from the ccron directory.
# That will essentially make ccron part of the corebft library. We do that, because we want to:
#  1. Avoid cyclic dependencies between the potential ccron and the corebft libraries.
#  2. Avoid the need for type erasure, i.e. by using a std::function callback or inheritance for calling
#     onInternalTick() inside ReplicaImp.
#  3. Avoid encapsulation violations - reason is that all parameters needed to construct a TicksGenerator are
#     protected ReplicaImp members and due to point 1), we cannot make the TicksGenerator a member of ReplicaImp
#     if ccron was a separate library.
# All of above might change when code is refactored such that corebft is split further.
# Another option is to move ccron inside the bftengine directory. 
cmake_policy(VERSION 3.13)

target_include_directories(corebft PUBLIC include)
target_link_libraries(corebft PUBLIC ccron_msgs)

add_subdirectory(cmf)

if (BUILD_TESTING)
  add_subdirectory(test)
endif()
