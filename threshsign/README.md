threshsign library
==================

This library can compile by itself, without needing to compile all of Concord BFT.

Build instructions
------------------

**Assumption:** Your terminal is pointed to the threshsign directory where this README file is.

    mkdir -p build/
    cd build/
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
    make -j `grep -c ^processor /proc/cpuinfo`

You have now successfully built the library, including tests and benchmarks.
(Feel free to change `RelWithDebInfo` to `Debug` or `Trace` if you are debugging and want more verbose logs.)
