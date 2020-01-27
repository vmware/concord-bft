from conans import ConanFile, CMake, tools


class HdrhistogramConan(ConanFile):
    name = "relic"
    version = "0.2.1"
    license = "BSD 2-Clause"
    author = "Yehonatan Buchnik ybuchnik@vmware.com"
    url = "https://github.com/relic-toolkit/relic"
    description = "The relic library"
    topics = ("HDR histogram")
    options = {"build_programs": [True, False]}
    default_options = {"build_programs": True}
    generators = "cmake"

    def source(self):
        self.run("git clone https://github.com/relic-toolkit/relic.git .")
        self.run("git checkout b984e901ba78c83ea4093ea96addd13628c8c2d0")
#         tools.replace_in_file("CMakeLists.txt", 'project(RELIC C CXX)',
#                               '''project(RELIC C CXX)
# include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
# conan_basic_setup()''')

    def build(self):
        cmake = CMake(self)
        cmake.configure(source_folder=".")
        build_args = [cmake.command_line, "-DCMAKE_BUILD_TYPE=RelWithDebInfo", '-DALLOC=AUTO', '-DWSIZE=64',
                      '-DRAND=UDEV', '-DSHLIB=ON', '-DSTLIB=ON', '-DSTBIN=OFF', '-DTIMER=HREAL', '-DCHECK=on',
                      '-DVERBS=on', '-DARITH=x64-asm-254', '-DFP_PRIME=254', '-DFP_METHD="INTEG;INTEG;INTEG;MONTY;LOWER;SLIDE"',
                      '-DCOMP="-O3 -funroll-loops -fomit-frame-pointer -finline-small-functions -march=native -mtune=native"',
                      '-DFP_PMERS=off', '-DFP_QNRES=on', '-DFPX_METHD="INTEG;INTEG;LAZYR"', '-DPP_METHD="LAZYR;OATEP"']
        self.run("cmake . " + "%s "* len(build_args) % tuple(build_args))
        self.run("make")

    def package(self):
        self.copy("*.h", dst="lib", src="include")
        self.copy("*hello.lib", dst="lib", keep_path=False)
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.so.*", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["relic"]

