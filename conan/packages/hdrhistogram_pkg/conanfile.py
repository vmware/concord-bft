from conans import ConanFile, CMake, tools


class HdrhistogramConan(ConanFile):
    name = "hdr_histogram"
    version = "0.9.12"
    license = "BSD 2-Clause"
    author = "Yehonatan Buchnik ybuchnik@vmware.com"
    url = "http://github.com/HdrHistogram/HdrHistogram_c"
    description = "A HDR histogram library"
    topics = ("HDR histogram")
    options = {"build_programs": [True, False]}
    default_options = {"build_programs": True}
    generators = "cmake"

    def source(self):
        git = tools.Git()
        git.clone("https://github.com/HdrHistogram/HdrHistogram_c.git", "0.9.12")

        # This small hack might be useful to guarantee proper /MT /MD linkage
        # in MSVC if the packaged project doesn't have variables to set it
        # properly
        tools.replace_in_file("CMakeLists.txt", 'project("hdr_histogram")',
                              '''project("hdr_histogram")
include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()''')

    def build(self):
        cmake = CMake(self)
        cmake.configure(source_folder="src")
        build_args = "-DHDR_HISTOGRAM_BUILD_PROGRAMS=ON" if self.options.build_programs else ""
        self.run("cmake . %s %s" % (cmake.command_line, build_args))

    def package(self):
        self.copy("*.h", dst="include", src="src")
        self.copy("*hello.lib", dst="lib", keep_path=False)
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.so.*", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["hdr_histogram"]

