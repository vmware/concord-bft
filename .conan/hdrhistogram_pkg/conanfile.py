from conans import ConanFile, CMake, tools


class HdrhistogramConan(ConanFile):
    scm = {
        "type": "git",
        "subfolder": ".",
        "url": "https://github.com/HdrHistogram/HdrHistogram_c.git",
        "revision": "0.9.12"
    }
    name = "hdr_histogram"
    version = "0.9.12"
    license = "BSD 2-Clause"
    url = "http://github.com/HdrHistogram/HdrHistogram_c"
    description = "A HDR histogram library"
    topics = ("HDR histogram")

    def requirements(self):
        self.requires("zlib/1.2.11@conan/stable")

    def configure(self):
        self.options["zlib"].shared = True

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        self.copy("*.h", dst="hdr_histogram", src="src")
        self.copy("*hdr_histogram.lib", dst="lib", keep_path=False)
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.so*", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["hdr_histogram"]
        self.cpp_info.includedirs = [self.package_folder]
