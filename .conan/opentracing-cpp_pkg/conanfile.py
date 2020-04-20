# Based on https://github.com/rodolfo-picoreti/conan_packages/blob/master/opentracing-cpp/conanfile.py
from conans import ConanFile, CMake, tools


class OpentracingCppConan(ConanFile):
    name = "opentracing-cpp"
    version = "1.5.0"
    license = "Apache-2.0"
    url = "https://github.com/opentracing/opentracing-cpp"
    description = "OpenTracing API for C++"

    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = "shared=False", "fPIC=True"
    generators = "cmake"

    def source(self):
        self.run("git clone https://github.com/opentracing/opentracing-cpp")
        self.run("cd opentracing-cpp && git checkout v1.5.0")
        tools.replace_in_file("opentracing-cpp/CMakeLists.txt", "project(opentracing-cpp)",
                              '''project(opentracing-cpp)
include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()''')

    def build(self):
        self.cmake = CMake(self)
        self.cmake.definitions["BUILD_TESTING"] = "OFF"
        if self.options.shared:
            self.cmake.definitions["BUILD_SHARED_LIBS"] = "ON"
            self.cmake.definitions["BUILD_STATIC_LIBS"] = "OFF"
        else:
            self.cmake.definitions["BUILD_STATIC_LIBS"] = "ON"
            self.cmake.definitions["BUILD_SHARED_LIBS"] = "OFF"
            self.cmake.definitions["CMAKE_POSITION_INDEPENDENT_CODE"] = self.options.fPIC
        self.cmake.configure(source_folder="opentracing-cpp")
        self.cmake.build()

    def package(self):
        self.cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["opentracing"]
