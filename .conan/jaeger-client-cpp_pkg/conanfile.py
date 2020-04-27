from conans import ConanFile
from conans import CMake
from conans import tools


class JaegerClientCpp(ConanFile):
    scm = {
        "type": "git",
        "subfolder": ".",
        "url": "https://github.com/jaegertracing/jaeger-client-cpp.git",
        "revision": "v0.5.0"
    }
    name = "jaeger-client-cpp"
    version = "0.5.0"
    url = "https://github.com/jaegertracing/jaeger-client-cpp"
    license = "Apache-2.0"
    description = "C++ OpenTracing binding for Jaeger"
    settings = "os", "compiler", "build_type", "arch"

    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = "shared=True", "fPIC=True"
    generators = "cmake"
    exports_sources = "Findthrift.cmake"

    def source(self):
        tools.replace_in_file("CMakeLists.txt", "project(jaegertracing VERSION 0.5.0)",
                      '''project(jaegertracing VERSION 0.5.0)
include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()''')

    def requirements(self):
        self.requires("jsonformoderncpp/3.7.3@vthiery/stable")
        self.requires("OpenSSL/1.1.1@conan/stable")
        self.requires("yaml-cpp/0.5.3")
        self.requires("opentracing-cpp/1.5.0")
        self.requires("thrift/0.11.0")

    def configure(self):
        if self.options.shared:
            self.options["opentracing-cpp"].fPIC = True

    def build(self):
        self.cmake = CMake(self)
        self.cmake.definitions["HUNTER_ENABLED"] = "NO"
        self.cmake.definitions["BUILD_TESTING"] = "NO"
        self.cmake.definitions["JAEGERTRACING_BUILD_EXAMPLES"] = "NO"
        if self.options.shared:
            self.cmake.definitions["BUILD_SHARED_LIBS"] = "YES"
        else:
            self.cmake.definitions["BUILD_SHARED_LIBS"] = "NO"
        self.cmake.configure()
        self.cmake.build()

    def _patch_cmake(self):
        # DD: Jaeger really wants to find BoostConfig.cmake, not
        # FindBoost.cmake. But, this wasn't introduced until boost 1.70. This
        # doesn't matter anyway, because the jaegertracing.cmake finds
        # FindBoost.cmake first anyway. The following sed just removes the
        # search for BoostConfig.cmake.
        tools.replace_in_file("generated/jaegertracingConfig.cmake",
                                "set(boost_components )", "")
        tools.replace_in_file("generated/jaegertracingConfig.cmake",
                                "find_package(Boost CONFIG REQUIRED ${boost_components})",
                                "")
        tools.replace_in_file("CMakeLists.txt", "list(APPEND LIBS ${THRIFT_LIBRARIES}",
                '''include_directories(${THRIFT_INCLUDE_DIRS})
  message("JAEGER THRIFT DIRS:" ${THRIFT_INCLUDE_DIRS})
  message("JAEGER THRIFT DIR:" ${THRIFT_INCLUDE_DIR})
  list(APPEND LIBS ${THRIFT_LIBRARIES}''')

    def package(self):
        self._patch_cmake()
        self.cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["jaegertracing"]
