from conans import ConanFile, CMake, tools


class YAMLCppConan(ConanFile):
    name = "yaml-cpp"
    version = "0.5.3"
    url = "https://github.com/uilianries/conan-yaml-cpp"
    homepage = "https://github.com/jbeder/yaml-cpp"
    description = "A YAML parser and emitter in C++"
    license = "MIT"
    exports = "LICENSE.md"
    exports_sources = "CMakeLists.txt"
    generators = "cmake"
    settings = "os", "arch", "compiler", "build_type"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = "shared=False", "fPIC=True"
    source_subfolder = "source_subfolder"
    build_subfolder = "build_subfolder"

    def requirements(self):
        self.requires("boost/1.64.0@conan/stable")
        self.requires("FindConanBoost/0.1")

    def config_options(self):
        if self.settings.os == 'Windows':
            self.options.remove("fPIC")

    def source(self):
        tools.get("{0}/archive/release-{1}.tar.gz".format(self.homepage, self.version))
        extracted_dir = self.name + "-release-" + self.version
        self.run("mv %s/* ."%extracted_dir)

        tools.replace_in_file("CMakeLists.txt", "project(YAML_CPP)",
                      '''project(YAML_CPP)
include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()''')

    def configure_cmake(self):
        cmake = CMake(self)
        cmake.definitions["YAML_CPP_BUILD_CONTRIB"] = True
        cmake.definitions["YAML_CPP_BUILD_TOOLS"] = False
        cmake.definitions["BUILD_SHARED_LIBS"] = True
        if self.settings.os == "Windows" and self.options.shared:
            cmake.definitions["BUILD_SHARED_LIBS"] = True
        cmake.configure()
        return cmake

    def build(self):
        cmake = self.configure_cmake()
        cmake.build()

    def package(self):
        cmake = self.configure_cmake()
        cmake.install()
        #cmake.install of this package does not copy cmake files
        #copy only specific cmakes
        self.copy("yaml-cpp-config.cmake", dst="lib/cmake/yaml-cpp/", src="")
        self.copy("yaml-cpp-config-version.cmake", dst="lib/cmake/yaml-cpp/", src="")
        self.copy("yaml-cpp-targets.cmake", dst="lib/cmake/yaml-cpp/", src="")
        self.copy("yaml-cpp-targets-release.cmake", dst="lib/cmake/yaml-cpp/", src="")

    def package_info(self):
        self.cpp_info.libs = tools.collect_libs(self)

