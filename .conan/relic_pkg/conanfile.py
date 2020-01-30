from conans import ConanFile, CMake, tools


class RelicConan(ConanFile):
    scm = {
        "type": "git",
        "subfolder": ".",
        "url": "https://github.com/relic-toolkit/relic.git",
        "revision": "b984e901ba78c83ea4093ea96addd13628c8c2d0"
    }
    settings = "os", "compiler", "build_type", "arch"
    name = "relic"
    version = "0.4.0"
    license = "Apache-2.0"
    url = "https://github.com/relic-toolkit/relic"
    description = "RELIC is a modern cryptographic meta-toolkit with emphasis on efficiency and flexibility. " \
                  "RELIC can be used to build efficient and usable cryptographic toolkits tailored for specific " \
                  "security levels and algorithmic choices."
    generators = "cmake"

    def requirements(self):
        self.requires("gmp/6.1.2@bincrafters/stable")

    #
    def configure(self):
        self.options["gmp"].shared = False

    def source(self):
        tools.replace_in_file("CMakeLists.txt", 'project(RELIC C CXX)',
                              '''project(RELIC C CXX)
include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()
''')

    def build(self):
        cmake = CMake(self)
        build_args = {"CMAKE_BUILD_TYPE": "RelWithDebInfo", 'ALLOC': 'AUTO', 'WSIZE': 64,
                      'RAND': 'UDEV', 'SHLIB': 'on', 'STLIB': 'on', 'STBIN': 'off', 'TIMER': 'HREAL', 'CHECK': 'on',
                      'VERBS': 'on', 'ARITH': 'x64-asm-254', 'FP_PRIME': 254,
                      'FP_METHD': 'INTEG;INTEG;INTEG;MONTY;LOWER;SLIDE',
                      'COMP': '-O3 -funroll-loops -fomit-frame-pointer -finline-small-functions -march=native -mtune=native',
                      'FP_PMERS': 'off', 'FP_QNRES': 'on', 'FPX_METHD': 'INTEG;INTEG;LAZYR', 'PP_METHD': 'LAZYR;OATEP',
                      'CMAKE_CXX_COMPILER': 'g++', 'CMAKE_C_COMPILER': 'gcc'}
        cmake.configure(defs=build_args)
        cmake.build()

    def package(self):
        self.copy("*.h", dst="relic", src="include")
        self.copy("*.lib", dst="lib", keep_path=False)
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.so*", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["relic"]
        self.cpp_info.includedirs = [self.package_folder]
