from conans import ConanFile, CMake
from os import path

class RapidCheckConan(ConanFile):
    scm = {
        "type": "git",
        "subfolder": ".",
        "url": "https://github.com/emil-e/rapidcheck.git",
        "revision": "258d907da00a0855f92c963d8f76eef115531716"
    }
    name = "rapidcheck"
    version = "258d907da00a0855f92c963d8f76eef115531716"
    license = "BSD 2-Clause"
    url = "https://github.com/emil-e/rapidcheck"
    description = "QuickCheck clone for C++"
    topics = ("rapidcheck", "property based testing")
    generators = "cmake"

    def build(self):
        cmake = CMake(self)
        cmake.definitions["RC_ENABLE_GTEST"] = "ON"
        cmake.definitions["RC_ENABLE_GMOCK"] = "ON"
        cmake.configure()
        cmake.build()

    def package(self):
        self.copy("*.h", dst="rapidcheck", src="include")
        self.copy("*.hpp", dst="rapidcheck", src="include")
        self.copy("*.h", dst="rapidcheck/extras", src="extras/gtest/include/rapidcheck")
        self.copy("*.hpp", dst="rapidcheck/extras", src="extras/gtest/include/rapidcheck")
        self.copy("*.h", dst="rapidcheck/extras", src="extras/gmock/include/rapidcheck")
        self.copy("*.hpp", dst="rapidcheck/extras", src="extras/gmock/include/rapidcheck")
        self.copy("*.lib", dst="lib", keep_path=False)
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.so*", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["rapidcheck"]
        self.cpp_info.includedirs = [path.join(self.package_folder, "rapidcheck")]
