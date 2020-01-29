from conans import ConanFile, CMake, tools, AutoToolsBuildEnvironment
from conans.tools import download, unzip
import shutil
import os

class GmpConan(ConanFile):
    name = "gmplib"
    version = "6.2.0"
    zip_name = "gmp-6.2.0.tar.xz"
    url = "https://gmplib.org"
    description = "RELIC is a modern cryptographic meta-toolkit with emphasis on efficiency and flexibility. " \
                  "RELIC can be used to build efficient and usable cryptographic toolkits tailored for specific " \
                  "security levels and algorithmic choices."
    generators = "cmake"

    def source(self):
        download("https://gmplib.org/download/gmp/" + self.zip_name, self.zip_name)
        self.run("tar xf " + self.zip_name)
        uncompress_name = "gmp-" + self.version
        shutil.move(uncompress_name, self.name)
        os.unlink(self.zip_name)

    def build(self):
        autotools = AutoToolsBuildEnvironment(self)
        autotools.configure(configure_dir=self.name)
        autotools.make()

    def package(self):
        self.copy("*.h", dst="include", src="")
        self.copy("*.lib", dst="lib", keep_path=False)
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.so", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["gmp"]
