from conans import ConanFile, CMake, tools, AutoToolsBuildEnvironment
from conans.tools import download, unzip
import shutil
import os

class RocksdbConan(ConanFile):
    scm = {
        "type": "git",
        "subfolder": ".",
        "url": "https://github.com/facebook/rocksdb.git",
        "revision": "v5.7.3"
    }
    name = "rocksdb"
    version = "5.7.3"
    license = "GPLv2"
    url = "https://github.com/facebook/rocksdb"
    description = "RocksDB is developed and maintained by Facebook Database Engineering Team." \
                  "It is built on earlier work on LevelDB by Sanjay Ghemawat (sanjay@google.com) " \
                  "and Jeff Dean (jeff@google.com)"

    def requirements(self):
        self.requires("zlib/1.2.11@conan/stable")
        self.requires("bzip2/1.0.8@conan/stable")
        self.requires("lz4/1.8.0@bincrafters/stable")
        self.requires("snappy/1.1.7@bincrafters/stable")
        self.requires("zstd/1.4.0@bincrafters/stable")

    def configure(self):
        self.options["zlib"].shared = True
        self.options["bzip2"].shared = True
        self.options["lz4"].shared = True
        self.options["snappy"].shared = True
        self.options["zstd"].shared = True

    def build(self):
        autotools = AutoToolsBuildEnvironment(self)
        args = ['shared_lib']
        autotools.make(args=args)

    def package(self):
        self.copy("*.h", dst="", src="include")
        self.copy("*.lib", dst="lib", keep_path=False)
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.so*", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)


def package_info(self):
        self.cpp_info.libs = ["rocksdb"]
        self.cpp_info.includedirs = [self.package_folder]

