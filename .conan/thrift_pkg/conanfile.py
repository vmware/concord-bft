from conans import AutoToolsBuildEnvironment
from conans import ConanFile
from conans import tools
import os


"This package is only for thrift 0.11.0"


class ThriftConan(ConanFile):
    name = "thrift"
    url = "https://github.com/apache/thrift"
    version = "0.11.0"
    license = "Apache-2.0"
    description = "Thrift is an associated code generation mechanism for RPC"

    source_dir = "thrift-0.11.0"
    targz_name = source_dir + ".tar.gz"
    exports_sources = "Findthrift.cmake"

    def requirements(self):
        self.requires("OpenSSL/1.1.1@conan/stable")
        self.requires("boost/1.64.0@conan/stable")
        self.requires("FindConanBoost/0.1")
        self.requires("zlib/1.2.11@conan/stable")

    def source(self):
        tools.download(
                "http://apache.mirrors.hoobly.com/thrift/0.11.0/thrift-0.11.0.tar.gz",
                self.targz_name)
        tools.unzip(filename=self.targz_name)
        self.run("mv %s/* ."%self.source_dir)

    def build(self):
        self.autotools = AutoToolsBuildEnvironment(self)
        self.autotools.configure(args = ["--with-cpp=yes",
                                    "--with-c_glib=yes",
                                    "--with-boost=yes",
                                    "--with-zlib=yes",
                                    "--with-libevent=no",
                                    "--enable-tests=no",
                                    "--enable-tutorial=no",
                                    "--with-libevent=no",
                                    "--with-qt4=no",
                                    "--with-qt5=no",
                                    "--with-csharp=no",
                                    "--with-java=no",
                                    "--with-erlang=no",
                                    "--with-nodejs=no",
                                    "--with-lua=no",
                                    "--with-python=no",
                                    "--with-perl=no",
                                    "--with-php=no",
                                    "--with-php_extension=no",
                                    "--with-dart=no",
                                    "--with-ruby=no",
                                    "--with-haskell=no",
                                    "--with-go=no",
                                    "--with-rs=no",
                                    "--with-haxe=no",
                                    "--with-dotnetcore=no",
                                    "--with-d=no"])
        self.autotools.make()

    def package(self):
        self.autotools.install()
        self.copy("Findthrift.cmake", dst="", src="")

    def package_info(self):
        self.cpp_info.libs = ["thrift"]
        self.cpp_info.includedirs = [os.path.join(self.package_folder, "thrift")]
