from conans import ConanFile
from conans import tools


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

    def build(self):
        self.run('CC=/usr/bin/gcc CXX=/usr/bin/g++ '
                'cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DALLOC=AUTO -DWSIZE=64 '
                '-DRAND=UDEV -DSHLIB=ON -DSTLIB=ON -DSTBIN=OFF -DTIMER=HREAL '
                '-DCHECK=on -DVERBS=on -DARITH=x64-asm-254 -DFP_PRIME=254 '
                '-DFP_METHD="INTEG;INTEG;INTEG;MONTY;LOWER;SLIDE" '
                '-DCOMP="-O3 -funroll-loops -fomit-frame-pointer -finline-small-functions -march=native -mtune=native" '
                '-DFP_PMERS=off -DFP_QNRES=on '
                '-DFPX_METHD="INTEG;INTEG;LAZYR" -DPP_METHD="LAZYR;OATEP"')
        self.run('CC=/usr/bin/gcc CXX=/usr/bin/g++ make')

    def _patch(self):
        # DD: RELIC defines MIN/MAX macros w/o checking if they already exist
        # This behavior leads to compilation errors
        tools.replace_path_in_file("include/relic_util.h",
                                    "#define MIN(A, B)			((A) < (B) ? (A) : (B))",
                                    '''#ifndef MIN
#define MIN(A, B)			((A) < (B) ? (A) : (B))
#endif''')

        tools.replace_path_in_file("include/relic_util.h",
                                    "#define MAX(A, B)			((A) > (B) ? (A) : (B))",
                                    '''#ifndef MAX
#define MAX(A, B)			((A) > (B) ? (A) : (B))
#endif''')

    def package(self):
        self._patch()
        self.copy("*.h", dst="relic", src="include")
        self.copy("*.lib", dst="lib", keep_path=False)
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.so*", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["relic"]
        self.cpp_info.includedirs = [self.package_folder]
