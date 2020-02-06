from conans import ConanFile, CMake, tools
from conans.tools import os_info, SystemPackageTool


class LibS3Conan(ConanFile):
    name = "libs3-dev"
    version = "2.0-3"
    def system_requirements(self):
        pack_name = None
        if os_info.linux_distro == "ubuntu":
            pack_name = "libs3-dev"
 
        if pack_name:
            installer = SystemPackageTool()
            installer.install(pack_name)