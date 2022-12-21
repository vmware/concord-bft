#!/bin/bash

# This script builds and installs additional debug, performance, and analysis tools for Concord-BFT development.
# The script was validated on specific Linux distribution only (Ubuntu 18.04 LTS).
# If you prefer working w/o docker at your dev station just run the script with sudo.
# If you need to add any tool or dependency this is the right place to do it.
# If you install temporary packages with apt-get and would like to remove them, add them to apt_pkgs_to_purge to
# be removed at the end of the script.

set -ex

APT_GET_INSTALL_FLAGS="-y --no-install-recommends"
TMPDIR=/tmp/
apt_pkgs_to_purge=() # To save time between calls to installs, purge all packages only at the end

# Install boost permanent and temporary library.
# install_deps.sh installs a custome new boost version 1.80 from source.
# Current Ubuntu doesn't support installing boost 1.80 from pre-compiled packages. Also, downloading and installing boost
# can take a long time, so here we add permanent/temporary additional boost packages from boost 1.80 for simplicity.
# Currently, only temporary libraries are built and installed.
# The temporary libraries are removed in uninstall_boost_temporary_libraries.
# To have permanent libraries, add BOOST_BOOTSTRAP_LIBRARIES
BOOST_BOOTSTRAP_TEMPORARY_LIBRARIES="system,iostreams,filesystem" # comma-seperated
install_boost() {
    cd ${TMPDIR}
    git clone --recurse-submodules --depth=1 --single-branch --branch=boost-1.80.0 https://github.com/boostorg/boost.git
    cd boost
    ./bootstrap.sh --with-libraries=${BOOST_BOOTSTRAP_TEMPORARY_LIBRARIES}
    ./b2 install
    # uninstall_boost_temporary_libraries will remove the temporary boost packages and the boost repository
}

# Heaptrack (at least) requirs QT, we use aqtinstall to install QT
install_qt() {
  QT_VER=5.15.2
  QT_BASE_PATH=/opt/qt/${QT_VER}/gcc_64/

  cd ${TMPDIR}
  temp_pkgs="libunwind-dev  libkchart-dev extra-cmake-modules mesa-common-dev libglu1-mesa-dev \
      libkf5filemetadata-dev libkf5kio-dev libkf5threadweaver-dev libkf5itemmodels-dev gettext"
  apt-get install ${APT_GET_INSTALL_FLAGS} ${temp_pkgs}
  pip3 install aqtinstall
  aqt install-qt linux desktop ${QT_VER} --outputdir /opt/qt

  apt_pkgs_to_purge+=(${temp_pkgs})
  pip3 uninstall aqtinstall -y
  rm -rf /${TMPDIR}/aqtinstall.log
}

# A heap memory usage profiler - installs heaptrack,heaptrack_print AND heaptrack_gui
install_heaptrack() {
  HEAPTRACK_VER=1.4.0
  git clone https://github.com/KDE/heaptrack.git
  cd heaptrack
  git checkout v${HEAPTRACK_VER}
  mkdir build && cd build
  cmake -DCMAKE_BUILD_TYPE=Release -DHEAPTRACK_BUILD_GUI:BOOL=ON -DCMAKE_PREFIX_PATH=${QT_BASE_PATH} ..
  make -j$(nproc)
  make install
  rm -rf ${TMPDIR}/heaptrack && cd ${HOME}

  echo "export LD_LIBRARY_PATH=${QT_BASE_PATH}/lib/" >> ~/.bashrc
  echo "export PATH=${QT_BASE_PATH}/bin/:$PATH" >> ~/.bashrc

  # check that all binaries are installed
  for binary in "heaptrack_gui" "heaptrack" "heaptrack_print"; do
    if [[ $(which ${binary}) == "" ]]; then echo
      "Error: Failed to install ${binary}"
      exit 1
    fi
  done
}

# Uninstall all boost temporary libraries defined in BOOST_BOOTSTRAP_TEMPORARY_LIBRARIES. This should be called last.
uninstall_boost_temporary_libraries() {
  libs=($(echo BOOST_BOOTSTRAP_TEMPORARY_LIBRARIES | tr "," " "))
  for lib in "${libs[@]}"; do
    echo "Remove temporary boost library ${lib}"
    rm -rf /usr/local/include/boost/${lib}
    rm -rf /usr/local/include/boost/${lib}.hpp
    find /usr/local/lib/ -iname libboost_${lib}.so* -exec rm {} \;
    rm -rf /usr/local/lib/libboost_${lib}.a
  done
  cd ${HOME}
  rm -rf ${TMPDIR}/boost
}

apt-get update -y
pip3 install -U pip
install_boost
install_qt
install_heaptrack
# apt/apt-get autoremove should not be called since some dependencies are not installed via package manager
uninstall_boost_temporary_libraries # must be last
apt purge -y ${apt_pkgs_to_purge[*]}
# After installing all libraries, let's make sure that they will be found at compile time
ldconfig -v
