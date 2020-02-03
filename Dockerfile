FROM pr-concord-bft


RUN apt-get update && \
    apt-get install -y ccache cmake clang-format python3-pip python3-setuptools git

RUN python3 -m pip install --upgrade wheel conan trio

RUN conan profile new default --detect
RUN conan profile update settings.compiler.libcxx=libstdc++11 default

#RUN source ~/.profile
COPY . /concord-bft
RUN sh /concord-bft/.conan/install_conan_pkgs.sh

ARG CMAKE_CXX_FLAGS="-DCMAKE_CXX_FLAGS_RELEASE=-O3 -g"
ARG USE_ROCKSDB="-DBUILD_ROCKSDB_STORAGE=TRUE"
ARG DEBUG="-DCMAKE_BUILD_TYPE=DEBUG"
RUN cd /concord-bft && mkdir build && cd build && conan install --build missing .. && cmake $CMAKE_CXX_FLAGS $DEBUG $RELEASE $USE_ROCKSDB .. && make -j $(getconf _NPROCESSORS_ONLN) && ctest --output-on-failure

ENTRYPOINT cd /concord-bft/build && ctest