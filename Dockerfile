FROM pr-concord-bft

#RUN source ~/.profile
COPY . /concord-bft

RUN apt-get -y install sudo

ARG CMAKE_CXX_FLAGS="-DCMAKE_CXX_FLAGS_RELEASE=-O3 -g"
ARG USE_ROCKSDB="-DBUILD_ROCKSDB_STORAGE=TRUE"
ARG DEBUG="-DCMAKE_BUILD_TYPE=DEBUG"
ARG USE_LOG4CPLUS="ON"
RUN cd /concord-bft && mkdir build && cd build && conan install --build missing .. && cmake $CMAKE_CXX_FLAGS $DEBUG $RELEASE $USE_ROCKSDB .. && make -j $(getconf _NPROCESSORS_ONLN)

ENTRYPOINT cd /concord-bft/build && ctest  --output-on-failure