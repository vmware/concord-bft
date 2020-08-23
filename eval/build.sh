export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib && export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:/usr/local/include
mkdir ../build
cd ../build
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_COMM_TCP_PLAIN=TRUE ..
make -j 4
cd ../eval
