#!/bin/bash
FOLDER=~/archipelago/paper/rslt
mkdir -p $FOLDER/concord
mkdir config
if [ "$1" == "client_inc" ]; then
    echo "RUN client increase experiment"
    for ((i=1;i<512;i=i*2)); do
        python generate_config.py config/test concord 4 4 16 $i false
        python run.py config/test_servers_4_clients_$i\_concord.json
        python analyze.py latest_rslt/config.json
        cp latest_rslt/rslt.json $FOLDER/concord/client_inc_servers_4_clients_$i.json
    done
else
    echo "RUN server increase experiment"
    for ((i=1;i<=5;i=i+1)); do
        numservers=$(($i*3+1))
        python generate_config.py config/test concord $numservers $numservers 4 64 false
        python run.py config/test_servers_$numservers\_clients_64_concord.json
        python analyze.py latest_rslt/config.json
        cp latest_rslt/rslt.json $FOLDER/concord/server_inc_servers_$numservers\_clients_64.json
    done
fi

