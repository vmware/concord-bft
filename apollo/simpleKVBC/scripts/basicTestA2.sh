#!/bin/bash
echo "Making sure no previous replicas are up..."
killall skvbc_replica

echo "Running replica 1..."
../TesterReplica/skvbc_replica -k setA_replica_ -i 0 >& /dev/null &
echo "Running replica 2..."
../TesterReplica/skvbc_replica -k setA_replica_ -i 1 >& /dev/null &
echo "Running replica 3..."
../TesterReplica/skvbc_replica -k setA_replica_ -i 2 >& /dev/null &

echo "Sleeping for 2 seconds"
sleep 2

echo "Running client!"
time ../TesterClient/skvbc_client -f 1 -c 0 -p 1800 -i 4  

echo "Finished!"
# Cleaning up
killall skvbc_replica
