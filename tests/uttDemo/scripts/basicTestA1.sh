#!/bin/bash
echo "Making sure no previous replicas are up..."
killall utt_replica

echo "Running replica 1..."
../UTTReplica/utt_replica -k setA_replica_ -i 0 >& /dev/null &
echo "Running replica 2..."
../UTTReplica/utt_replica -k setA_replica_ -i 1 >& /dev/null &
echo "Running replica 3..."
../UTTReplica/utt_replica -k setA_replica_ -i 2 >& /dev/null &
echo "Running replica 4..."
../UTTReplica/utt_replica -k setA_replica_ -i 3 >& /dev/null &

echo "Sleeping for 2 seconds"
sleep 2

echo "Running client!"
time ../UTTClient/utt_client -f 1 -c 0 -p 1800 -i 4  

echo "Finished!"
# Cleaning up
killall utt_replica
