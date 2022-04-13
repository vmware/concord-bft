#!/bin/bash
echo "Making sure no previous replicas are up..."
killall utt_replica

echo "Running replica 1..."
../UTTReplica/utt_replica -k config/replica_ -n config/net_localhost.txt -i 0 >& /dev/null &
echo "Running replica 2..."
../UTTReplica/utt_replica -k config/replica_ -n config/net_localhost.txt -i 1 >& /dev/null &
echo "Running replica 3..."
../UTTReplica/utt_replica -k config/replica_ -n config/net_localhost.txt -i 2 >& /dev/null &
echo "Running replica 4..."
../UTTReplica/utt_replica -k config/replica_ -n config/net_localhost.txt -i 3 >& /dev/null &
