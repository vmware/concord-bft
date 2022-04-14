#!/bin/bash
echo "Making sure no previous services are up..."
killall utt_replica
killall payment_service

echo "Running replica 1..."
../UTTReplica/utt_replica -k config/replica_ -n config/net_localhost.txt -i 0 >& /dev/null &
echo "Running replica 2..."
../UTTReplica/utt_replica -k config/replica_ -n config/net_localhost.txt -i 1 >& /dev/null &
echo "Running replica 3..."
../UTTReplica/utt_replica -k config/replica_ -n config/net_localhost.txt -i 2 >& /dev/null &
echo "Running replica 4..."
../UTTReplica/utt_replica -k config/replica_ -n config/net_localhost.txt -i 3 >& /dev/null &

echo "Running payment_service 1..."
../PaymentService/payment_service -n config/net_localhost.txt -f 1 -i 1 >& /dev/null &