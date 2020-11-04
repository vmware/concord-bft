#!/bin/bash -e

cleanup() {
  killall -q skvbc_replica || true
  rm -rf simpleKVBTests_DB_*
  rm -rf ro_config_*
}

trap 'cleanup' SIGINT

cleanup

../../../tools/GenerateConcordKeys -f 1 -n 4 -r 1 -o ror_perf_

../TesterReplica/skvbc_replica -k ror_perf_ -i 0 -l logging.properties &
../TesterReplica/skvbc_replica -k ror_perf_ -i 1 -l logging.properties &
../TesterReplica/skvbc_replica -k ror_perf_ -i 2 -l logging.properties &
../TesterReplica/skvbc_replica -k ror_perf_ -i 3 -l logging.properties &

echo "Sleeping for 5 seconds"
sleep 5
# The params below generate checkpoint on around each 10secs on dev machine (2020 mbp)
# The client generates 200 keys per request (hardcoded value).
time ../TesterClient/skvbc_client -f 1 -c 0 -p 40000 -i 5 -s 1 -d 20 > client.log &

# (check this document for explanation: https://confluence.eng.vmware.com/display/BLOC/ROR+%28Object+Store%29+performance+analysis)
# s3-op-delay sets delay per key. Client generates 200 keys per request => 20/200 = 0.1ms delay per req
../TesterReplica/skvbc_replica -k ror_perf_ -i 4 -p --s3-config-file test_s3_config.txt --ror-performance --s3-op-delay 10 -l logging.properties &

sleep 3
python3 dump_stats.py 

echo "Finished!"
cleanup

