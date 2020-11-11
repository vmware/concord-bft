#!/bin/bash -e

cleanup() {
  killall -q skvbc_replica || true
  killall -q skvbc_client || true
  killall -q minio || true
  rm -rf simpleKVBTests_DB_*
  rm -rf ro_config_*
  rm -rf s3-data/blockchain
}

trap 'cleanup' SIGINT

cleanup

../../../tools/GenerateConcordKeys -f 1 -n 4 -r 1 -o ror_perf_

../TesterReplica/skvbc_replica -k ror_perf_ -i 0 -l logging.properties &
../TesterReplica/skvbc_replica -k ror_perf_ -i 1 -l logging.properties &
../TesterReplica/skvbc_replica -k ror_perf_ -i 2 -l logging.properties &
../TesterReplica/skvbc_replica -k ror_perf_ -i 3 -l logging.properties &

mkdir -p s3-data/blockchain
MINIO_ACCESS_KEY="concordbft" MINIO_SECRET_KEY="concordbft" ~/minio server ./s3-data &

echo "Sleeping for 5 seconds"
sleep 5

# (check this document for explanation: https://confluence.eng.vmware.com/display/BLOC/ROR+%28Object+Store%29+performance+analysis)
# s3-op-delay sets delay per key. Client generates 200 keys per request => 20/200 = 0.1ms delay per req
../TesterReplica/skvbc_replica -k ror_perf_ -i 4 -p --s3-config-file test_s3_config.txt -l logging-ror.properties > ./ror.log 2>&1 &

sleep 3
python3 dump_stats.py &

while true; do
# The params below generate checkpoint on around each 10secs on dev machine (2020 mbp)
# The client generates 200 keys per request (hardcoded value).
../TesterClient/skvbc_client -f 1 -c 0 -p 40000 -i 5 -s 1 -d 10 > client.log 
done

echo "Finished!"
cleanup

