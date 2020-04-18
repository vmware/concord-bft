#!/bin/bash -e

cleanup() {
  killall -q skvbc_replica || true
  rm -rf simpleKVBTests_DB_*
  rm -rf ro_config_*
}

trap 'cleanup' SIGINT

cleanup

../../../tools/GenerateConcordKeys -f 1 -n 4 -r 1 -o ro_config_

../TesterReplica/skvbc_replica -k ro_config_ -i 0 -p &
../TesterReplica/skvbc_replica -k ro_config_ -i 1 -p &
../TesterReplica/skvbc_replica -k ro_config_ -i 2 -p &
../TesterReplica/skvbc_replica -k ro_config_ -i 3 -p &

echo "Sleeping for 5 seconds"
sleep 5
time ../TesterClient/skvbc_client -f 1 -c 0 -p 400 -i 5

../TesterReplica/skvbc_replica -k ro_config_ -i 4 -p --s3-config-file test_s3_config.txt


echo "Finished!"
cleanup

