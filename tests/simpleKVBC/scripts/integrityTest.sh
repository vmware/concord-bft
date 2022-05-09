#!/bin/bash -e

cleanup() {
  killall -q skvbc_replica || true
  killall -q minio || true
  rm -rf simpleKVBTests_DB_*
  rm -rf ro_config_*
  rm -rf gen-sec.*
}

#trap 'cleanup' SIGINT

cleanup

../../../tools/GenerateConcordKeys -f 1 -n 4 -r 1 -o ro_config_

../TesterReplica/skvbc_replica -k ro_config_ -i 0 -h 10 &
../TesterReplica/skvbc_replica -k ro_config_ -i 1 &
../TesterReplica/skvbc_replica -k ro_config_ -i 2 &
../TesterReplica/skvbc_replica -k ro_config_ -i 3 &

../TesterClient/skvbc_client -f 1 -c 0 -p 20000 -i 5 &

env MINIO_ROOT_USER=concordbft MINIO_ROOT_PASSWORD=concordbft ~/minio server --console-address 127.0.0.1:12345 minio_data_dir &

sleep 5

../TesterReplica/skvbc_replica -k ro_config_ -i 4 --s3-config-file test_s3_config_prefix.txt


echo "Finished!"

