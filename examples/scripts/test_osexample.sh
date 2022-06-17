#!/bin/bash -e

cleanup() {
  killall -q test_replica || true
  killall -q minio || true
  rm -rf gen-sec.*
  rm -rf exampleReplicaTests_DB_*
  rm -rf certs
  rm -rf minio_data_dir
  rm -rf replica_keys_*
}

cleanup

scriptdir=$(cd $(dirname $0); pwd -P)
echo $scriptdir

echo "Starting example demo run..."

cd $scriptdir

echo "Generating new keys..."
$scriptdir/../../tools/GenerateConcordKeys -f 1 -n 4 -o replica_keys_

# Generates num_participants number of key pairs
$scriptdir/create_concord_clients_transaction_signing_keys.sh -n 5 -o /tmp

echo "Generating SSL certificates for TlsTcp communication..."
$scriptdir/create_tls_certs.sh 10

# run 4 replica's with unique replica id's
echo "Running replica 1..."
$scriptdir/../replica/test_replica -i 0 -a $scriptdir/replica_conf &
echo "Running replica 2..."
$scriptdir/../replica/test_replica -i 1 -a $scriptdir/replica_conf &
echo "Running replica 3..."
$scriptdir/../replica/test_replica -i 2 -a $scriptdir/replica_conf &
echo "Running replica 4..."
$scriptdir/../replica/test_replica -i 3 -a $scriptdir/replica_conf &

sleep 10

echo "Running client!"
time $scriptdir/../client/test_client -f 1 -c 0 -i 4 -r 4 -e 0 -m $scriptdir/../msg-configs/msg-1

sleep 5
echo "Run completed!"
