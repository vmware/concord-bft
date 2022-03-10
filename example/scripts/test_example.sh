#!/bin/bash -e

cleanup() {
  killall -q test_replica || true
  killall -q minio || true
  rm -rf gen-sec.*
  rm -rf exampleReplicaTests_DB_*
}

#trap 'cleanup' SIGINT

cleanup

scriptdir=$(cd $(dirname $0); pwd -P)
echo $scriptdir

echo "Generating new keys..."
$scriptdir/../../tools/GenerateConcordKeys -f 1 -n 4 -o replica_keys_

# Generates num_participants number of key pairs
$scriptdir/create_concord_clients_transaction_signing_keys.sh -n 5 -o /tmp

# Generate TLS certificates
rm -rf $scriptdir/certs/
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

env MINIO_ROOT_USER=concordbft MINIO_ROOT_PASSWORD=concordbft ~/minio server minio_data_dir &

sleep 5

echo "Finished!"
# cleanup
