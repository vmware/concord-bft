#!/bin/sh
set -e

scriptdir=$(cd $(dirname $0); pwd -P)

echo "Generating new keys..."

rm -f private_replica_*

../../../../tools/GenerateConcordKeys -n 4 -f 1 -o private_replica_

parallel --halt now,fail=1 -j0 ::: \
    "$scriptdir/../server 0" \
    "$scriptdir/../server 1" \
    "$scriptdir/../server 2" \
    "$scriptdir/../server 3" &
	
repl_pid=$!

$scriptdir/../client

# Once the client is done, kill the 4 replicas (need to send to SIGTERMs to GNU parallel for this)
echo
echo "Client is done, killing 'parallel' at PID $repl_pid"
echo
kill $repl_pid
sleep 2
kill $repl_pid

# On Linux, we just have to kill them manually.
echo
echo "Killing server processes named '$scriptdir/../server'"
killall "$scriptdir/../server" || :

