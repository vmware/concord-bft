#!/bin/sh

set -e

scriptdir=$(cd $(dirname $0); pwd -P)
. $scriptdir/shlibs/os.sh

if [ $# -ne 2 ]; then
    echo "Usage: $0 <output-trap-file> <q>"
    exit 1
fi

trap_file=$1; shift 1;
q=$1; shift 1;
num_chunks=$NUM_CPUS

if [ $q -lt $num_chunks ]; then
    num_chunks=$q
fi

chunk_size=$(($q/$num_chunks))

echo "Generating q-SDH params in '$trap_file' (q = $q, numChunks = $NUM_CPUS, chunk_size = $chunk_size) ..."

ParamsGenTrapdoors "$trap_file" $q

for i in `seq 0 $(($num_chunks-1))`; do
    start=$(($i * $chunk_size))
    if [ $i -lt $(($num_chunks - 1)) ]; then
        end=$((($i+1) * $chunk_size))
    else
        end=$(($q+1))
    fi

    echo "Generating [$start, $end) ..."
    # Send to background
    out_file=${trap_file}-$i
    ParamsGenPowers "$trap_file" "$out_file" $start $end &>$out_file.log &
done
