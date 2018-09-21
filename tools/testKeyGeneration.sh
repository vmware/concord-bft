#!/bin/bash

# Test GenerateConcordKeys using TestGeneratedKeys for several different
# dimensions of Concord clusters.

# Only run the large test if it was explicitly requested, since it may take a
# while to run.

run_large_test=false

for i in "$@"
do
  if [ "$i" = "--run_large_test" ]; then
    run_large_test=true
  else
    echo "testKeyGeneration: Unrecognized Argument: $i"
    exit -1
  fi
done

echo "testKeyGeneration: Testing GenerateConcordKeys for several different" \
  " cluster sizes..."

rm test_keyfile_*
echo "Generating keys for a 4-replica cluster..."
./GenerateConcordKeys -f 1 -n 4 -o test_keyfile_
echo "Done. Testing the keys..."
if ! ./TestGeneratedKeys -n 4 -o test_keyfile_; then
  echo "testKeyGeneration: FAILURE"
  exit -1
fi

rm test_keyfile_*
echo "Generating keys for a 6-replica cluster..."
./GenerateConcordKeys -f 1 -n 6 -o test_keyfile_
echo "Done. Testing the keys..."

if ! ./TestGeneratedKeys -n 6 -o test_keyfile_; then
  echo "testKeyGeneration: FAILURE"
  exit -1
fi

rm test_keyfile_*
echo "Generating keys for a 61-replica cluster. This won't be instantaneous..."
./GenerateConcordKeys -f 20 -n 61 -o test_keyfile_
echo "Done. Testing the keys..."

if ! ./TestGeneratedKeys -n 61 -o test_keyfile_; then
  echo "testKeyGeneration: FAILURE"
  exit -1
fi

rm test_keyfile_*
echo "Generating keys for a 67-replica cluster. This won't be instantaneous..."
./GenerateConcordKeys -f 20 -n 67 -o test_keyfile_
echo "Done. Testing the keys..."

if ! ./TestGeneratedKeys -n 67 -o test_keyfile_; then
  echo "testKeyGeneration: FAILURE"
  exit -1
fi

if [ "$run_large_test" = true ]; then
  rm test_keyfile_*
  echo "Generating keys for a 931-replica cluster. This may take a while..."
  ./GenerateConcordKeys -f 300 -n 931 -o test_keyfile_
  echo "Done. Testing the keys..."

  if ! ./TestGeneratedKeys -n 931 -o test_keyfile_; then
    echo "testKeyGeneration: FAILURE"
    exit -1
  fi
fi

echo "testKeyGeneration: SUCCESS: All tests were successful."
exit 0
