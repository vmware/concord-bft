#!/bin/bash

./runAutomation.sh automation/walletConcurrency.txt

EXPECTED_BLOCK_ID=1350
EXPECTED_PUBLIC_BALANCE=1150

echo "Expected block id: ${EXPECTED_BLOCK_ID}"
echo "Expected public balance per wallet: ${EXPECTED_PUBLIC_BALANCE}"

# Validate
awk -v x=${EXPECTED_BLOCK_ID} '$2 != x {print "Error: unexpected block id!",$0}' automation/summary.txt
awk -v x=${EXPECTED_PUBLIC_BALANCE} '$3 != x {print "Error: unexpected public balance!",$0}' automation/summary.txt