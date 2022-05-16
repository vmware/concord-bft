#!/bin/bash

echo ""
echo "###########################################################"
echo "# This test concurrently runs deposits of \$1 for each"
echo "# wallet. At the end we check that all deposits have been"
echo "# made and that each wallet has the expected amount of"
echo "# additional public money added to its balance."
echo "###########################################################"

# Each wallet executes 150 additions of $1 to its balance (defined by the checkpoint command)
. runAutomation.sh "checkpoint\nq"

# We expect all deposits to succeed and that each wallet adds the expected
# value to its public balance
EXPECTED_BLOCK_ID=$((INIT_BLOCK_ID + NUM_WALLETS * 150))
EXPECTED_PUBLIC_BALANCE=$((INIT_PUBLIC_BALANCE + 150)) 

echo "Expected block id: ${EXPECTED_BLOCK_ID}"
echo "Expected public balance per wallet: ${EXPECTED_PUBLIC_BALANCE}"

# Validate
awk -v x=${EXPECTED_BLOCK_ID} 'BEGIN {err=0} $2 != x {print "Error: unexpected block id!",$0; err=1} END {if (!err) print "All block ids are Ok."}' automation/final_*
awk -v x=${EXPECTED_PUBLIC_BALANCE} 'BEGIN {err=0} $3 != x {print "Error: unexpected public balance!",$0; err=1} END {if (!err) print "All public balances are Ok."}' automation/final_*