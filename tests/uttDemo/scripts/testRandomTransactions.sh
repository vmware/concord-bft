#!/bin/bash

echo ""
echo "###############################################################"
echo "# This test concurrently runs randomized public and utt"
echo "# transafers between random wallets. At the end we check"
echo "# that the total public and utt balance remain the same."
echo "# You can obtain the random seeds from automation/run_ files."
echo "###############################################################"

# Each wallet executes random transfers and quits
. runAutomation.sh "random 100\nq"

# Total balances must be conserved
EXPECTED_TOTAL_BALANCE=$((NUM_WALLETS * (INIT_PUBLIC_BALANCE + INIT_UTT_BALANCE)))

echo "Expected total balance: ${EXPECTED_TOTAL_BALANCE}"

# Validate
awk -v x=${EXPECTED_TOTAL_BALANCE} '{ sum += $3 + $4 } END {if(sum != x) print "Error: unexpected total balance!",sum; else print "Total public balance Ok."}' automation/final_*