#!/bin/bash

echo ""
echo "###########################################################"
echo "# This test concurrently runs randomized public and utt"
echo "# transafers between random wallets. At the end we check"
echo "# that the total public and utt balance remain the same."
echo "###########################################################"

# Each wallet executes random transfers and quits
. runAutomation.sh "random 50\nq"

# Total balances must be conserved
EXPECTED_TOTAL_PUBLIC_BALANCE=$((NUM_WALLETS * INIT_PUBLIC_BALANCE))
EXPECTED_TOTAL_UTT_BALANCE=$((NUM_WALLETS * INIT_UTT_BALANCE))

echo "Expected total public balance: ${EXPECTED_TOTAL_PUBLIC_BALANCE}"
echo "Expected total utt balance: ${EXPECTED_TOTAL_UTT_BALANCE}"

# Validate
awk -v x=${EXPECTED_TOTAL_PUBLIC_BALANCE} '{ sum += $3 } END {if(sum != x) print "Error: unexpected total public balance!",sum; else print "Total public balance Ok."}' automation/final_*
awk -v x=${EXPECTED_TOTAL_UTT_BALANCE} '{ sum += $4 } END {if(sum != x) print "Error: unexpected total UTT balance!",sum; else print "Total UTT balance Ok."}' automation/final_*