#!/bin/bash

./runAutomation.sh automation/randomTransactions.txt

declare -i NUM_WALLETS=9
declare -i INIT_PUBLIC_BALANCE=1000
declare -i INIT_UTT_BALANCE=200
EXPECTED_TOTAL_PUBLIC_BALANCE=$((NUM_WALLETS * INIT_PUBLIC_BALANCE))
EXPECTED_TOTAL_UTT_BALANCE=$((NUM_WALLETS * INIT_UTT_BALANCE))

echo "Expected total public balance: ${EXPECTED_TOTAL_PUBLIC_BALANCE}"
echo "Expected total utt balance: ${EXPECTED_TOTAL_UTT_BALANCE}"

# Validate
awk -v x=${EXPECTED_TOTAL_PUBLIC_BALANCE} '{ sum += $3 } END {if(sum != x) print "Error: unexpected total public balance!",sum; else print "Total public balance Ok."}' automation/summary_*
awk -v x=${EXPECTED_TOTAL_UTT_BALANCE} '{ sum += $4 } END {if(sum != x) print "Error: unexpected total UTT balance!",sum; else print "Total UTT balance Ok."}' automation/summary_*