#!/bin/bash

if [[ -z $1 ]]; then
    echo "Usage: specify the automation commands to be used."
    exit 1
fi

# Reset the state
./reset.sh

# Start replicas and payment services (and keep them running in the background)
. startServices.sh

mkdir -p automation

function printState() {
    echo ""
    printf "%-15s%-15s%-15s%-15s%-15s\n" WalletId LastBlockId PublicBalance UttBalance UttBudget
    echo "---------------------------------------------------------------------------"
    awk '{printf "%-15s%-15s%-15s%-15s%-15s\n", $1,$2,$3,$4,$5}' $1
    echo ""
}

function waitWallets() {
    echo "Waiting wallet pids (${WALLET_PIDS}) to finish..."
    for pid in ${WALLET_PIDS}; do
        wait $pid
        if [ $? -ne 0 ]; then
            echo "Error: wallet with pid ${pid} failed to execute correctly!"
            exit 1
        fi
    done
    echo "Done."
}

# Summarize the initial state of each wallet
echo ""
echo "Gather the initial state of the system..."
WALLET_PIDS=()
for id in {1..9}
do
    ../UTTClient/utt_client -n config/net_localhost.txt -i $id -s automation/init_$id.txt > /dev/null &
    WALLET_PIDS+=" $!"
done
waitWallets
printState "automation/init_*"

# Export variables describing the initial state
NUM_WALLETS=9
INIT_BLOCK_ID=$(awk 'BEGIN {value=0} {if(NR==1) value=$2; else if(value != $2) value="error: inconsistent initial value"} END {print value}' automation/init_*)
INIT_PUBLIC_BALANCE=$(awk 'BEGIN {value=0} {if(NR==1) value=$3; else if(value != $3) value="error: inconsistent initial value"} END {print value}' automation/init_*)
INIT_UTT_BALANCE=$(awk 'BEGIN {value=0} {if(NR==1) value=$4; else if(value != $4) value="error: inconsistent initial value"} END {print value}' automation/init_*)
INIT_UTT_BUDGET=$(awk 'BEGIN {value=0} {if(NR==1) value=$5; else if(value != $5) value="error: inconsistent initial value"} END {print value}' automation/init_*)

# Run automation on each wallet
echo ""
echo "Run automation..."
WALLET_PIDS=()
for id in {1..9}
do
    echo -e $1 | ../UTTClient/utt_client -n config/net_localhost.txt -i $id &> automation/run_$id.txt &
    WALLET_PIDS+=" $!"
done
waitWallets

# Summarize the final state of each wallet
echo ""
echo "Gather the final state of the system..."
WALLET_PIDS=()
for id in {1..9}
do
    ../UTTClient/utt_client -n config/net_localhost.txt -i $id -s automation/final_$id.txt > /dev/null &
    WALLET_PIDS+=" $!"
done
waitWallets
printState "automation/final_*"

# Stop replicas and payment services
. stopServices.sh

wait # Wait all services to terminate
echo ""
echo "Automation completed."

