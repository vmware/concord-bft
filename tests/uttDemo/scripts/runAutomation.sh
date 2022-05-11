#!/bin/bash

set +x

if [[ -z $1 ]]; then
    echo "Usage: specify the automation script file to be used. Scripts are located in the automation folder."
    exit 1
fi

# Reset the state
./reset.sh

# Start replicas and payment services (and keep them running in the background)
. startServices.sh

# Run each wallet with the automation script
WALLET_PIDS=()
for id in {1..9}
do
    echo "Running automation with wallet $id"
    ../UTTClient/utt_client -n config/net_localhost.txt -i $id &> automation/wallet_$id.txt < $1 &
    WALLET_PIDS+=" $!"
done

echo "Waiting wallet pids (${WALLET_PIDS}) to finish automation..."
wait ${WALLET_PIDS}
echo "Done."

# Generate summary of state for each client
WALLET_PIDS=()
for id in {1..9}
do
    echo "Summarize wallet $id"
    ../UTTClient/utt_client -n config/net_localhost.txt -i $id -s automation/summary_$id.txt > /dev/null &
    WALLET_PIDS+=" $!"
done

echo "Waiting wallet pids (${WALLET_PIDS}) to finish summarizing..."
wait ${WALLET_PIDS}

echo "Automation with summary done."

# Stop replicas and payment services
. stopServices.sh

