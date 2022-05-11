#!/bin/bash

set +x

if [[ -z $1 ]]; then
    echo "Usage: specify the automation script file to be used. Scripts are located in the automation folder."
    exit 1
fi

# Start replicas and payment services
. startServices.sh

# Run each wallet with the automation script
WALLET_PIDS=()
for id in {1..9}
do
    echo "Running automation '$1' with client $id"
    ../UTTClient/utt_client -n config/net_localhost.txt -i $id &> automation/wallet_$id.txt < $1 &
    WALLET_PIDS+=" $!"
done

echo "Waiting wallet pids (${WALLET_PIDS}) to finish the automation script..."
wait ${WALLET_PIDS}
echo "Done."

# Generate summary of state for each client
> automation/summary.txt # Truncate
for id in {1..9}
do
    echo "Summarize client $id"
    ../UTTClient/utt_client -n config/net_localhost.txt -i $id -s &>> automation/summary.txt
done

echo "Automation with summary done."

# Stop replicas and payment services
. stopServices.sh

