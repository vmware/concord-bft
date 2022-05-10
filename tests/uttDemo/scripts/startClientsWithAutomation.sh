#!/bin/bash

# Run the automation script
for id in {1..9}
do
    echo "Running automation with client $id"
    ../UTTClient/utt_client -n config/net_localhost.txt -i $id &> automation/wallet_$id.txt < automation/input.txt &
done

echo "Waiting clients to finish the automation script..."
wait
echo "Done."

# Print summary of state for each client
for id in {1..9}
do
    echo "Summarize client $id"
    ../UTTClient/utt_client -n config/net_localhost.txt -i $id -s &>> automation/summary.txt
done

echo "Automation with summary done."

