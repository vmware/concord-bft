#!/bin/bash
echo "Running client $1!"
../UTTClient/utt_client -n config/net_localhost.txt -i $1

echo "Finished!"

