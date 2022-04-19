#!/bin/bash
echo "Running client $1!"
gdb --args ../UTTClient/utt_client -n config/net_localhost.txt -i $1

echo "Finished!"

