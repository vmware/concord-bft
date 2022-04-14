#!/bin/bash
echo "Running client!"
gdb --args ../UTTClient/utt_client -n config/net_localhost.txt -i 1

echo "Finished!"

