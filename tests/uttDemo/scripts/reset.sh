#!/bin/bash
echo "Killing all services..."
killall utt_replica
killall payment_service
echo "Done."

echo "Cleaning files..."
rm -rf logs/* core.* rocksdb/* automation/wallet_* automation/summary_*
echo "Done."

