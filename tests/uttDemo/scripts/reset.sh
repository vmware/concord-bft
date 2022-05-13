#!/bin/bash
echo ""
echo "Killing all services..."
killall utt_replica
killall payment_service

echo ""
echo "Cleaning files..."
rm -rf logs/* core.* rocksdb/* automation/*
echo "Done."

