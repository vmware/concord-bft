#!/bin/bash
echo "Killing all services..."
killall utt_replica
killall payment_service

echo "Finished!"