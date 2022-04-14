#!/bin/bash
make format
make TARGET="utt_replica utt_client payment_service copy_utt_demo_scripts gen_utt_cfg"

mkdir -p utt-demo-run/bin
mkdir -p utt-demo-run/logs
mkdir -p utt-demo-run/rocksdb

cp build/tests/uttDemo/UTTClient/utt_client utt-demo-run/bin
cp build/tests/uttDemo/UTTReplica/utt_replica utt-demo-run/bin
cp build/tests/uttDemo/PaymentService/payment_service utt-demo-run/bin