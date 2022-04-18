#!/bin/bash
docker-compose up --no-start
docker-compose start replica-1 replica-2 replica-3 replica-4 \
payment-service-1 payment-service-2 payment-service-3