#!/bin/bash
docker-compose up --no-start
docker-compose run wallet-$1
