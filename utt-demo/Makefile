.DEFAULT_GOAL:=help
UTT_DEMO_BIN_DIR=bin
UTT_DEMO_RUN_CONFIG_DIR=config
UTT_DEMO_RUN_LOGS_DIR=run/logs
UTT_DEMO_RUN_ROCKSDB_DIR=run/rocksdb

.PHONY: help
help: ## The Makefile helps to build and run the UTT demo.
	@cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: pull
pull: ## Pull the latest demo images
	@docker-compose pull

.PHONY: build
build: ## Builds the UTT demo binaries and prepares the required directories.
	@make -C .. format
	@make -C .. TARGET="utt_replica utt_client payment_service copy_utt_demo_scripts gen_utt_cfg"

	@mkdir -p ${UTT_DEMO_BIN_DIR}
	@mkdir -p ${UTT_DEMO_RUN_LOGS_DIR}
	@mkdir -p ${UTT_DEMO_RUN_CONFIG_DIR}
	@mkdir -p ${UTT_DEMO_RUN_ROCKSDB_DIR}

	@cp ../build/tests/uttDemo/UTTClient/utt_client ${UTT_DEMO_BIN_DIR}
	@cp ../build/tests/uttDemo/UTTReplica/utt_replica ${UTT_DEMO_BIN_DIR}
	@cp ../build/tests/uttDemo/PaymentService/payment_service ${UTT_DEMO_BIN_DIR}

	@cp ../build/tests/uttDemo/scripts/config/* ${UTT_DEMO_RUN_CONFIG_DIR}

.PHONY: build-docker-all
build-docker-all: build-docker-wallet build-docker-payment-service build-docker-replica ## Builds all required images for the wallet, payment service and replica.

.PHONY: build-docker-runenv
build-docker-runenv: ## Builds the environment image for running the demo
	@docker build --rm --no-cache=true -f Dockerfile.runenv . -t concordbft/utt-demo-runenv
	@echo
	@echo "Build finished. Docker image name: \"concordbft/utt-demo-runenv:latest\"."
	@echo "Before you push it to Docker Hub, please tag it to match the respective build environment tag."

.PHONY: build-docker-wallet
build-docker-wallet: ## Builds only the wallet docker image.
	@docker build -f ../tests/uttDemo/UTTClient/Dockerfile . -t concordbft/utt-demo-wallet

.PHONY: build-docker-payment-service
build-docker-payment-service: ## Builds only the payment service docker image.
	@docker build -f ../tests/uttDemo/PaymentService/Dockerfile . -t concordbft/utt-demo-payment-service

.PHONY: build-docker-replica
build-docker-replica: ## Builds only the replica docker image.
	@docker build -f ../tests/uttDemo/UTTReplica/Dockerfile . -t concordbft/utt-demo-replica

.PHONY: start
start: ## Starts the demo by running all required services.
	@docker-compose up --no-start
	@docker-compose start replica-1 replica-2 replica-3 replica-4 payment-service-1 payment-service-2 payment-service-3
	
	@echo
	@echo "All services are running. You can now start using a wallet with 'make wallet-<1-9>'"

.PHONY: wallet
wallet: ## Starts the selected wallet with ID from 1 to 9. Type 'wallet-<ID>'. 
	@docker-compose up --no-start
	@docker-compose run wallet-${WALLET_ID}

.PHONY: stop
stop: ## Stops all running demo services
	@docker-compose stop

	@echo
	@echo "All services are stopped. Use 'make start' to resume from where you left."

.PHONY: reset
reset: ## Stops the demo, resets all service containers and deletes the demo state and logs.
	@docker-compose down
	@sudo rm -rf ./run/logs/* ./run/rocksdb/*
	
	@echo
	@echo "You have reset the demo. Use 'make start' to begin from scratch."

.PHONY: wallet-1
wallet-1: 
	$(MAKE) wallet WALLET_ID=1

.PHONY: wallet-2
wallet-2: 
	$(MAKE) wallet WALLET_ID=2

.PHONY: wallet-3
wallet-3: 
	$(MAKE) wallet WALLET_ID=3

.PHONY: wallet-4
wallet-4: 
	$(MAKE) wallet WALLET_ID=4

.PHONY: wallet-5
wallet-5: 
	$(MAKE) wallet WALLET_ID=5

.PHONY: wallet-6
wallet-6: 
	$(MAKE) wallet WALLET_ID=6

.PHONY: wallet-7
wallet-7: 
	$(MAKE) wallet WALLET_ID=7

.PHONY: wallet-8
wallet-8: 
	$(MAKE) wallet WALLET_ID=8

.PHONY: wallet-9
wallet-9: 
	$(MAKE) wallet WALLET_ID=9