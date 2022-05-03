.DEFAULT_GOAL:=help
UTT_DEMO_BIN_DIR=bin
UTT_DEMO_RUN_LOGS_DIR=run/logs
UTT_DEMO_RUN_CONFIG_DIR=run/config
UTT_DEMO_RUN_ROCKSDB_DIR=run/rocksdb

.PHONY: help
help: ## The Makefile helps to build and run the UTT demo.
	@cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

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

.PHONY: build-docker-wallet
build-docker-wallet: ## Builds only the wallet docker image.
	@docker build -f ../tests/uttDemo/UTTClient/Dockerfile . -t utt-demo-wallet

.PHONY: build-docker-payment-service
build-docker-payment-service: ## Builds only the payment service docker image.
	@docker build -f ../tests/uttDemo/PaymentService/Dockerfile . -t utt-demo-payment-service

.PHONY: build-docker-replica
build-docker-replica: ## Builds only the replica docker image.
	@docker build -f ../tests/uttDemo/UTTReplica/Dockerfile . -t utt-demo-replica

.PHONY: demo-start
demo-start: ## Starts the demo by running all required services.
	@docker-compose up --no-start
	@docker-compose start replica-1 replica-2 replica-3 replica-4 payment-service-1 payment-service-2 payment-service-3
	
	@echo
	@echo "All services are running. You can now start using a wallet with 'demo-use-wallet-<1-9>'"

.PHONY: demo-use-wallet
demo-use-wallet: ## Starts the selected wallet with ID from 1 to 9. Type 'demo-use-wallet-<ID>'. 
	@docker-compose up --no-start
	@docker-compose run wallet-${WALLET_ID}

.PHONY: demo-stop
demo-stop: ## Stops all running demo services
	@docker-compose stop

	@echo
	@echo "All services are stopped. Use 'demo-start' to resume from where you left."

.PHONY: demo-reset
demo-reset: ## Stops the demo, resets all service containers and deletes the demo state and logs.
	@docker-compose down
	@sudo rm -rf ./run/logs/* ./run/rocksdb/*
	
	@echo
	@echo "You have reset the demo. Use 'demo-start' to begin from scratch."

.PHONY: demo-use-wallet-1
demo-use-wallet-1: 
	$(MAKE) demo-use-wallet WALLET_ID=1

.PHONY: demo-use-wallet-2
demo-use-wallet-2: 
	$(MAKE) demo-use-wallet WALLET_ID=2

.PHONY: demo-use-wallet-3
demo-use-wallet-3: 
	$(MAKE) demo-use-wallet WALLET_ID=3

.PHONY: demo-use-wallet-4
demo-use-wallet-4: 
	$(MAKE) demo-use-wallet WALLET_ID=4

.PHONY: demo-use-wallet-5
demo-use-wallet-5: 
	$(MAKE) demo-use-wallet WALLET_ID=5

.PHONY: demo-use-wallet-6
demo-use-wallet-6: 
	$(MAKE) demo-use-wallet WALLET_ID=6

.PHONY: demo-use-wallet-7
demo-use-wallet-7: 
	$(MAKE) demo-use-wallet WALLET_ID=7

.PHONY: demo-use-wallet-8
demo-use-wallet-8: 
	$(MAKE) demo-use-wallet WALLET_ID=8

.PHONY: demo-use-wallet-9
demo-use-wallet-9: 
	$(MAKE) demo-use-wallet WALLET_ID=9