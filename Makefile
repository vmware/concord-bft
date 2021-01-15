CONCORD_BFT_DOCKER_REPO:=concordbft/
CONCORD_BFT_DOCKER_IMAGE:=concord-bft
CONCORD_BFT_DOCKER_IMAGE_VERSION:=0.10
CONCORD_BFT_DOCKER_CONTAINER:=concord-bft

CONCORD_BFT_DOCKERFILE:=Dockerfile
CONCORD_BFT_BUILD_DIR:=build
CONCORD_BFT_TARGET_SOURCE_PATH:=/concord-bft
CONCORD_BFT_CMF_PATHS:=/concord-bft/build/*/cmf
CONCORD_BFT_CONTAINER_SHELL:=/bin/bash
CONCORD_BFT_CONTAINER_CC:=clang
CONCORD_BFT_CONTAINER_CXX:=clang++
CONCORD_BFT_CMAKE_FLAGS:= \
			-DCMAKE_BUILD_TYPE=Debug \
			-DBUILD_TESTING=ON \
			-DBUILD_COMM_TCP_PLAIN=FALSE \
			-DBUILD_COMM_TCP_TLS=TRUE\
			-DCMAKE_CXX_FLAGS_RELEASE=-O3 -g \
			-DUSE_LOG4CPP=TRUE \
			-DBUILD_ROCKSDB_STORAGE=TRUE \
			-DUSE_S3_OBJECT_STORE=TRUE \
			-DUSE_OPENTRACING=ON \
			-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
			-DOMIT_TEST_OUTPUT=OFF \
			-DKEEP_APOLLO_LOGS=TRUE

# The consistency parameter makes sense only at MacOS.
# It is ignored at all other platforms.
CONCORD_BFT_CONTAINER_MOUNT_CONSISTENCY:=,consistency=cached
CONCORD_BFT_CTEST_TIMEOUT:=3000 # Default value is 1500 sec. It takes 2500 to run all the tests at my dev station
CONCORD_BFT_USER_GROUP:=--user `id -u`:`id -g`
CONCORD_BFT_CORE_DIR:=${CONCORD_BFT_TARGET_SOURCE_PATH}/${CONCORD_BFT_BUILD_DIR}/cores

CONCORD_BFT_ADDITIONAL_RUN_PARAMS:=

BASIC_RUN_PARAMS:=-it --init --rm --privileged=true \
					  --memory-swap -1 \
					  --cap-add NET_ADMIN --cap-add=SYS_PTRACE --ulimit core=-1 \
					  --name="${CONCORD_BFT_DOCKER_CONTAINER}" \
					  --workdir=${CONCORD_BFT_TARGET_SOURCE_PATH} \
					  --mount type=bind,source=${CURDIR},target=${CONCORD_BFT_TARGET_SOURCE_PATH}${CONCORD_BFT_CONTAINER_MOUNT_CONSISTENCY} \
					  ${CONCORD_BFT_ADDITIONAL_RUN_PARAMS} \
					  ${CONCORD_BFT_DOCKER_REPO}${CONCORD_BFT_DOCKER_IMAGE}:${CONCORD_BFT_DOCKER_IMAGE_VERSION}

.DEFAULT_GOAL:=build

# MakefileCustom may be useful for overriding the default variables
# or adding custom targets. The include directive is ignored if MakefileCustom file does not exist.
-include MakefileCustom

IF_CONTAINER_RUNS=$(shell docker container inspect -f '{{.State.Running}}' ${CONCORD_BFT_DOCKER_CONTAINER} 2>/dev/null)

.PHONY: help
help: ## The Makefile helps to build Concord-BFT in a docker container
	@cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: pull
pull: ## Pull image from remote
	docker pull ${CONCORD_BFT_DOCKER_REPO}${CONCORD_BFT_DOCKER_IMAGE}:${CONCORD_BFT_DOCKER_IMAGE_VERSION}

.PHONY: login
login: ## Login to the container. Note: if the container is already running, login into existing one
	@if [ "${IF_CONTAINER_RUNS}" != "true" ]; then \
		docker run ${BASIC_RUN_PARAMS} \
			${CONCORD_BFT_CONTAINER_SHELL};exit 0; \
	else \
		docker exec -it ${CONCORD_BFT_DOCKER_CONTAINER} \
			${CONCORD_BFT_CONTAINER_SHELL};exit 0; \
	fi

.PHONY: build
build: ## Build Concord-BFT source. In order to build a specific target run: make TARGET=<target name>.
	docker run ${CONCORD_BFT_USER_GROUP} ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_TARGET_SOURCE_PATH}/${CONCORD_BFT_BUILD_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		CC=${CONCORD_BFT_CONTAINER_CC} CXX=${CONCORD_BFT_CONTAINER_CXX} \
		cmake ${CONCORD_BFT_CMAKE_FLAGS} .. && \
		make format-check && \
		make -j $$(nproc) ${TARGET}"
	@echo
	@echo "Build finished. The binaries are in ${CURDIR}/${CONCORD_BFT_BUILD_DIR}"

.PHONY: list-targets
list-targets: ## Prints the list of available targets
	docker run ${CONCORD_BFT_USER_GROUP} ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_TARGET_SOURCE_PATH}/${CONCORD_BFT_BUILD_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		CC=${CONCORD_BFT_CONTAINER_CC} CXX=${CONCORD_BFT_CONTAINER_CXX} \
		cmake ${CONCORD_BFT_CMAKE_FLAGS} .. && \
		make help"

.PHONY: format
format: ## Format Concord-BFT source with clang-format
	docker run ${CONCORD_BFT_USER_GROUP} ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_TARGET_SOURCE_PATH}/${CONCORD_BFT_BUILD_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		CC=${CONCORD_BFT_CONTAINER_CC} CXX=${CONCORD_BFT_CONTAINER_CXX} \
		cmake ${CONCORD_BFT_CMAKE_FLAGS} .. && \
		make format"
	@echo
	@echo "Format finished."

.PHONY: tidy-check
tidy-check: ## Run clang-tidy
	docker run ${CONCORD_BFT_USER_GROUP} ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_TARGET_SOURCE_PATH}/${CONCORD_BFT_BUILD_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		CC=${CONCORD_BFT_CONTAINER_CC} CXX=${CONCORD_BFT_CONTAINER_CXX} \
		cmake ${CONCORD_BFT_CMAKE_FLAGS} .. && \
		make -C ${CONCORD_BFT_CMF_PATHS} &> /dev/null && \
		run-clang-tidy-10 &> clang-tidy-report.txt && \
		../scripts/check-forbidden-usage.sh .." \
		&& (echo "\nClang-tidy finished successfully.") \
		|| ( echo "\nClang-tidy failed. The report is in ${CURDIR}/${CONCORD_BFT_BUILD_DIR}/clang-tidy-report.txt. \
			 \nFor detail information about the checks, please refer to https://clang.llvm.org/extra/clang-tidy/checks/list.html" \
			 ; exit 1)

.PHONY: test
test: ## Run all tests
	docker run ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_CORE_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		ctest --timeout ${CONCORD_BFT_CTEST_TIMEOUT} --output-on-failure"

.PHONY: list-tests
list-tests: ## List all tests. This one is helpful to choose which test to run when calling `make single-test TEST_NAME=<test name>`
	docker run  ${CONCORD_BFT_USER_GROUP} ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_BUILD_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		CC=${CONCORD_BFT_CONTAINER_CC} CXX=${CONCORD_BFT_CONTAINER_CXX} \
		cmake ${CONCORD_BFT_CMAKE_FLAGS} .. && \
		ctest -N"

.PHONY: single-test
single-test: ## Run a single test `make single-test TEST_NAME=<test name>`
	docker run ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_CORE_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		ctest -V -R ${TEST_NAME} --timeout ${CONCORD_BFT_CTEST_TIMEOUT} --output-on-failure"

.PHONY: clean
clean: ## Clean Concord-BFT build directory
	docker run ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"rm -rf ${CONCORD_BFT_BUILD_DIR}"

.PHONY: build-docker-image
build-docker-image: ## Build the image. Note: without caching
	docker build --rm --no-cache=true -t ${CONCORD_BFT_DOCKER_IMAGE}:latest \
		-f ./${CONCORD_BFT_DOCKERFILE} .
	@echo
	@echo "Build finished. Docker image name: \"${CONCORD_BFT_DOCKER_IMAGE}:latest\"."
	@echo "Before you push it to Docker Hub, please tag it(CONCORD_BFT_DOCKER_IMAGE_VERSION + 1)."
	@echo "If you want the image to be the default, please update the following variables:"
	@echo "${CURDIR}/Makefile: CONCORD_BFT_DOCKER_IMAGE_VERSION"
