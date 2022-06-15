CONCORD_BFT_DOCKER_REPO?=concordbft/
CONCORD_BFT_DOCKER_IMAGE?=concord-bft
CONCORD_BFT_DOCKER_IMAGE_VERSION?=0.42
CONCORD_BFT_DOCKER_CONTAINER?=concord-bft

CONCORD_BFT_DOCKERFILE?=Dockerfile
CONCORD_BFT_BUILD_DIR?=build
CONCORD_BFT_TARGET_SOURCE_PATH?=/concord-bft
CONCORD_BFT_KVBC_CMF_PATHS?=${CONCORD_BFT_TARGET_SOURCE_PATH}/build/kvbc/cmf
CONCORD_BFT_RECONFIGURATION_CMF_PATHS?=${CONCORD_BFT_TARGET_SOURCE_PATH}/build/reconfiguration/cmf
CONCORD_BFT_BFTENGINE_CMF_PATHS?=${CONCORD_BFT_TARGET_SOURCE_PATH}/build/bftengine/cmf
CONCORD_BFT_CCRON_CMF_PATHS?=${CONCORD_BFT_TARGET_SOURCE_PATH}/build/ccron/cmf
CONCORD_BFT_SKVBC_CMF_PATHS?=${CONCORD_BFT_TARGET_SOURCE_PATH}/build/tests/simpleKVBC/cmf
CONCORD_BFT_CLIENT_PROTO_PATH?=${CONCORD_BFT_TARGET_SOURCE_PATH}/build/client/proto
CONCORD_BFT_THIN_REPLICA_PROTO_PATH?=${CONCORD_BFT_TARGET_SOURCE_PATH}/build/thin-replica-server/proto
CONCORD_BFT_KVBC_PROTO_PATH?=${CONCORD_BFT_TARGET_SOURCE_PATH}/build/kvbc/proto
CONCORD_BFT_CONTAINER_SHELL?=/bin/bash
CONCORD_BFT_CONTAINER_CC?=clang
CONCORD_BFT_CONTAINER_CXX?=clang++
CONCORD_BFT_CMAKE_BUILD_TYPE?=Release
CONCORD_BFT_CMAKE_BUILD_TESTING?=TRUE
CONCORD_BFT_CLANG_TIDY?=${CONCORD_BFT_TARGET_SOURCE_PATH}/tools/run-clang-tidy.py
CONCORD_BFT_RUN_SIMPLE_TEST?=./build/tests/simpleTest/scripts/testReplicasAndClient.sh

# UDP | TLS | TCP
CONCORD_BFT_CMAKE_TRANSPORT?=TLS
ifeq (${CONCORD_BFT_CMAKE_TRANSPORT},TLS)
   TLS_ENABLED__:=TRUE
   TCP_ENABLED__:=FALSE
else ifeq (${CONCORD_BFT_CMAKE_TRANSPORT},TCP)
   TLS_ENABLED__:=FALSE
   TCP_ENABLED__:=TRUE
else
   TLS_ENABLED__:=FALSE
   TCP_ENABLED__:=FALSE
endif

CONCORD_BFT_CMAKE_CXX_FLAGS_RELEASE?='-O3 -g'
CONCORD_BFT_CMAKE_USE_LOG4CPP?=TRUE
CONCORD_BFT_CMAKE_BUILD_ROCKSDB_STORAGE?=TRUE
CONCORD_BFT_CMAKE_USE_S3_OBJECT_STORE?=TRUE
CONCORD_BFT_CMAKE_USE_OPENTRACING?=TRUE
CONCORD_BFT_CMAKE_USE_PROMETHEUS?=TRUE
CONCORD_BFT_CMAKE_USE_JAEGER?=TRUE
CONCORD_BFT_CMAKE_USE_JSON?=TRUE
CONCORD_BFT_CMAKE_USE_HTTPLIB?=TRUE
CONCORD_BFT_CMAKE_EXPORT_COMPILE_COMMANDS?=TRUE
CONCORD_BFT_CMAKE_OMIT_TEST_OUTPUT?=FALSE
CONCORD_BFT_CMAKE_KEEP_APOLLO_LOGS?=TRUE
CONCORD_BFT_CMAKE_RUN_APOLLO_TESTS?=TRUE
CONCORD_BFT_CMAKE_TRANSACTION_SIGNING_ENABLED?=TRUE
CONCORD_BFT_CMAKE_BUILD_SLOWDOWN?=FALSE
# Only useful with CONCORD_BFT_CMAKE_BUILD_TYPE:=Release
CONCORD_BFT_CMAKE_BUILD_KVBC_BENCH?=TRUE
# Only usefull with CONCORD_BFT_CMAKE_CXX_FLAGS_RELEASE=-O0 -g
CONCORD_BFT_CMAKE_ASAN?=FALSE
CONCORD_BFT_CMAKE_TSAN?=FALSE
CONCORD_BFT_CMAKE_CODECOVERAGE?=FALSE
CONCORD_BFT_CMAKE_USE_FAKE_CLOCK_IN_TIME_SERVICE?=FALSE
ENABLE_RESTART_RECOVERY_TESTS?=FALSE
ifeq (${CONCORD_BFT_CMAKE_ASAN},TRUE)
	CONCORD_BFT_CMAKE_CXX_FLAGS_RELEASE='-O0 -g'
else ifeq (${CONCORD_BFT_CMAKE_TSAN},TRUE)
	CONCORD_BFT_CMAKE_CXX_FLAGS_RELEASE='-O0 -g'
endif
ifeq (${CONCORD_BFT_CMAKE_CODECOVERAGE},TRUE)
	CONCORD_BFT_CMAKE_CXX_FLAGS_RELEASE='-O0 -g'
endif

CONCORD_BFT_CMAKE_FLAGS?= \
			-DCMAKE_BUILD_TYPE=${CONCORD_BFT_CMAKE_BUILD_TYPE} \
			-DBUILD_TESTING=${CONCORD_BFT_CMAKE_BUILD_TESTING} \
			-DBUILD_COMM_TCP_PLAIN=${TCP_ENABLED__} \
			-DBUILD_COMM_TCP_TLS=${TLS_ENABLED__} \
			-DCMAKE_CXX_FLAGS_RELEASE=${CONCORD_BFT_CMAKE_CXX_FLAGS_RELEASE} \
			-DUSE_LOG4CPP=${CONCORD_BFT_CMAKE_USE_LOG4CPP} \
			-DBUILD_ROCKSDB_STORAGE=${CONCORD_BFT_CMAKE_BUILD_ROCKSDB_STORAGE} \
			-DUSE_S3_OBJECT_STORE=${CONCORD_BFT_CMAKE_USE_S3_OBJECT_STORE} \
			-DUSE_OPENTRACING=${CONCORD_BFT_CMAKE_USE_OPENTRACING} \
			-DUSE_PROMETHEUS=${CONCORD_BFT_CMAKE_USE_PROMETHEUS} \
			-DUSE_JAEGER=${CONCORD_BFT_CMAKE_USE_JAEGER} \
			-DUSE_JSON=${CONCORD_BFT_CMAKE_USE_JSON} \
			-DUSE_HTTPLIB=${CONCORD_BFT_CMAKE_USE_HTTPLIB} \
			-DCMAKE_EXPORT_COMPILE_COMMANDS=${CONCORD_BFT_CMAKE_EXPORT_COMPILE_COMMANDS} \
			-DOMIT_TEST_OUTPUT=${CONCORD_BFT_CMAKE_OMIT_TEST_OUTPUT} \
			-DKEEP_APOLLO_LOGS=${CONCORD_BFT_CMAKE_KEEP_APOLLO_LOGS} \
			-DRUN_APOLLO_TESTS=${CONCORD_BFT_CMAKE_RUN_APOLLO_TESTS} \
			-DBUILD_SLOWDOWN=${CONCORD_BFT_CMAKE_BUILD_SLOWDOWN} \
			-DUSE_FAKE_CLOCK_IN_TIME_SERVICE=${CONCORD_BFT_CMAKE_USE_FAKE_CLOCK_IN_TIME_SERVICE} \
			-DLEAKCHECK=${CONCORD_BFT_CMAKE_ASAN} \
			-DTHREADCHECK=${CONCORD_BFT_CMAKE_TSAN} \
			-DCODECOVERAGE=${CONCORD_BFT_CMAKE_CODECOVERAGE} \
			-DTXN_SIGNING_ENABLED=${CONCORD_BFT_CMAKE_TRANSACTION_SIGNING_ENABLED} \
			-DENABLE_RESTART_RECOVERY_TESTS=${ENABLE_RESTART_RECOVERY_TESTS}


# The consistency parameter makes sense only at MacOS.
# It is ignored at all other platforms.
CONCORD_BFT_CONTAINER_MOUNT_CONSISTENCY?=,consistency=cached
CONCORD_BFT_CTEST_TIMEOUT?=3000 # Default value is 1500 sec. It takes 2500 to run all the tests at my dev station
CONCORD_BFT_USER_GROUP?=--user `id -u`:`id -g`
CONCORD_BFT_CORE_DIR?=${CONCORD_BFT_TARGET_SOURCE_PATH}/${CONCORD_BFT_BUILD_DIR}/cores

CONCORD_BFT_ADDITIONAL_RUN_PARAMS?=
APOLLO_CTEST_RUN_PARAMS?=
CONCORD_BFT_FORMAT_CMD=make format-check &&

ifneq (${APOLLO_LOG_STDOUT},)
	CONCORD_BFT_ADDITIONAL_RUN_PARAMS+=--env APOLLO_LOG_STDOUT=TRUE
	CONCORD_BFT_ADDITIONAL_CTEST_RUN_PARAMS+=-V
endif

ifneq (${SKIP_FORMAT},)
	CONCORD_BFT_FORMAT_CMD=
endif

BASIC_RUN_PARAMS?=-it --init --rm --privileged=true \
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
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%s:\033[0m \n%s\n", $$1, $$2}'

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

.PHONY: gen_cmake
gen_cmake: ## Generate cmake files, used internally
	docker run ${CONCORD_BFT_USER_GROUP} ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_TARGET_SOURCE_PATH}/${CONCORD_BFT_BUILD_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		CC=${CONCORD_BFT_CONTAINER_CC} CXX=${CONCORD_BFT_CONTAINER_CXX} \
		cmake ${CONCORD_BFT_CMAKE_FLAGS} .."
	@echo
	@echo "CMake finished."

.PHONY: build
build: gen_cmake ## Build Concord-BFT source. In order to build a specific target run: make TARGET=<target name>.
	docker run ${CONCORD_BFT_USER_GROUP} ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"cd ${CONCORD_BFT_BUILD_DIR} && \
		${CONCORD_BFT_FORMAT_CMD} \
		make -j $$(nproc) ${TARGET}"
	@echo
	@echo "Build finished. The binaries are in ${CURDIR}/${CONCORD_BFT_BUILD_DIR}"

.PHONY: list-targets
list-targets: gen_cmake ## Prints the list of available targets
	docker run ${CONCORD_BFT_USER_GROUP} ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"cd ${CONCORD_BFT_BUILD_DIR} && \
		make help"

.PHONY: format
format: gen_cmake ## Format Concord-BFT source with clang-format
	docker run ${CONCORD_BFT_USER_GROUP} ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"cd ${CONCORD_BFT_BUILD_DIR} && \
		make format"
	@echo
	@echo "Format finished."

.PHONY: tidy-check
tidy-check: gen_cmake ## Run clang-tidy
	docker run ${CONCORD_BFT_USER_GROUP} ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"set -eo pipefail && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		make -C ${CONCORD_BFT_KVBC_CMF_PATHS} &> /dev/null && \
		make -C ${CONCORD_BFT_RECONFIGURATION_CMF_PATHS} &> /dev/null && \
		make -C ${CONCORD_BFT_BFTENGINE_CMF_PATHS} &> /dev/null && \
		make -C ${CONCORD_BFT_CCRON_CMF_PATHS} &> /dev/null && \
		make -C ${CONCORD_BFT_SKVBC_CMF_PATHS} &> /dev/null && \
		make -C ${CONCORD_BFT_CLIENT_PROTO_PATH} &> /dev/null && \
		make -C ${CONCORD_BFT_THIN_REPLICA_PROTO_PATH} &> /dev/null && \
		make -C ${CONCORD_BFT_KVBC_PROTO_PATH} &> /dev/null && \
		${CONCORD_BFT_CLANG_TIDY} -ignore ../.clang-tidy-ignore 2>&1 | tee clang-tidy-report.txt && \
		cat clang-tidy-report.txt | ( ! grep 'error:\|note:' ) && \
		../scripts/check-forbidden-usage.sh .." \
		&& (echo "\nClang-tidy finished successfully.") \
		|| ( echo "\nClang-tidy failed. The full report is in ${CURDIR}/${CONCORD_BFT_BUILD_DIR}/clang-tidy-report.txt. \
			 \nFor detail information about the checks, please refer to https://clang.llvm.org/extra/clang-tidy/checks/list.html" \
			 ; exit 1)

.PHONY: list-tests
list-tests: gen_cmake ## List all tests. This one is helpful to choose which test to run when calling `make single-test TEST_NAME=<test name>`
	docker run  ${CONCORD_BFT_USER_GROUP} ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"cd ${CONCORD_BFT_BUILD_DIR} && \
		ctest -N"

# Test targets
NUM_REPEATS?=1
ifneq (${NUM_REPEATS},)
	NUM_REPEATS__:=${NUM_REPEATS}
endif
BREAK_ON_FAILURE__:=FALSE
ifneq (${BREAK_ON_FAILURE},)
	BREAK_ON_FAILURE__:=${BREAK_ON_FAILURE}
endif

.PHONY: test
test: ## Run all tests
	docker run ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_CORE_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		ctest ${CONCORD_BFT_ADDITIONAL_CTEST_RUN_PARAMS} --timeout ${CONCORD_BFT_CTEST_TIMEOUT} --output-on-failure"

.PHONY: simple-test
simple-test: ## Run Simple Test
	docker run ${BASIC_RUN_PARAMS} \
	        ${CONCORD_BFT_CONTAINER_SHELL} -c \
	        "timeout 300 ${CONCORD_BFT_RUN_SIMPLE_TEST}"

.PHONY: test-range
test-range: ## Run all tests in the range [START,END], inclusive: `make test-range START=<#start_test> END=<#end_test>`. To get test numbers, use list-tests.
	docker run ${BASIC_RUN_PARAMS} \
			${CONCORD_BFT_CONTAINER_SHELL} -c \
			"mkdir -p ${CONCORD_BFT_CORE_DIR} && \
			cd ${CONCORD_BFT_BUILD_DIR} && \
			ctest ${CONCORD_BFT_ADDITIONAL_CTEST_RUN_PARAMS} -I ${START},${END}"

.PHONY: test-single-suite
test-single-suite: ## Run a single test `make test-single-suite TEST_NAME=<test name>`
	docker run ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_CORE_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		ctest ${CONCORD_BFT_ADDITIONAL_CTEST_RUN_PARAMS} -V -R ${TEST_NAME} --timeout ${CONCORD_BFT_CTEST_TIMEOUT} --output-on-failure"

.PHONY: test-single-apollo-case
test-single-apollo-case: ## Run a single Apollo test case: `make test-single-apollo-case TEST_FILE_NAME=<test file name> TEST_CASE_NAME=<test case name> NUM_REPEATS=<number of repeats,default=1,optional> BREAK_ON_FAILURE=<TRUE|FALSE,optional>`. Test suite file name should come without *.py. Test case is expected without a class name, and must be unique. Example: `make test-single-apollo-case BREAK_ON_FAILURE=TRUE NUM_REPEATS=100 TEST_FILE_NAME=test_skvbc_reconfiguration TEST_CASE_NAME=test_tls_exchange_client_replica_with_st`
	@if [ -z ${TEST_FILE_NAME} ]; then echo "Error: TEST_FILE_NAME is mandatory"; exit 1; fi
	@if [ -z ${TEST_CASE_NAME} ]; then echo "Error: TEST_CASE_NAME is mandatory"; exit 1; fi
	$(eval PREFIX := $(shell docker run ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_CORE_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		ctest -VV -N | grep -m 1 '${TEST_FILE_NAME}' | grep -o 'env.*' | sed 's/\unittest.*/unittest/'"))
	$(eval POSTFIX := $(shell docker run ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"python3 scripts/apollo_list_tests.py \
		${CONCORD_BFT_TARGET_SOURCE_PATH}/tests/apollo/ f | grep ${TEST_FILE_NAME} | grep -w ${TEST_CASE_NAME}"))
	@if [ -z "${PREFIX}" ] || [ -z "${POSTFIX}" ]; then \
		echo "Error: Failed to start test, check if TEST_FILE_NAME=${TEST_FILE_NAME}" \
			"or TEST_CASE_NAME=${TEST_CASE_NAME} exist."; exit 1;\
	fi
	@docker run ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c "cd tests/apollo/; \
		BREAK_ON_FAILURE=${BREAK_ON_FAILURE__} NUM_REPEATS=${NUM_REPEATS__} $(PREFIX) $(POSTFIX)"

.PHONY: test-single-gtest-case
test-single-gtest-case: ## Run a single GoogleTest test case: `make test-single-gtest-case TEST_NAME=<test suite name> TEST_CASE_FILTER=<test case name> NUM_REPEATS=<number of repeats,default=1,optional> BREAK_ON_FAILURE=<TRUE|FALSE,optional>`. Call `make lists-tests` to get test suite name. The test case STRING is a filter used with --gtest_filter=*<STRING>*. Example: `make test-single-gtest-case BREAK_ON_FAILURE=TRUE NUM_REPEATS=10 TEST_NAME=bcstatetransfer_tests TEST_CASE_FILTER=srcHandleAskForCheckpointSummariesMsg`
	@if [ -z ${TEST_NAME} ]; then echo "Error: TEST_NAME is mandatory"; exit 1; fi
	@if [ -z ${TEST_CASE_FILTER} ]; then echo "Error: TEST_CASE_FILTER is mandatory"; exit 1; fi
	$(eval PREFIX := $(shell docker run ${BASIC_RUN_PARAMS} \
			${CONCORD_BFT_CONTAINER_SHELL} -c "cd ${CONCORD_BFT_BUILD_DIR} && find . -iname ${TEST_NAME}"))
	@if [ '${BREAK_ON_FAILURE__}' = 'TRUE' ]; then break_on_failure_opt="--gtest_throw_on_failure"; fi; \
	docker run ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c "${CONCORD_BFT_BUILD_DIR}/${PREFIX} \
		--gtest_filter=*${TEST_CASE_FILTER}* --gtest_repeat=${NUM_REPEATS__} $${break_on_failure_opt}";

.PHONY: clean
clean: ## Clean Concord-BFT build directory
	docker run ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"rm -rf ${CONCORD_BFT_BUILD_DIR}"

.PHONY: clean-all
clean-all: ## Clean Concord-BFT build directory and any other untracked/ignored files.For a 'dry run' use DRY_RUN=TRUE, for an interactive run use INTER=TRUE.
	docker run ${BASIC_RUN_PARAMS} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"rm -rf ${CONCORD_BFT_BUILD_DIR}"
		@if [ "${DRY_RUN}" = "TRUE" ] && [ "${INTER}" = "TRUE" ]; then git clean -dxfni; fi
		@if [ "${DRY_RUN}" != "TRUE" ] && [ "${INTER}" != "TRUE" ]; then git clean -dxf; fi
		@if [ "${DRY_RUN}" = "TRUE" ] && [ "${INTER}" != "TRUE" ]; then git clean -dxfn; fi
		@if [ "${DRY_RUN}" != "TRUE" ] && [ "${INTER}" = "TRUE" ]; then git clean -dxfi; fi

.PHONY: codecoverage
codecoverage: ## Generate Code Coverage report for Apollo tests
	docker run  ${BASIC_RUN_PARAMS} \
                ${CONCORD_BFT_CONTAINER_SHELL} -c \
                "./scripts/run-codecoverage.sh"
	@echo "Completed make codecoverage"

.PHONY: build-docker-image
build-docker-image: ## Build the image. Note: without caching
	docker build --rm --no-cache=true -t ${CONCORD_BFT_DOCKER_IMAGE}:latest \
		-f ./${CONCORD_BFT_DOCKERFILE} .
	@echo
	@echo "Build finished. Docker image name: \"${CONCORD_BFT_DOCKER_IMAGE}:latest\"."
	@echo "Before you push it to Docker Hub, please tag it(CONCORD_BFT_DOCKER_IMAGE_VERSION + 1)."
	@echo "If you want the image to be the default, please update the following variables:"
	@echo "${CURDIR}/Makefile: CONCORD_BFT_DOCKER_IMAGE_VERSION"
