CONCORD_BFT_DOCKER_REPO:=concordbft/
CONCORD_BFT_DOCKER_IMAGE:=concord-bft
CONCORD_BFT_DOCKER_IMAGE_VERSION:=0.4

CONCORD_BFT_DOCKERFILE:=Dockerfile
CONCORD_BFT_BUILD_DIR:=build
CONCORD_BFT_TARGET_SOURCE_PATH:=/concord-bft
CONCORD_BFT_CONTAINER_SHELL:=/bin/bash
CONCORD_BFT_CONTAINER_CC:=clang
CONCORD_BFT_CONTAINER_CXX:=clang++
CONCORD_BFT_CMAKE_FLAGS:=-DUSE_CONAN=OFF \
			-DCMAKE_BUILD_TYPE=Debug \
			-DBUILD_TESTING=ON \
			-DBUILD_COMM_TCP_PLAIN=FALSE \
			-DBUILD_COMM_TCP_TLS=FALSE \
			-DCMAKE_CXX_FLAGS_RELEASE=-O3 -g \
			-DUSE_LOG4CPP=TRUE \
			-DBUILD_ROCKSDB_STORAGE=TRUE \
			-DUSE_S3_OBJECT_STORE=FALSE \
			-DUSE_OPENTRACING=ON \
			-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
			-DOMIT_TEST_OUTPUT=OFF

# The consistency parameter makes sense only at MacOS.
# It is ignored at all other platforms.
CONCORD_BFT_CONTAINER_MOUNT_CONSISTENCY=,consistency=cached
CONCORD_BFT_CTEST_TIMEOUT:=3000 # Default value is 1500 sec. It takes 2500 to run all the tests at my dev station

.PHONY: help
help: ## The Makefile helps to build Concord-BFT in a docker container
	@cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

	# Basic HOW-TO:
	# make pull               # Pull image from Docker Hub
	# make run-c              # Run container in background
	# make build-s            # Build Concord-BFT sources
	# make test               # Run tests
	# make remove-c           # Remove existing container
	# make build-docker-image # Build docker image locally

.PHONY: pull
pull: ## Pull image from remote
	docker pull ${CONCORD_BFT_DOCKER_REPO}${CONCORD_BFT_DOCKER_IMAGE}:${CONCORD_BFT_DOCKER_IMAGE_VERSION}

.PHONY: run-c
run-c: ## Run container in background
	docker run -d --rm --privileged=true \
			  --cap-add NET_ADMIN --cap-add=SYS_PTRACE --ulimit core=-1 \
			  --name="${CONCORD_BFT_DOCKER_IMAGE}" \
			  --mount type=bind,source=${CURDIR},target=/cores \
			  --mount type=bind,source=${CURDIR},target=${CONCORD_BFT_TARGET_SOURCE_PATH}${CONCORD_BFT_CONTAINER_MOUNT_CONSISTENCY} \
			  ${CONCORD_BFT_DOCKER_REPO}${CONCORD_BFT_DOCKER_IMAGE}:${CONCORD_BFT_DOCKER_IMAGE_VERSION} \
			  /usr/bin/tail -f /dev/null
	@echo
	@echo "The container \"${CONCORD_BFT_DOCKER_IMAGE}\" with the build environment is started as daemon."
	@echo "Run \"make stop-c\" to stop or \"make remove-c\" to delete."

.PHONY: login
login: ## Login to the container
	docker exec -it --workdir=${CONCORD_BFT_TARGET_SOURCE_PATH} \
		${CONCORD_BFT_DOCKER_IMAGE} ${CONCORD_BFT_CONTAINER_SHELL};exit 0

.PHONY: stop-c
stop-c: ## Stop the container
	docker container stop ${CONCORD_BFT_DOCKER_IMAGE}
	@echo
	@echo "The container \"${CONCORD_BFT_DOCKER_IMAGE}\" is successfully stopped."

.PHONY: remove-c
remove-c: ## Remove the container
	docker container rm -f ${CONCORD_BFT_DOCKER_IMAGE}
	@echo
	@echo "The container \"${CONCORD_BFT_DOCKER_IMAGE}\" is successfully removed."

.PHONY: build
build: ## Build Concord-BFT source. Note: this command is mostly for developers
	docker exec -t --workdir=${CONCORD_BFT_TARGET_SOURCE_PATH} ${CONCORD_BFT_DOCKER_IMAGE} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"mkdir -p ${CONCORD_BFT_TARGET_SOURCE_PATH}/${CONCORD_BFT_BUILD_DIR} && \
		cd ${CONCORD_BFT_BUILD_DIR} && \
		CC=${CONCORD_BFT_CONTAINER_CC} CXX=${CONCORD_BFT_CONTAINER_CXX} \
		cmake ${CONCORD_BFT_CMAKE_FLAGS} .. && \
		make format-check && \
		make -j $$(nproc)"
	@echo
	@echo "Build finished. The binaries are in ${CURDIR}/${CONCORD_BFT_BUILD_DIR}"

.PHONY: test
test: ## Run all tests
	docker exec -t --workdir=${CONCORD_BFT_TARGET_SOURCE_PATH} ${CONCORD_BFT_DOCKER_IMAGE} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"cd ${CONCORD_BFT_BUILD_DIR} && \
		ctest --timeout ${CONCORD_BFT_CTEST_TIMEOUT} --output-on-failure"

.PHONY: single-test
single-test: ## Run single test `make single-test TEST_NAME=<test name>`
	docker exec -t --workdir=${CONCORD_BFT_TARGET_SOURCE_PATH} ${CONCORD_BFT_DOCKER_IMAGE} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"cd ${CONCORD_BFT_BUILD_DIR} && \
		ctest -R ${TEST_NAME} --timeout ${CONCORD_BFT_CTEST_TIMEOUT} --output-on-failure"

.PHONY: clean
clean: ## Clean Concord-BFT build directory
	docker exec -t --workdir=${CONCORD_BFT_TARGET_SOURCE_PATH} ${CONCORD_BFT_DOCKER_IMAGE} \
		${CONCORD_BFT_CONTAINER_SHELL} -c \
		"rm -rf ${CONCORD_BFT_BUILD_DIR}"

.PHONY: build-docker-image
build-docker-image: ## Re-build the container without caching
	docker build --rm --no-cache=true -t ${CONCORD_BFT_DOCKER_IMAGE}:latest \
		-f ./${CONCORD_BFT_DOCKERFILE} .
	@echo
	@echo "Build finished. Docker image name: \"${CONCORD_BFT_DOCKER_IMAGE}:latest\"."
	@echo "Before you push it to Docker Hub, please tag it(CONCORD_BFT_DOCKER_IMAGE_VERSION + 1)."
	@echo "If you want the image to be the default, please update the following variables:"
	@echo "1. ${CURDIR}/Makefile: CONCORD_BFT_DOCKER_IMAGE_VERSION"
	@echo "2. ${CURDIR}/.travis.yml: DOCKER_IMAGE_VER"
	@echo "3. ${CURDIR}/.github/workflows/build_and_test.yml: DOCKER_IMAGE_VER"
