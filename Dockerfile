# NOTES:
# 1. The Dockerfile does not have an entry point because everything has to be managed outside of it.
# 2. Please do not install anything directly in this file.
#   You need to use "install_deps_release.sh" - we want to keep a possibility to develop w/o docker.
# 3. If a tool or library that you install in the "install_deps_release.sh" is needed only for building 3rd party
#   tools/libraries please uninstall them after the script run - we want to keep image' size minimal.
#   For example wget and pip-tools.

ARG CONCORD_BFT_TOOLCHAIN_IMAGE_REPO="concordbft/concord-bft"
ARG CONCORD_BFT_TOOLCHAIN_IMAGE_TAG="toolchain-0.02"
FROM ${CONCORD_BFT_TOOLCHAIN_IMAGE_REPO}:${CONCORD_BFT_TOOLCHAIN_IMAGE_TAG}

ARG GIT_COMMIT
ARG GIT_BRANCH
ARG BUILD_CREATOR

LABEL build_image.branch=$GIT_BRANCH
LABEL build_image.commit=$GIT_COMMIT
LABEL build_image.build-creator=$BUILD_CREATOR
LABEL build_image.description="Build environment for concord-bft"

ENV HOME /root
COPY ./install_deps_release.sh /install_deps_release.sh

SHELL ["/bin/bash", "-c"]

RUN echo $'path-exclude /usr/share/doc/* \n\
path-exclude /usr/share/doc/*/copyright \n\
path-exclude /usr/share/man/* \n\
path-exclude /usr/share/groff/* \n\
path-exclude /usr/share/info/* \n\
path-exclude /usr/share/lintian/* \n\
path-exclude /usr/share/linda/*' > /etc/dpkg/dpkg.cfg.d/01_nodoc && \
        /bin/bash -c "/install_deps_release.sh" && \
        apt-get clean && \
        apt-get autoclean && \
        rm -rf /var/lib/apt/lists/*

# For eliot-tree's rendering
ENV LC_ALL=C.UTF-8
