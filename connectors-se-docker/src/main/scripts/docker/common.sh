#! /bin/bash

#
#  Copyright (C) 2006-2018 Talend Inc. - www.talend.com
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

export COMPONENT_SERVER_IMAGE_VERSION=1.1.2_20181016100528

export BASEDIR=$(cd "$(dirname "$0")" ; pwd -P)/../../../..
export CONNECTOR_VERSION=$(grep "<version>" "$BASEDIR/pom.xml" | head -n 1 | sed "s/.*>\\(.*\\)<.*/\\1/")
export TALEND_REGISTRY="${TALEND_REGISTRY:-registry.datapwn.com}"
DOCKER_IMAGE_VERSION=${DOCKER_IMAGE_VERSION:-$CONNECTOR_VERSION}
if [[ "$DOCKER_IMAGE_VERSION" = *"SNAPSHOT" ]]; then
    BRANCH=_${1}
    DOCKER_IMAGE_VERSION=$(echo $CONNECTOR_VERSION | sed "s/-SNAPSHOT//")${BRANCH}_$(date +%Y%m%d%H%M%S)
fi
export DOCKER_IMAGE_VERSION

function extractApplicationName() {
    echo $(grep 'com.talend.application' "${1:-Dockerfile}"  | sed -r 's/.*\"([^"]+)\".*/\1/')
}

function buildAndTag() {
    imageName=$(extractApplicationName "${1:-Dockerfile}")
    image="talend/$imageName:$DOCKER_IMAGE_VERSION"
    echo ""
    echo "Building connector image >$image<"
    echo ""
    docker build \
        --tag "$image" \
        --build-arg GIT_URL=$(git config --get remote.origin.url) \
        --build-arg GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD) \
        --build-arg GIT_REF=$(git rev-parse HEAD) \
        --build-arg DOCKER_IMAGE_VERSION=$DOCKER_IMAGE_VERSION \
        --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
        . && \
    docker tag "$image" "$TALEND_REGISTRY/$image" || exit 1
    export LAST_IMAGE="$image"
}

function pushImage() {
    if [ -n "$DOCKER_LOGIN" ]; then
        set +x
        echo "$DOCKER_PASSWORD" | docker login "$TALEND_REGISTRY" -u "$DOCKER_LOGIN" --password-stdin
        set -x
        for i in {1..5}; do
            docker push "$TALEND_REGISTRY/$1" && break || sleep 15
        done
    else
        echo "No DOCKER_LOGIN set so skipping push of >$1<"
    fi
}

function slurpImageFolder() {
    docker container create --name "$1" "$2"
    docker container cp "$1:$3" "$4"
    docker container rm -f "$1"
}

function createComponentRegistry() {
    grep '^      <artifactId>' "$BASEDIR/pom.xml" | \
        sed "s#.*<artifactId>\(.*\)</artifactId>#\1=org.talend.components:\1:$CONNECTOR_VERSION#" | \
        grep -v 'tomitribe-crest'   | \
        grep -v 'johnzon-core'      | \
        grep -v 'container-core'    | \
        grep -v 'slf4j'             | \
        sort -u > component-registry.properties
}

echo "Environment:"
echo " - TALEND_REGISTRY=$TALEND_REGISTRY"
echo " - DOCKER_IMAGE_VERSION=$DOCKER_IMAGE_VERSION"
echo ""
echo "-----------------------------------------------------"
