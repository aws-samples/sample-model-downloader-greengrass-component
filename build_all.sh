#!/bin/bash
# set -e

source .env

echo "########## BUILD DOCKER IMAGES ##########"
if [ -d "docker" ] && [ "$(find docker -mindepth 1 -maxdepth 1 -type d | grep -v 'docker/checksums' | wc -l)" -gt 0 ]; then
    for DIR in $(find docker -mindepth 1 -maxdepth 1 -type d | grep -v 'docker/checksums' | sed 's/docker\///' | sed 's/\///')
    do
        echo "Building $DIR"
        pushd "docker/$DIR"
        source ./build.sh
        popd
        echo "$DIR build complete"
    done
else
    echo "No Docker directories found. Skipping Docker build."
fi
echo "########## FINISHED DOCKER BUILD ##########"

echo "########## BUILD GREENGRASS COMPONENTS ##########"
if [ -d "components" ] && [ "$(find components -mindepth 1 -maxdepth 1 -type d | grep -v 'components/checksums' | wc -l)" -gt 0 ]; then
    for DIR in $(find components -mindepth 1 -maxdepth 1 -type d | grep -v 'components/checksums' | sed 's/components\///' | sed 's/\///')
    do
        echo "Building $DIR"
        pushd "components/$DIR"
        source ./build.sh
        popd
        echo "$DIR build complete"
    done
else
    echo "No component directories found. Skipping Greengrass build."
fi
echo "########## FINISHED GREENGRASS BUILD ##########"

echo "All builds complete. Run ./publish_all.sh to publish components and Docker images."