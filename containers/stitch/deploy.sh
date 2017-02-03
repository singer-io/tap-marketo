#!/bin/bash

set -e

if [ "$#" -ne 1 ]; then
    echo Usage: $0 VERSION
    exit 1
fi

name=platform.marketo
version=$1
repo=REPLACE
docker build -t $name:$version -f Dockerfile ../..
docker tag $name:$version $repo/$name:$version
sudo docker push $repo/$name:$version