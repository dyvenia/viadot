#!/usr/bin/env bash

IMAGE_TAG=latest
PROFILE="user"
PLATFORM=""

while getopts "t:p:" flag
do
    case "${flag}" in
        t) IMAGE_TAG=${OPTARG}
            case ${OPTARG} in
                dev) PROFILE="dev";;
            esac
        ;;
        p) PLATFORM=${OPTARG}
        ;;
    esac
done

PLATFORM=${PLATFORM:-linux/amd64}


echo "Using platform: $PLATFORM"

IMAGE_TAG=$IMAGE_TAG PLATFORM=$PLATFORM docker compose up -d --force-recreate

echo ""
echo "Press Enter to exit."

read
