#!/bin/bash

#exit non-zero return code if a simple command exits with non-zero return code
set -e

echo "TRAVIS_PULL_REQUEST = $TRAVIS_PULL_REQUEST"
echo "TRAVIS_BRANCH = $TRAVIS_BRANCH"
echo "TRAVIS_TAG = $TRAVIS_TAG"
version=`cat version.sbt`
echo "version = $version"

sbt_cmd="sbt ++$TRAVIS_SCALA_VERSION"

eval "$sbt_cmd clean compile test it:test"
