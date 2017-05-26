#!/bin/bash

#exit non-zero return code if a simple command exits with non-zero return code
set -e

sbt_cmd="sbt ++$TRAVIS_SCALA_VERSION"

eval "$sbt_cmd clean compile test"
