#!/usr/bin/env bash

docker run -it -v /Users/bobuzhang/Downloads/code/incubator-doris/.m2:/root/.m2 -v /Users/bobuzhang/Downloads/code/incubator-doris/:/root/doris apache/incubator-doris:build-env-1.3
