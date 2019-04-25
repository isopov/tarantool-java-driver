[![Build Status](https://travis-ci.org/isopov/tarantool-java-driver.svg?branch=master)](https://travis-ci.org/isopov/tarantool-java-driver)
[![CircleCI Build Status](https://circleci.com/gh/isopov/tarantool-java-driver/tree/master.svg?style=svg)](https://circleci.com/gh/isopov/tarantool-java-driver/tree/master)

This connector is not used anywhere in production alternative to official Tarantool java connector that can be found at https://github.com/tarantool/tarantool-java
It has extensive test suite and tests are run regularly (on every push) on 4 Tarantool versions - 1.6, 1.7, 1.9 and 2.1 on Travis and on 2.1 version on CircleCi

Pros (why this connector is better):
* (This list was larger - official connector addressed some issues)
* It uses https://github.com/msgpack/msgpack-java instead of custom Msgpack implementation. This leads to at least some level of confidence, and in more memory-efficient msgpack processing (https://github.com/isopov/tarantool-java-driver/blob/master/tarantool-benchmarks/src/main/java/com/sopovs/moradanen/tarantool/benchmarks/SingleSelectBenchmark.java)
* API is more strict - no need for casts

Cons (why oficial connector is better)
* It is used somewhere
* It uses custom implementation of Msgpack (It seems that it can be potentially better than common library)
* It has working async version
* Having essentially 1 or 2 public methods API may be considered simplier
* It is deployed to Maven Central
* API is certainly more stable - this driver may change in any not foreseeable way 
* Java 6 support

# Building
* Run tarantool from docker: `docker run -p 3301:3301 --rm -t -i -e TARANTOOL_USER_NAME=admin -e TARANTOOL_USER_PASSWORD=javapass tarantool/tarantool:2.1`
* `./mvnw clean verify`

# Getting started
Please use samples repo - https://github.com/isopov/tarantool-java-driver-samples