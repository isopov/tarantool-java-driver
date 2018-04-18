[![Build Status](https://travis-ci.org/isopov/tarantool-java-driver.svg?branch=master)](https://travis-ci.org/isopov/tarantool-java-driver)
[![CircleCI Build Status](https://circleci.com/gh/isopov/tarantool-java-driver/tree/master.svg?style=svg)](https://circleci.com/gh/isopov/tarantool-java-driver/tree/master)

This connector is not used anywhere in production alternative to official Tarantool java connector that can be found at https://github.com/tarantool/tarantool-java

Pros (why this connector is better):
* It has tests (oficial driver has none and do not want any https://github.com/tarantool/tarantool-java/pull/25 )
* Tests are run regularly (on every push) on 4 Tarantool versions - 1.6, 1.7, 1.9 and 2.0 on Travis and on 2.0 docker version on CircleCi
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

