[![Build Status](https://travis-ci.org/isopov/tarantool-java-driver.svg?branch=master)](https://travis-ci.org/isopov/tarantool-java-driver)
[![CircleCI Build Status](https://circleci.com/gh/isopov/tarantool-java-driver/tree/master.svg?style=svg)](https://circleci.com/gh/isopov/tarantool-java-driver/tree/master)

This connector is not used anywhere in production alternative to official Tarantool java connector that can be found at https://github.com/tarantool/tarantool-java

Pros (why this connector is better):
* It has tests (oficial driver has none and do not want any https://github.com/tarantool/tarantool-java/pull/25 )
* Tests are run regularly (on every push) on 5 Tarantool versions - 1.6, 1.7, 1.9, 1.10 and 2.0 on Travis and on 2.0 docker version on CircleCi
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

# Bulding
* Install tarantool (2+ version to test support of 2+ features) or run it from docker: `docker run -p 3301:3301 --rm -t -i progaudi/tarantool:2.0`
* `./mvnw clean verify`

# Getting started
Currently this connector is deployed to jitpack only - https://jitpack.io/ and to use it you need to enable this repo in your build:
```
	<repositories>
		<repository>
		    <id>jitpack.io</id>
		    <url>https://jitpack.io</url>
		</repository>
	</repositories>
```

Then you can include dependency:
```
        <dependency>
            <groupId>com.github.isopov.tarantool-java-driver</groupId>
            <artifactId>tarantool-driver</artifactId>
            <version>0.0.4</version>
        </dependency>
```

And write some code:
```
        try (TarantoolClientSource clientSource = new TarantoolNewClientSource("localhost", 3301);
             TarantoolClient client = clientSource.getClient()) {
            client.selectAll("_vspace");
            Result result = client.execute();
            while (result.next()) {
                System.out.println(result.getString(2));
            }
        }
```
(In real use cases please use `TarantoolPooledClientSource` if you are going to execute many queries.

If you have recent enough version of Tarantool with SQL support you can use jdbc driver:
```
        <dependency>
            <groupId>com.github.isopov.tarantool-java-driver</groupId>
            <artifactId>tarantool-jdbc</artifactId>
            <version>0.0.4</version>
        </dependency>
```

With code similar to
```
        try (Connection con = DriverManager.getConnection("jdbc:tarantool://localhost");
             Statement st = con.createStatement()) {
            st.executeUpdate("CREATE TABLE MESSAGES (ID INTEGER PRIMARY KEY, VALUE VARCHAR(100))");

            try (PreparedStatement pst = con.prepareStatement("INSERT INTO MESSAGES VALUES(?,?)")) {
                pst.setInt(1, 1);
                pst.setString(2, "Hello World!");
                pst.executeUpdate();
            }

            try (ResultSet res = st.executeQuery("SELECT * FROM MESSAGES")) {
                while (res.next()) {
                    System.out.println(res.getString("VALUE"));
                }
            }

            st.executeUpdate("DROP TABLE MESSAGES");
        }
```
Similar in real use cases you probaly should use some jdbc connection pool such as [HikariCP](https://github.com/brettwooldridge/HikariCP)