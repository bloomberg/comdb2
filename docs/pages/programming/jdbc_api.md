---
title: JDBC API
keywords: JDBC Java
sidebar: mydoc_sidebar
permalink: jdbc_api.html
---

## Introduction

Comdb2 provides Java programming language binding with _Cdb2jdbc_.
Cdb2jdbc is a JDBC Type 4 driver, which means that the driver is a platform-independent, pure Java implementation.


## Installation

### Installing from Source

To check out the source, please follow [the instructions](install.html#installing-from-source).

#### Maven

To install cdb2jdbc from source, the following additional software packages are required:

*   JDK 1.6 or above
*   [Protocol Buffers compiler 3.2](https://developers.google.com/protocol-buffers/). Make sure that Protocol Buffers compiler is included in your `PATH`.
*   [Maven 3.x](https://maven.apache.org/)

Once you check out the source and have all the required software installed on the system, change directory to cdb2jdbc under comdb2 source and type `mvn clean install`.

```shell
cd cdb2jdbc
mvn clean install
```

cdb2jdbc should be successfully installed in your local Maven repository.
The JAR files normally can be found in `~/.m2/repository/com/bloomberg/comdb2/cdb2jdbc/`.

**A word of caution**: the build can fail if the version of protocol buffers installed on the system mismatches the version specified in
`cdb2jdbc/pom.xml`.  If you encounter problems, update the protobuf-java version in the .pom file to match what's on the system.
Another option is to build the driver inside a Docker container by running `make jdbc-docker-build` in `cdb2jdbc` (JAR files will be written
to `cdb2jdbc/maven.m2/repository/com/bloomberg/comdb2/cdb2jdbc/2.0.0/`)

#### Gradle

Another way to install cdb2jdbc from source is the gradle wrapper. The following software packages are required:

*   JDK 1.7 or above

> Note that existing installations of gradle and protocol buffers **are not** required for the gradle build

Once you check out the source and have all the required software installed on the system, change directory to cdb2jdbc under comdb2 source and type `./gradlew install` or `./gradlew.bat install` for Windows.

```shell
cd cdb2jdbc
./gradlew install
```

> You can also use an existing install of gradle instead of the wrapper. At least version 2.12 is required.

This build will install cdb2jdbc into your *local Maven* repository.
The JAR files normally can be found in `~/.m2/repository/com/bloomberg/comdb2/cdb2jdbc/`.

## Setting up Cdb2jdbc

There are 2 approaches to set up cdb2jdbc: using build tools or setting the classpath.

### Build tools

#### As a Maven dependency

To introduce cdb2jdbc in your applications as a Maven dependency, add the following to `pom.xml`, with the version available from your Maven repository.

```xml
<dependency>
  <groupId>com.bloomberg.comdb2</groupId>
  <artifactId>cdb2jdbc</artifactId>
  <version>major.minor.patch</version>
</dependency>
```

#### As a Gradle dependency

If you followed the steps above to install cdb2jdbc form source, it should be in your local Maven repository. Add the following to your `build.gradle` with the version replaced to the cdb2jdbc version. 

```groovy
repositories {
    mavenLocal()
}

dependencies {
    compile 'com.bloomberg.comdb2:cdb2jdbc:major.minor.patch'
}
```

### Setting the Classpath

By default, an uber JAR is built along with cdb2jdbc and is named `cdb2jdbc-<version>-shaded.jar`.
An uber JAR is a JAR file which contains all its dependencies. To use the JAR without Maven, you would include it in `CLASSPATH`.

```shell
export CLASSPATH=<path_to_the_uber_jar>:$CLASSPATH
```

## Using Cdb2jdbc

\* _The section is not intended as a guide to JDBC programming.
For more information, please refer to [the official JDBC website](http://www.oracle.com/technetwork/java/javase/jdbc/index.html)_

### Loading the Driver

One common approach for loading the driver is to use `Class.forName()`. To load cdb2jdbc, you would use:

```java
Class.forName("com.bloomberg.comdb2.jdbc.Driver");
```

### Connecting to a Database

A Comdb2 database is represented by a JDBC URL. Cdb2jdbc takes one of the following forms:

```
jdbc:comdb2://<stage>/<database>[?options]
jdbc:comdb2://<host>[:port][, <host>[:port], ...]/<database>[?options]
```

For example, to connect to a local database called `testdb`, you would use:

```java
Connection conn = DriverManager.getConnection("jdbc:comdb2://localhost/testdb");
```

The parameters are as follows:

* _stage_

  Please refer to [cdb2_open](c_api.html#cdb2open) for the information about `stage`.

* _host_

  The host name of the server.

* _port_

  The port number the database is listening on. If not specified, cdb2jdbc queries `pmux` to get the dynamically allocated port number.

  \* _To support connection failover, you would define multiple **host/port** pairs in the URL, separated by commas._

* _database_

  The database name.

* _options_


    * _user_=String

      The database user. The default is empty.
        
    * _password_=String
      
      The password of the database user. The default is empty.

    * _comdb2_timeout_=Integer

      The server-side socket read and write timeouts in seconds. The default is 10 seconds.

    * _load_balance_="room" \| "randomroom" \| "random"

      The load balance policy.

    * _default_type_=String

      Please refer to [Client Setup](clients.html#client-configuration-files) for more information.

    * _room_=String

      Please refer to [Client Setup](clients.html#client-configuration-files) for more information.

    * _comdb2dbname_=String

      Please refer to [Client Setup](clients.html#client-configuration-files) for more information.

    * _dnssuffix_=String

      Please refer to [Client Setup](clients.html#client-configuration-files) for more information.

    * _portmuxport_=Integer

      The port number which `pmux` is listening on. The default is 5105.

    * _tcpbufsz_=Integer

      The value for SO_RCVBUF, which is the maximum socket receive buffer size for the underlying TCP connections to the database server.

    * _microsecond_fraction_=Boolean

      The precision of the fractional second when binding a `java.sql.Timestamp` parameter.
      If it is set to false, millisecond precision is used. otherwise, microsecond precision is used. The default is true.

    * _preferred_machine_=String

      When set, the driver connects to the preferred machine whenever possible,
      with "RANDOM" load balance strategy.
      
   * _ssl_mode_=String

      Can be one of the following values:

        * ALLOW

        * REQUIRE

        * VERIFY_CA

        * VERIFY_HOSTNAME
      
    * _key_store_=String

      Path to the client keystore.

    * _key_store_password_=String

      Passphrase of the client keystore.
      
    * _key_store_type_=String

      Type of the keystore. The default is `"JKS"`.
      
    * _trust_store_=String

      Path to the certificate authority (CA) keystore.

    * _trust_store_password_=String

      Passphrase of the CA keystore.
      
    * _trust_store_type_=String

      Type of the trusted CA keystore. The default is `"JKS"`.

    * _crl_=String

      Path to the Certificate Revocation List (CRL), in `PEM` format.

    * _allow_pmux_route_=Boolean

      Allow connection forwarding via `pmux`. The default is `false`.
        
    * _verify_retry_=Boolean

      Toggle verifyretry. The default is `false`. See also [optimistic concurrency control](transaction_model.html#optimistic-concurrency-control).

        
    \* _To define multiple options, separate them by ampersands._

### Issuing a Query and Browsing the Result

The example below issues a simple `SELECT` query and prints the rows.

```java
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT column FROM table WHERE column = 1");
while (rs.next())
    System.format("column is %s\n", rs.getString(1));
rs.close();
stmt.close();
```

The example uses `PreparedStatement` to issue a query.

```java
int column = 1;
PreparedStatement ps = conn.prepareStatement("SELECT column FROM table WHERE column = ?");
ps.setInt(1, column);
ResultSet rs = ps.executeQuery();
while (rs.next())
    System.format("column is %s\n", rs.getString(1));
rs.close();
ps.close();
```

### Performing Updates

To perform updates (`INSERT`, `UPDATE` or `DELETE`), you would use `executeUpdate()`.
This function does not return a `ResultSet` object. Instead it returns the number of rows affected.

The example performs a delete and prints the number of rows deleted.

```java
Statement stmt = conn.createStatement();
int ndel = stmt.executeUpdate("DELETE FROM table WHERE column = 1");
System.format("%d row(s) deleted\n", ndel);
stmt.close();
```

### Invoking Stored Procedures

Cdb2jdbc does not implement the `CallableStatement` interface.
Instead, you should use the `Statement` or `PreparedStatement`.

The example invokes a stored procedure called `foo`.

```java
Statement sp = conn.createStatement();
sp.executeQuery("exec procedure foo()");
```

The result would come back as a normal `ResultSet` object.

### Using SSL connections

The jdbc driver has built-in SSL support. It uses SSL if the server requires SSL. Otherwise it falls back to plaintext.
The process is transparent to the application. To enforce SSL, you would add `ssl_mode=REQUIRE` to the JDBC URL.

If the server requires client certificates, you need to provide the driver with a Java KeyStore (JKS).

A JKS can be converted from an OpenSSL certificate. First, you would convert the certificate to `DER` format if it is in `PEM` format using OpenSSL:

```shell
# Convert PEM to something java understands first
$ openssl x509 -in client.pem -out client.crt.der -outform der
```

To convert a DER certificate to JKS, you would use `$JAVA_HOME/bin/keytool`:

```shell
# Convert the DER certificate to a java KeyStore file.
$ keytool -import -file client.crt.der -keystore path/to/keystore -alias comdb2
```

To load the JKS into the driver, the JDBC URL looks like this:

```
jdbc:comdb2//<hostname>/<database>?key_store=<path/to/jks>&key_store_password=<passwd>
```

A trusted CA keystore can be generated the same way to authenticate the server.

To load the trusted CA JKS into the driver, the JDBC URL looks like this:

```
jdbc:comdb2//<hostname>/<database>?trust_store=<path/to/jks>&trust_store_password=<passwd>
```


## Comdb2 Extensions to the JDBC API

### Executing Typed Queries

A typed query gives applications more control over return types.
The database will coerce the types of the resulting columns to the types specified by the application.
If the types aren’t compatible, an exception will be thrown.

To access the extension, you would need to cast the `java.sql.Statement` object to `com.bloomberg.comdb2.jdbc.Comdb2Statement`.
For example:

```java
conn = DriverManager.getConnection("jdbc:comdb2:/localhost/testdb");
stmt = conn.createStatement();
comdb2stmt = (Comdb2Statement)stmt;
String sql = "select now()";
rset = comdb2stmt.executeQuery(sql, Arrays.asList(java.sql.Types.VARCHAR));
```

In the example above, the row will come back as a `java.lang.String` instead of a `java.sql.TIMESTAMP`.


### Using Intervals

JDBC standard does not define data types for intervals. You could work it around using Comdb2 interval types.
For example, to bind an INTERVALDS (interval day to second) value, you would use:

```
// Load driver, get connection and create a parepared statement...
// bind an interval year-month (+3 mon)
stmt.setObject(1, new Cdb2Types.IntervalYearMonth(1, 0, 3));
```

### Using Named Parameters for PreparedStatement

Comdb2 has built-in support for named parameters. You would simply use `@param_name` instead of `?` in your queries:

```
PreparedStatement stmt = conn.prepareStatement("insert into employee values (@id, @firstname, @lastname)");
```


## Java and Comdb2 Types

The table below shows the conversions between Java and Comdb2 types.

| Java Type | Comdb2 Type |
|-----------|-------------|
| java.lang.Short | short |
| java.lang.Short | u_short |
| java.lang.Integer | int |
| java.lang.Integer | u_int |
| java.lang.Long | longlong |
| java.lang.Float | float |
| java.lang.Double | double |
| java.math.BigDecimal<br>java.lang.String | decimal32 |
| java.math.BigDecimal<br>java.lang,String | decimal64 |
| java.math.BigDecimal<br>java.lang.String | decimal128 |
| java.sql.Blob<br>java.sql.Array<br>byte[] | byte |
| java.sql.Blob<br>java.sql.Array<br>byte[] | blob |
| java.lang.String | cstring |
| java.lang.String | vutf8 |
| java.sql.Date<br>java.sql.Time<br>java.sql.Timestamp<br>java.util.Calendar | datetime |
| java.sql.Timestamp | datetimeus |
| com.bloomberg.comdb2.jdbc.Cdb2Types.IntervalYearMonth | intervalym |
| com.bloomberg.comdb2.jdbc.Cdb2Types.IntervalDaySecond | intervalds |
| com.bloomberg.comdb2.jdbc.Cdb2Types.IntervalDaySecondUs | intervaldsus |



## SQLSTATEs in Cdb2jdbc

The following table shows how SQLSTATEs are mapped to SQLExceptions in cdb2jdbc.

| SQLSTATE | SQLException | Comment |
|----------|--------------|---------|
| 08000 | SQLNonTransientConnectionException<br>SQLTransientConnectionException | connection errors |
| 42000 | SQLSyntaxErrorException | syntax errors |
| 28000 | SQLInvalidAuthorizationSpecException | invalid credentials |
| 22000 | SQLNonTransientException | data errors |
| 25000 | SQLNonTransientException | wrong sql engine state |
| 0A000 | SQLFeatureNotSupportedException | not supported |
| 23000 | SQLIntegrityConstraintViolationException | constraint violations |
| COMDB | SQLException | db errors |
