## Overview

Comdb2 is a clustered RDBMS built on Optimistic Concurrency Control
techniques. It provides multiple isolation levels, including Snapshot 
and Serializable Isolation. Read/Write transactions run on any node, 
with client library transparently negotiating connections to lowest 
cost (latency) node which is available. Client library provides
transparent reconnect.

Work on Comdb2 was started at Bloomberg LP in 2004 and it has been under heavy
developement since. More information about the architechture of the project can
be found in our [VLDB 2016 paper](http://www.vldb.org/pvldb/vol9/p1377-scotti.pdf)
and for more information on usage please look in in the [Docs](https://bloomberg.github.io/comdb2/overview_home.html).

[![Build](http://comdb2.s3-website-us-east-1.amazonaws.com/master.svg)](http://comdb2.s3-website-us-east-1.amazonaws.com/tests/master/detail.txt)


## Comdb2 Directory Contents

| Directory | Description |
| --- | --- |
| bb/           | Useful generic modules |
| bbinc/        | Header files |
| bdb/          | Table layer |
| bbinc/        | Generic include files |
| berkdb/       | Btrees layer |
| cdb2api/      | Client code |
| cdb2jdbc/     | JDBC driver |
| comdb2rle/    | Run length encoding |
| comdb2tests/  | Contains all the tests that will be run for this version |
| config/       | lrl config files |
| contrib/      | Misc useful programs that aren't part of core Comdb2 |
| crc32c/       | Dhecksum component |
| csc2/         | csc2 processing |
| csc2files/    | csc2 config files |
| cson/         | JSON library |
| datetime/     | Datetime component |
| db/           | Types layer and overall glue |
| deb/          | Sample debian package config |
| dfp/          | Decimal number component |
| dlmalloc/     | Local malloc version |
| docs/         | Documentation |
| lua/          | All things pertaining to lua VM used for stored procedures |
| net/          | Network component |
| protobuf/     | API to communicate with the server |
| schemachange  | Code for table create/alter/truncate/etc |
| sqlite/       | Sqlite VM SQL engine  |
| tests/        | Comdb2 test suite |
| tools/        | Tools that are part of Comdb2 core |

## Documentation

[Comdb2 documentation](http://bloomberg.github.io/comdb2) is included in the `docs` directory. 
It can be hosted locally with jekyll by running `jekyll serve` from the `docs` directory.

## Quick Start

On every machine in the cluster:

1. Make sure all machines in the cluster can talk to each other via ssh.  Copy keys around if
needed.

2. Install prerequisites: 
   
   ** Ubuntu 16.04, 16.10 **
        
   ```
   sudo apt-get install -y build-essential bison flex libprotobuf-c-dev   \
   libreadline-dev libsqlite3-dev libssl-dev libunwind-dev libz1 libz-dev \
   make gawk protobuf-c-compiler uuid-dev liblz4-tool liblz4-dev          \
   libprotobuf-c1 libreadline6 libsqlite3-0 libuuid1 libz1 tzdata         \
   ncurses-dev tcl bc
   ```

   ** CentOS 7 **

   ```
   sudo yum install -y gcc gcc-c++ protobuf-c libunwind libunwind-devel   \
   protobuf-c-devel byacc flex openssl openssl-devel openssl-libs         \
   readline-devel sqlite sqlite-devel libuuid libuuid-devel zlib-devel    \
   zlib lz4-devel gawk tcl epel-release lz4
   ```

3. Build Comdb2:

   ```
   make && sudo make install
   ```

4. Add */opt/bb/bin* to your PATH

   ```
   export PATH=$PATH:/opt/bb/bin
   ```

5. Start pmux:
   ```
   pmux -n
   ```
6. We need to tell comdb2 our FQDN - all nodes must agree on each other's names:
   ```bash
   vi /opt/bb/etc/cdb2/config/comdb2.d/hostname.lrl
   configure add current machine's name, e.g.
   hostname mptest-1.comdb2.dob1.bcpc.bloomberg.com
   ```

7. On one machine, create a database.
   ```
   comdb2 --create --dir /home/mponomar/db mikedb
   ```
   
8. Configure the nodes in the cluster:
   ```
   vi /home/mponomar/db/mikedb.lrl
   add
   cluster nodes mptest-1.comdb2.dob1.bcpc.bloomberg.com mptest-2.comdb2.dob1.bcpc.bloomberg.com
   ```
   
9. On other nodes, copy the database over:
   ```
   copycomdb2 mptest-1.comdb2.dob1.bcpc.bloomberg.com:/home/mponomar/db/mikedb.lrl
   ```
   
0. On all nodes, start the database.
   ```
   comdb2 --lrl /home/mponomar/db/mikedb.lrl mikedb
   ```
   All nodes will say 'I AM READY.' when ready.

1. On any node, start using the database.  You don't have any tables yet.  You can add them with *cdb2sql* 
   Example -
   ```sql
   cdb2sql mikedb local 'CREATE TABLE t1 {
        schema {
            int a
        }
   }'
   ```

   Database can be queried/updated with cdb2sql:
   ```sql
   cdb2sql mikedb local 'insert into t1(a) values(1)'
   (rows inserted=1)
   cdb2sql mikedb local 'select * from t1'
   (a=1)
   ```
