## Overview

Comdb2 is a clustered RDBMS built on Optimistic Concurrency Control techniques. 
It provides multiple isolation levels, including Snapshot and Serializable Isolation. 
Read/Write transactions run on any node, with the client library transparently negotiating connections to lowest cost (latency) node which is available.
The client library provides transparent reconnect.

Work on Comdb2 was started at Bloomberg LP in 2004 and it has been under heavy development since.
More information about the architecture of the project can be found in our [VLDB 2016 paper](http://www.vldb.org/pvldb/vol9/p1377-scotti.pdf) and for more information on usage please look in the [Docs](https://bloomberg.github.io/comdb2/overview_home.html).

[![Build](http://comdb2.s3-website-us-east-1.amazonaws.com/master.svg)](http://comdb2.s3-website-us-east-1.amazonaws.com/tests/master/detail.txt)


## Documentation

[Comdb2 documentation](http://bloomberg.github.io/comdb2) is included in the `docs` directory. 
It can be hosted locally with jekyll by running `jekyll serve` from the `docs` directory.

## Contributing

Please refer to our [contribution guide](https://bloomberg.github.io/comdb2/contrib.html) for instructions.
We welcome code and idea contributions.

## Quick Start

On every machine in the cluster:

1. Make sure all machines in the cluster can talk to each other via ssh.  
   Copy keys around if needed.  

2. Install prerequisites: 
   
   ** Ubuntu 16.04, 16.10, 17.04, Windows Subsystem for Linux (WSL) **
        
   ```
   sudo apt-get install -y build-essential cmake bison flex libprotobuf-c-dev libreadline-dev libsqlite3-dev libssl-dev libunwind-dev libz1 libz-dev make gawk protobuf-c-compiler uuid-dev liblz4-tool liblz4-dev libprotobuf-c1 libsqlite3-0 libuuid1 libz1 tzdata ncurses-dev tcl bc
   ```

   ** CentOS 7 **

   ```
   sudo yum install -y gcc gcc-c++ cmake3 protobuf-c libunwind libunwind-devel protobuf-c-devel byacc flex openssl openssl-devel openssl-libs readline-devel sqlite sqlite-devel libuuid libuuid-devel zlib-devel zlib lz4-devel gawk tcl epel-release lz4 rpm-build which
   ```

   ** macOS High Sierra (experimental) **

   Install Xcode and Homebrew. Then install required libraries:

   ```
   brew install cmake lz4 openssl protobuf-c readline
   ```

   To run tests, install following:

   ```
   brew install coreutils bash
   export PATH="/usr/local/opt/coreutils/libexec/gnubin:$PATH"
   ```

   It is recommended to increase the open file limits, at least in the sessions which start pmux and server.


3. Build and Install Comdb2:

   ```
   mkdir build && cd build && cmake .. && make && sudo make install
   ```

4. Add */opt/bb/bin* to your PATH

   ```
   export PATH=$PATH:/opt/bb/bin
   ```

5. Start pmux:

   ```
   pmux -n
   ```
   Note: on WSL this needs to be run elevated, such as: `sudo /opt/bb/bin/pmux -n` 

6. _(optional)_ Comdb2 nodes identify each other by their hostnames.  If the hostname 
   of each node isn't resolvable from other nodes, we should tell Comdb2 the full 
   domain name to use for the current node.  Most setups won't have this issue.

   Tell comdb2 our FQDN.
   ```bash
   vi /opt/bb/etc/cdb2/config/comdb2.d/hostname.lrl
   add current machine's name, e.g.
   hostname machine-1.comdb2.example.com
   ```

7. On one machine (say machine-1), create a database - this example creates a database 
   called _testdb_ stored in _~/db_.

   ```
   comdb2 --create --dir ~/db testdb
   ```
   
   Note: the `--dir PATH` parameter is optional, and if it is omitted comdb2 uses a default root of */opt/bb/var/cdb2/* for creating a database directory to contain the database files, which is named as per the database name parameter; hence in this case  */opt/bb/var/cdb2/testdb*.  
   The default root will have to be created explicitly with the desired permissions before invoking `comdb2 --create` for a database.  
   In this quick start, we use the home directory to avoid obfuscating the key steps of the process. 
   
   
8. Configure the nodes in the cluster:
   ```
   vi ~/db/testdb.lrl
   add
   cluster nodes machine-1.comdb2.example.com machine-2.comdb2.example.com
   ```
   
9. On other nodes, copy the database over:
   ```
   copycomdb2 mptest-1.comdb2.example.com:${HOME}/db/testdb.lrl
   ```
   
0. On all nodes, start the database.
   ```
   comdb2 --lrl ~/db/testdb.lrl testdb
   ```
   All nodes will say 'I AM READY.' when ready.
   
   Note: the log dir comdb2 uses by default is */opt/bb/var/log/cdb2/* 
   If this directory does not have permissions allowing the user to create file, there will be diagnostics output such as:  
   > [ERROR] error opening '/opt/bb/var/log/cdb2/testdb.longreqs' for logging: 13 Permission denied  
   
   This condition will not impact operation of the database for the purposes of this quick start.  
   

1. On any node, start using the database.  You don't have any tables yet.  You can add them with *cdb2sql* 
   Example -
   ```sql
   cdb2sql testdb local 'CREATE TABLE t1 {
        schema {
            int a
        }
   }'
   ```

   Database can be queried/updated with cdb2sql:
   ```sql
   cdb2sql testdb local 'insert into t1(a) values(1)'
   (rows inserted=1)
   cdb2sql testdb local 'select * from t1'
   (a=1)
   ```

## Comdb2 Directory Contents

| Directory | Description |
| --- | --- |
| bbinc/        | Header & Generic include files |
| bdb/          | Table layer |
| berkdb/       | Btrees layer |
| cdb2api/      | Client code |
| cdb2jdbc/     | JDBC driver |
| cmake/        | cmake configuration files |
| comdb2rle/    | Run length encoding |
| contrib/      | Misc useful programs that aren't part of core Comdb2 |
| crc32c/       | Checksum component |
| csc2/         | csc2 processing |
| csc2files/    | csc2 config files |
| cson/         | JSON library |
| datetime/     | Datetime component |
| db/           | Types layer and overall glue |
| dfp/          | Decimal number component |
| dlmalloc/     | Local malloc version |
| docs/         | Documentation |
| lua/          | All things pertaining to lua VM used for stored procedures |
| mem/          | Memory accounting subsystem |
| net/          | Network component |
| pkg/          | deb and rpm packaging rules |
| plugin/       | Plugin subsystem |
| protobuf/     | API to communicate with the server |
| schemachange/ | Code for table create/alter/truncate/etc |
| sockpool/     | sockpool related files  |
| sqlite/       | Sqlite VM SQL engine  |
| tcl/          | Tcl language bindings |
| tests/        | Comdb2 test suite |
| tools/        | Tools that are part of Comdb2 core |
| util/         | Useful generic modules |

