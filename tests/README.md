## Overview

This is the comdb2 test suite.  To run the full suite, build and install the
database with `make && sudo user=$USER make install`, make sure `pmux` is running
(`pmux -l`), then run `make` in the tests directory.  Any failure will stop the
run.  `make -k` will allow other tests to run.  `make -j5` will let 5 tests to
run in parallel.  `make -kj5` is a good setting, experiment with the number
depending on your available hardware.

`testrunner` in the tests directory is a wrapper script that shows you status
of running tests. It requires python 2.7 and the `blessings` module. It implies
`-k` and takes an optional `-j` setting and a list of tests to run.

Each test is a directory with a `.test` ending.  To run a specific test, run
`make testname`.  For example `make cdb2api` will run the cdb2api test, which
is stored in `cdb2api.test`.

Tests run on the current machine.  Databases are brought up locally by default.
To test against a clustered database, export a CLUSTER variable to contain a
list of machines to use, eg: `CLUSTER="m1 m2 m3"` make cdb2api will build a
cluster on m1/m2/m3/m4/m5 and run the test there. Make can take argumests so
the same can be achieved via `make cdb2api CLUSTER="m1 m2 m3"`. The databases
are torn down after the test is over.

## Adding new tests

### 4 *easy* steps
1. Create `testname.test` directory in the tests directory.  All your test files
   (`Makefile`, `runit`, any testcases, etc.) go there.
2. Create a `Makefile`.  The following minimal makefile is sufficient for most
   tests: `include ../testcase.mk`
3. Create a script called `runit`.  This should `exit 0` if the test succeeded,
   `exit 1` if it failed. The test script will get one argument passed to it:
   the name of the database it should use.  Output from each test run will go
   into `test_xxx/logs/testname.testcase`.  If using `cdb2sql`, be sure to pass it
   `${CDB2_OPTIONS}` 

   Example `runit` file:
   ```sh
   #!/bin/bash
   # Here's some information about what we're testing for.

   dbname=$1
   if [[ -z "$dbname" ]] ; then
     echo dbname missing
     exit 1
   fi

   for testreq in `ls t*.req` ; do
     cmd="`cdb2sql` ${CDB2_OPTIONS} -f $testreq $dbname default > $testname.output 2>&1"
     $cmd
     cmd="diff $testname.expected $testname.output >/dev/null"
     $cmd
     if [[  $? -ne 0 ]]; then 
         echo "failed $testreq"
         exit 1
     fi  
   done
   exit 0
   ```

4. To help you write the test, you can start the db in iterative mode by doing
   ```sh
      cd testname.test
      echo "export TESTSROOTDIR?=$(shell readlink -f $(PWD)/..)" > Makefile
      echo "include ../testcase.mk" >> Makefile
      make setup TESTID=1234 TESTSROOTDIR=\`readlink -f ${PWD}/..\`
   ```
   
   if you want to have test directories in a particular location, set TESTDIR:
   ```sh
      make setup TESTID=1234 TESTSROOTDIR=\`readlink -f ${PWD}/..\=\` TESTDIR=/tmp/somedirfortest
   ```


For reference, if you already have the test directory properly populated, and
you can run make setup as follows:
```sh
    make setup TESTID=1234 TESTSROOTDIR=$(readlink -f $PWD/..)
```
then follow the instructions printed by the above command.


## Details

The default makefile will include logic for setting up/running the test, and
cleaning up transient files.  If the transient files are written to the test
directory and have .res or .output endings, that's all you need.  If you have
more complicated cleanup rules, add a `clean::` (note two colons) rule that does
the necessary cleanup.

The test script will get one argument passed to it: the name of the database it
should use.  You can assume the database is already reachable and routing is
set up.  Use the `default` class in any API/`cdb2sql` calls.  

The test suite generates its own lrl file for you.  If you want to add
tunables, place an `lrl.options` file in your testcase directory.  If you want
to supply your own lrl file, place an `lrl` file in your testcase directory.
Note that the test suite will append its own db name and directory options -
don't include those in your lrl file.

The test script has other information available to it in environment variables.
The following variables are available (changing them is not recommended):
Variable | Description
--- | ---
`TESTID`        | Randomly generated; all output will go into `test_${TESTID}`
`TESTDIR`       | `Path/to/source/tests/test_${TESTID}`<br>You can have the db run in a directory of your choosing by passing `TESTDIR=/tmp/somedirfortest` to `make`, ex: <br>`make testname TESTCASE=/tmp/somedirfortest`
`TESTCASE`      | Name of the testcase currently running.  You can assume `${TESTDIR}/${TESTCASE}.test/ is where your files are.`
`DBNAME`        | Name of the database used for the testcase: name of testcase with `$TESTID` appended to the end.
`DBDIR`         | Database files directory
`CDB2_OPTIONS`  | Option that `runit` has to pass to ``cdb2sql``
`CDB2_CONFIG`   | Path to `cdb2api` config file to use for tools other than ``cdb2sql``
`CLUSTER`       | Machines where the database cluster is set up. Database directory is `${DBDIR}` on all machines.

You can export your own `DBDIR`/`DBNAME` when running a single test. 

Export `CLUSTER` to a list of machines you want to use for a clustered test.  The
test suite will run as the user running the test suite.  It assumes you have
ssh and all the keys set up to allow password-less authentication between the
current machine and the machines in the cluster.  comdb2 needs to be installed
on the cluster machines.  Path to `${DBDIR}` must accessible to the current user.
The test directory and any prefix directories will be created with `mkdir -p`.

Log files are written to `tests/$TESTDIR/logs`.  Here's a list of log files produced:
File | Description
--- | ---
`$TESTDIR/logs/$DBNAME.init  `           | Output for creating the database
`$TESTDIR/logs/$DBNAME.db      `         | If running on local machine (`$CLUSTER` isn't set), the output from the database
`$TESTDIR/logs/$DBNAME.testcase   `      | Output from the runit script
`$TESTDIR/logs/$DBNAME.$MACHINE.copy   ` | Output from copying database to `$MACHINE` (when $CLUSTER is set) This is usually empty unless something went wrong.
`$TESTDIR/logs/$DBNAME.deb1.db   `       | If running clustered (`$CLUSTER` is set), the output from the database on `$MACHINE`

Running `make clean` in the 'tests' directory will remove all logs, all
databases, and all files produced by the running tests. It will NOT remove
anything on `${CLUSTER}` machines.

Running `make stop` will attempt to stop all databases for all tests.  You'll
only need to do this if a test case was interrupted. For a slower but more
reliable stoppage (kills any database with the same name as a test case,
unconditionally, locally and on `${CLUSTER}`), try `make kill`

To setup an individual test but not run it, you can do `make setup` from within
the testcase directory.

There's a timeout (default 5 minutes) on a runtime of each testcase, and 1
minute for setup.  If a test needs more time, `export TEST_TIMEOUT` and
`SETUP_TIMEOUT` in the test `Makefile`.

## Good ideas/practices

Show what commands you're running to make the output easier to debug, if
necessary.  If your test case produces files other than *.res or *.output,
supply a clean:: rule.  If you have a test that needs a binary, please add it
to tests/tools, not to your testcase.  tests/tools gets built before any tests
run. Share binaries between tests when possible.

## Internals

Make has a rule for `*.test`, which runs make recursively in that directory.
Each test's makefile includes `testcase.mk`.  `testcase.mk` exports variables (if
not set by user), and calls runtestcase.  runtestcase (when reading these
scripts remember that current working directory is the testcase directory when
the test runs - make switches to it before running the test make).  runtestcase
calls setup, calls the testcase's runit script, then calls unsetup.  It calls
runit only if setup worked.  setup is the most complicated test of the bunch -
it creates a database, copies it to a cluster if `CLUSTER` is set, starts the db.
Database (or ssh session) will create a pidfile when it starts.  unsetup just
kills the pids in the pidfiles if they match the database name.

Timeouts are done with `timeout` from `coreutils`.  The downside is that there's
no way to tell if a testcase failed, or was timed out.  Either way, it's
failure--we have allowed for timeout to be a reasonable amount of time for
a test to complete. 

You can use the same test machine to run multiple tests simultaneously--running 
any two tests in parallel is easy by doing:
```sh
  make test1; make test2
```

To run all the tests in the directory simply do:
```sh
  make -k -j 16
```

...sit back and wait for the results (-j seems a good paralellism level 
to get a 16 core system busy). When the tests will complete, a concise log
with information about success/failure/timeout can be found in `test_xxx/test.log`,
where `xxx` is the ID assigned for that test.

pmux by default runs on port 5105. It is used by both the server (`comdb2`) and 
client (`cdb2sql`) to discover the ports which a DB uses. We can run the testsuite
in a different port by passing parameter `PMUXPORT=xxx` to make:

```sh
  make -j 16 PMUXPORT=5555
```

NB: the default settings for `pmux` allow for `1000` DB ports to be assigned.
If you want to run more than `1000` DB tasks in one node, you will need 
to pass `-r` parameter to `pmux` or change the default range in pmux source. 

By default the testsuite running in cluster mode cleans up the db directory 
on the cluster nodes at the end of a successful test. If you want to keep
the db directory, please pass parameter `CLEANUPDBDIR=0` to make:
```sh
  make biginplace CLUSTER='node1 node2' CLEANUPDBDIR=0
```

If you want to cleanup all test directories run accross nodes of a cluster 
you can issue:
```sh
  make testclean CLUSTER='node1 node2'
```

## Running a comdb2 cluster in lxc containers

Here are some notes on how to setup lxc containers to run tests for clusters 
of comdb2 servers in those containers.

1.  Please install all necessary packages for `lxc`, ex in debian:
    ```sh
      apt-get install lxc apparmor lxcfs 
    ```

    If the above did *not* create a bridge for the networking to the container
    (for instance ubuntu creates `lxcbr0` when installing `lxc` package), 
    you can manually setup a bridge by adding in /etc/network/interfaces:
    ```
    auto br0
    iface br0 inet static
            address 192.168.1.1
            network 192.168.1.0
            netmask 255.255.255.0
            bridge_ports dummy0
            bridge_stp off
            bridge_maxwait 5
            bridge_fd 0
            pre-up /sbin/modprobe dummy
            iptables -t nat -A POSTROUTING -o wlp1s0 -j MASQUERADE
    ```

    and then bring `br0` up:
    ```sh
      ifconfig br0 up
    ```

    also you might need to start apparmor:
    ```sh
      /etc/init.d/apparmor start
    ```

2.  Create a container with a recent distribution release for instance debian stretch:
    ```sh
      lxc-create -n node1 -t debian -- -r stretch
    ```

3.  Change the config for `node1` to have `br0` (or `lxcbr0`) as interface for networking. In debian the config file resides for node1 we just created resides in `/var/lib/lxc/node1/config` and you should have it contain the following
    ```
    lxc.network.type = veth
    lxc.network.flags = up
    lxc.network.link = br0
    lxc.network.veth.pair = vethvm1
    lxc.network.ipv4 = 192.168.1.101/24
    lxc.network.ipv4.gateway = 192.168.1.1  #same as br0 address above
    lxc.network.hwaddr = 00:FF:AA:BB:CC:DD
    ```

    Now you can bring up the container to finish up the configuration:
    ```sh
      lxc-start -n node1
    ```

    Add `node1` to `/etc/hosts`
    ```
      192.168.1.101   node1
    ```

    Connect to the container as root:
    ```sh
      ssh root@node1
    ```

    If you don't know the container's root password, you can set it with: 
    ```sh
      lxc-attach -n node1 passwd
      ```
    (or clear it in: `/var/lib/lxc/node1/rootfs/etc/shadow`)

    Add user with same userid as what you use on host machine: 
    ```sh
      adduser userid
    ```

    Login as userid and create`.ssh` directory in `node1`:
    ```sh
      mkdir ~userid/.ssh
      chmod 755 ~userid/.ssh
    ```

    In the host create ssh keys via `ssh-keygen` and copy the public key onto 
    ```
      node1:~userid/.ssh/authorized_keys
    ```
    Make sure you can ssh from host to node1 without typing a password at this point.
    This is needed so we can copy the DB files with `copycomdb2` and to start the db
    without being prompted for passwords.

    Add in `node1` the FQDN (step 6 in README: in our case add node1) to comdb2 host config:
    ```sh
      mkdir -p /opt/bb/etc/cdb2/config/comdb2.d/
      vi /opt/bb/etc/cdb2/config/comdb2.d/hostname.lrl
    ```

    add current machine (`node1`) name:
    ```
    hostname node1
    ```

4.  At this time you might want to make copies of this container:
    ```sh
      lxc-copy -n node1 -N node2
      lxc-copy -n node1 -N node3
    ```

    Modify the config files for node2 to have a different ip addr and
    different veth, so in `/var/lib/lxc/node2/config`:
    ```
    lxc.network.veth.pair = vethvm2
    lxc.network.ipv4 = 192.168.1.102/24
    ```

    and do the same for `node3`. For `node2` and `node3` need to add node name to:
    `/opt/bb/etc/cdb2/config/comdb2.d/hostname.lrl`
    (so `node2` and `node3` respectively)

    Add `node1`, `node2` and `node3` to `/etc/hosts` all nodes as well as to containers' host.
    lxc-start all the containers, and make sure you can ssh from any one to the 
    others.

5.  To run tests in clusters residing in the above containers, launch the tests from the host machine: 
    ```sh
      export CLUSTER="node1 node2 node"
      make test1
    ```
    or
    ```sh
      make -k -j 16
    ```

## Running tests on AWS EC2 clusters

1.  Create an EC2 cluster using comdb2aws utilities.
    For example, to create a 3-node cluster called "TEST", you would use:
    ```sh
      comdb2aws create-cluster --cluster TEST --count 3
    ```

2.  Log onto any instance within the cluster if the host machine isn't in the cluster's security group. By default, a comdb2 AWS EC2 cluster disallows any non-SSH traffic from outside the cluster's security group. If you run step 1 on an EC2 instance, the instance will be automatically added to the cluster's security group.

3.  Run tests on the AWS EC2 cluster.
    For example, to run cdb2api test on cluster `TEST`, you would use:
    ```sh
      export CLUSTER='comdb2aws which -c TEST --pvtdns'
      make cdb2api
    ```

4.  (Optional) Destroy the AWS EC2 cluster when done.
    For example, to destroy cluster `TEST`, you would use:
    ```sh
      comdb2aws terminate-cluster TEST
    ```
