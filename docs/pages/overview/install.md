---
title: Installing Comdb2
keywords: code
tags:
sidebar: mydoc_sidebar
permalink: install.html
---

Comdb2 has been running in production on x86 Linux, PowerPC AIX, and SPARC Solaris systems.  This guide is written with Linux in mind.  Admin commands for other OSes will be different, but database commands will remain the same.

## Installing from source

You can pull the latest source from [GitHub](https://github.com/bloomberg/comdb2).  

```
git clone https://github.com/bloomberg/comdb2
cd comdb2
```

Our development policy is to tag each release.  Major releases are branched.  Building
from the latest commit in the latest branch is a good bet.  Master is for bleeding edge
development.

### Dependencies

You'll need to install a few dependencies first. It's not a big list.  Exact names will depend on your distribution type and version.  Dependencies for some major distributions:

|Distro          | Dependencies |
|----------------|--------------|
|  Ubuntu 16.04, 16.10 | `sudo apt-get install -y build-essential bison flex libprotobuf-c-dev libreadline-dev libsqlite3-dev libssl-dev libunwind-dev libz1 libz-dev make gawk protobuf-c-compiler uuid-dev liblz4-tool liblz4-dev libprotobuf-c1 libreadline6 libsqlite3-0 libuuid1 libz1 tzdata ncurses-dev tcl bc`
| CentOS 7  | `sudo yum install -y gcc gcc-c++ protobuf-c libunwind libunwind-devel protobuf-c-devel byacc flex openssl openssl-devel openssl-libs readline-devel sqlite sqlite-devel libuuid libuuid-devel zlib-devel zlib lz4-devel gawk tcl epel-release lz4 which`

### Building

Just run ```make```.

### Installing

```sudo USER='username' make install```

To make testing out Comdb2 easier, ```make install``` will use the value of ```$USER``` to set ownership of log/configuration directories.  For a personal/development install ```sudo USER=$(whoami) make install``` is a good option. For database deployments, create a dedicated user.

Comdb2 installs by default rely on a small program called ```pmux``` to deal out port numbers to databases. Port assignments are dynamic.  This is very useful if you run large numbers of databases and don't want to manage port assignment yourself.  You'll need to start this program on system startup.  For systemd-based systems, we provide
a pmux.service file. If you're going to be running databases as the current user, you're all set.  To user a dedicated user, change the ```User=``` line in ```tools/pmux/pmux.service``` To use it:

```
sudo cp tools/pmux/pmux.service /etc/systemd/system
sudo systemctl daemon-reload
sudo systemctl enable pmux
sudo systemctl start pmux
```

## Installing from a package

Packages can be built from source with `make deb-current` on Debian-derived systems, or `make rpm-current` on Redhat-derived systems.

Installing a package creates a comdb2 user and group and installs pmux and cdb2sockpool as systemd-managed services.  Log/data directories are set up with read/write permissions for the `comdb2` user.  Databases should be created and run as `comdb2`.  As a convenience, installing from a package also starts an instance of [supervisord](http://supervisord.org/) that can be used to manage running databases.  A simple script called `comdb2admin` is installed to make it easier to start/create databases through supervisord.

## Installation directories

Installing (from source or a package) creates a directory structure like this:

```
/opt/bb
├── bin
│   ├── cdb2_dump
│   ├── cdb2_printlog
│   ├── cdb2sql
│   ├── cdb2_stat
│   ├── cdb2_verify
│   ├── comdb2
│   ├── comdb2ar
│   ├── comdb2dumpcsc
│   ├── copycomdb2
│   └── pmux
│   └── comdb2admin
├── etc
│   └── cdb2
│       └── config
│           ├── comdb2.d
│           └── comdb2.lrl
├── include
│   └── cdb2api.h
├── lib
│   ├── libcdb2api.a
│   ├── libcdb2api.so
│   └── libcdb2protobuf.a
│   └── libcdb2protobuf.so
└── var
    └── cdb2
        ├── databases
        └── logs

```

A quick overview:

| Directory  | Notes |
|------------|-------|
| ```bin/``` | Programs.  ```comdb2``` is the main Comdb2 binary.  Other programs are helpers that allow copying/backing up/examining/operating databases.|
| ```etc/cdb2/config/comdb2.lrl```   | Global database tunables, applies to all databases |
| ```etc/cdb2/config/comdb2.d```     | Global database config files, settings in all *.lrl files in this directory apply to all databases |
| ```include/``` and ```lib/```        | headers and libraries |
| ```var/cdb2/databases/``` | Default location for databases. Every database gets a subdirectory at create time. |
| ```var/cdb2/logs/``` | Default location for database informational log files |
