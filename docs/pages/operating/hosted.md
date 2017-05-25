---
title: Setting up hosted services
keywords:
tags:
sidebar: mydoc_sidebar
permalink: hosted.html
---

One topic that wasn't addressed in the [Setting up clusters](clusters.html) section
is how to manage multiple running databases.  We provide some tools for managing
Comdb2 databases running on AWS EC2 instances, and on your own hardware.

## Comdb2 on AWS

The contrib/comdb2aws directory in the source distribution contains a set of scripts to make it easy to 
stand up Comdb2 clusters on AWS.  Running `sudo make install` in that directory install the required scripts.
The only dependency is the AWS command line tools.

Comdb2aws uses [Supervisor](http://supervisord.org/) to manage databases.

### Prerequisites

* AWS Command Line Interface

[AWSCLI](http://docs.aws.amazon.com/cli/latest/userguide/installing.html) must be installed beforehand.

### Installing Comdb2aws

Run `sudo make install` in the `contrib/comdb2aws` directory. The scripts are installed in `/opt/bb/bin`.
You can change the installation directory by setting `$PREFIX`. You can verify the installation by 
running `comdb2aws -h`.

### Creating an AWS EC2 Comdb2 Cluster

Use `comdb2aws create-cluster` to create a cluster.

For example, to create a cluster called COLDPLAY with 4 nodes, run:

`comdb2aws create-cluster --cluster COLDPLAY --count 4`

The command will launch an ssh-agent and import the ssh key of the cluster to
the agent, if no existing ssh-agent is detected. However, the ssh-agent is not
be accessible from your login shell after finishing the command. To use the
ssh-agent in your login shell, you can use the following command instead:

`source comdb2aws create-cluster --cluster COLDPLAY --count 4`

The cluster will have its own dedicated security group. By default, the security
group disallows non-SSH (22) traffic from outside the security group. If
your host is an EC2 instance but has reached security group limit, or it is not
an EC2 instance (e.g., your laptop), you would need to log on to a node from
the cluster to access pmux port and database ports.

### Listing the Nodes in the Cluster

Run `comdb2aws which` to list all nodes in the cluster.

For example, to list all COLDPLAY nodes, you would use:

`comdb2aws which --cluster COLDPLAY`

For the create example above, you would get something like this:

```
$ comdb2aws which --cluster COLDPLAY
       Name                 ID           State        Public IP            Private DNS
       COLDPLAY-1           i-12345678   running      123.45.67.89         ip-123-45-67-89.ec2.internal
       COLDPLAY-2           i-12345678   running      123.45.67.89         ip-123-45-67-89.ec2.internal
       COLDPLAY-3           i-12345678   running      123.45.67.89         ip-123-45-67-89.ec2.internal
       COLDPLAY-4           i-12345678   running      123.45.67.89         ip-123-45-67-89.ec2.internal
```

### Deploying a Database

Use "comdb2aws deploy" to deploy a new database. The database will be registered under Supervisord.

For example, to deploy a database called yellowdb to the COLDPLAY cluster we created above, you would use:

`comdb2aws deploy-database -d yellowdb -c COLDPLAY`

The command generates database configuration for you. Therefore, to query the database, you would simply use:

`cdb2sql yellowdb default "select now()"`

### Managing Databases using Supervisord Web Interface

By default, Supervisord web server listens on port `9001` on localhost.
To access the web interface from outside a cluster, you could do a local port forwarding from
your host to the cluster.

For example, to forward port 80 on localhost to port 9001 on the COLDPLAY cluster, you would use:

```shell
ssh -L 80:localhost:9001 comdb2@123.45.67.89
```

Now, Pointing your browser at `http://localhost` would forward you to `http://123.45.67.89:9001`
via a secure connection. You would be able to see yellowdb we just created in the web interface:

[supervisor-web-interface](images/supervisor-web-interface.gif)


### Taking backups

Use "comdb2aws backup-database" to take a database backup.

To take a backup for yellowdb remotely, you can use:

`comdb2aws backup-database -c COLDPLAY -d yellowdb`

The command would take a backup from a random node in the COLDPLAY cluster.

To take a backup for yellowdb and store it locally, you would need the `--dl` option:

`comdb2aws backup-database -c COLDPLAY -d yellowdb --dl`

The command would take a backup and save it under the current working directory.

comdb2aws can save backups in an S3 bucket with the `-b` option. The specified
S3 bucket must already exist.  For example, to save a backup in an S3 bucket called
comdb2backups, you would use:

`comdb2aws backup-database -c COLDPLAY -d yellowdb -b comdb2backups`

### Listing the Backups in an S3 bucket

Use "comdb2aws list-backups-in-s3". For example, to list all the backups in comdb2backups bucket, you can run:

`comdb2aws list-backups-in-s3 -b comdb2backups`

For this example you'd see something like this:

```
$ comdb2aws list-backups-in-s3 -b comdb2backups
2016-12-07 23:09:34 yellowdb/COLDPLAY/20161207T2309Z386
```

The string in the 3rd column (<db>/<cluster>/<time>) is the backup ID.

### Restoring a Database from an S3 Backup

Use `comdb2aws get-backup-from-s3`. To restore yellowdb locally,

`comdb2aws get-backup-from-s3 -b comdb2backups -a yellowdb/COLDPLAY/20161207T2309Z386 -r localhost`

and then bring up the database.

You can also restore yellowdb to a cluster. To restore yellowdb on COLDPLAY cluster, first you would stop yellowdb on 
COLDPLAY by running:

`comdb2aws stop-database -c COLDPLAY -d yellowdb`

then restore it by running:

`comdb2aws get-backup-from-s3 -b comdb2backups -a yellowdb/COLDPLAY/20161207T2309Z386 -r COLDPLAY`

then start yellowdb by running:

`comdb2aws start-database -c COLDPLAY -d yellowdb`

### Creating Your Own Comdb2 Cluster AMI (Recommended)

Your custom AMI must include the following software packages:

- Comdb2
- Coreutils
- NTP
- Curl
- Cloud-utils

By default, Comdb2aws searches Comdb2 binaries under /opt/bb/. If the binaries are installed
somewhere else, you would run "PREFIX=<your_path> make install" to change the prefix.

To create a cluster using your AMI, you would use:

`comdb2aws create-cluster --cluster <name> --count <#> --image-id <id>`

Comdb2aws uses 'comdb2' role by default. If you had changed the database user,
you would need to export the correct SSH user before running comdb2aws.

For example, to tell comdb2aws to use 'bob' as the SSH username, you would run:

`export SSHUSER='bob'`

## Comdb2 on your machines

Installing Comdb2 via a deb/rpm package, or following post-install instructions
sets up an instance of supervisord.  It can be used to manage database installs
on a machine.

### supervisor

An instance of supervisord runs with a config file (default location) of
`/opt/bb/etc/supervisord_cdb2.conf`.  You can use existing supervisor tools
like supervisorctl to talk to this instance.  The default configuration
brings up an http server on 9001, listening on localhost only.

Databases configured with comdb2admin (see below) will be started/stopped
as a unit when the supervisor instance starts/stops.

### comdb2admin

comdb2admin is a small convenience tool for managing databases on a machine. Run
it with no arguments to see the available options.

#### Creating a new database

A new database can be created with `comdb2admin create dbname`.  This command
will create a new database in a default location (`/opt/bb/var/cdb2/dbname` by
default) and start it.  The database is automatically configured to start
when supervisor comes up.

#### Starting and stopping a database

`comdb2admin start dbname` will start a database stopped with 
`comdb2admin stop dbname`.  A stopped database will still restart as 
supervisor is started (eg: via systemctl or at machine boot).

#### Destroying a database

`comdb2admin drop dbname` will stop a database and delete it.  It'll also
be removed from supervisor configs, and will not run when supervisor runs.

#### See what's running

`comdb2admin status` will show configured databases and their status.

#### Admin commands 

`send dbname cmd ...` is equivalent to a much more verbose

`cdb2sql dbname "exec stored procedure sys.cmd.send('cmd ...')"` (see
[operational commands](op.html)).
