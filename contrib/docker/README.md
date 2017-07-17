# Comdb2 on Docker

## Prerequsites
1) Install [Docker Engine](https://docs.docker.com/engine/installation/)
2) *For using `make docker-cluster`* Install [Docker Compose](https://docs.docker.com/compose/install/)
3) Read these docs to see what everything is used for

## Dockerfiles

This folder contains several Dockerfiles which serve different functions in running Comdb2 on Docker.

File | Description
--- | ---
`Dockerfile.standalone` | Contains dependencies needed to run the database and the built source installed
`Dockerfile.dev` | Replaces the entrypoint in `Dockerfile.standalone` with one that spins up a database
`Dockerfile.build` | Contains all build dependencies to build source and output into `./build` folder for use by dependent images 
`Dockerfile.jdbc.build` | JDBC Driver builder

### Dependency Between Images

<table>
<tr>
    <td>Dockerfile.build</td>
    <td>=> Dockerfile.standalone</td>
    <td>=> Dockerfile.dev</td>
</tr>
<tr>
    <td></td>
    <td></td>
    <td>=> Dockerfile.cluster</td>
</tr>
</table>

## Using `make`
Docker image builds and spining up a Comdb2 cluster through Docker Compose can be done through `make`. At the root directory of the Comdb2 project, the following commands can be ran.

Command | Usage
--- | ---
`make docker-standalone` | Builds the `comdb2-standalone:<version>` Docker image
`make docker-dev` | Builds the `comdb2-dev:<version>` Docker image
`make docker-cluster` | Creates and runs a Comdb2 cluster using `docker-compose` after initial configuration. *See docker-cluster instructions*
`make docker-build` | Creates a build container and builds the source into the `contrib/docker/build/` folder
`make docker-clean` | Removes the `contrib/docker/build/` folder created in the `docker-build` process and runs `docker-compose down` to shut down and remove any running compose clusters

### Using `make docker-cluster`
The `make docker-cluster` command is a way to run an example Comdb2 cluster. As configured in `docker-compose.yml`, this command will create a 5 node Comdb2 cluster. There are a few setup steps to configure the host environment.

1) Run `make docker-cluster`. This will create the volume-linked folders on your host machine in `contrib/docker/volumes`. It is expected that this will fail to start because no databases have been created yet.

2) Create a Comdb2 Database called `testdb` in the `node1` folder created in `contrib/docker/volumes/`. Steps to create a database can be found [here](https://bloomberg.github.io/comdb2/example_db.html#the-slightly-longer-version). If you would like to modify the name of the database, also change the name in the `cluster-entrypoint.sh` file.

3) Open the LRL file (ex. `testdb.lrl`) in the folder. Change the `dir /some/path` to `dir /db`. Also add the following line to the end of the file `cluster nodes node1 node2 node3 node4 node5`.

4) Copy all the database files that have just been created in the `node1` folder into all the other `nodeX` folders.

5) Run `make docker-cluster` and the cluster should start running

To open a shell in one of the nodes in the cluster, run `docker exec -it nodeX /bin/bash` to open a `bash` shell on that node.


## Misc. Files

There are several additional files that are used to support Docker operations.

File | Description
--- | ---
`docker-compose.yml` | This is an example Docker Compose file for running a Comdb2 cluster
`cluster-entrypoint.sh` | Entrypoint script for the example cluster
`docker-dev-entrypoint.sh` | Entrypoint script for `comdb2-dev:<version>` image
