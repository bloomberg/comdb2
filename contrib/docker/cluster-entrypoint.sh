#!/bin/bash
#
# Entrypoint for a cluster node. The database and its lrl have already been
# placed in /db by the init service; here we just start pmux and the server.
set -e

# The client first queries pmux to find out which port the database is
# listening on.
pmux -l

exec comdb2 --lrl /db/testdb.lrl testdb
