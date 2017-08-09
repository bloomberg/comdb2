#!/bin/bash

# Start pmux
pmux -l

# Start DB
comdb2 --lrl /db/testdb.lrl testdb
