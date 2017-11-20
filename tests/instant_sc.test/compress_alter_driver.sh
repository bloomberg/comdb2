#!/usr/bin/env bash

./compress_alter.sh $2 2>&1 | sed "s/.n_writeops_done=1/rows inserted=\'1\'/g"
