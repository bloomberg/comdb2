#!/usr/bin/env bash

cat $1 | sed "s/failed with rc 3 /failed with rc 304 /g"
