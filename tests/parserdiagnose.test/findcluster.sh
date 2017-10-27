#!/usr/bin/env bash

dbname=$1
if [[ -z $dbname ]] ; then
    echo dbname missing
    exit 1
fi

# Setup all the tables this test case needs :-
# First drop the table (its ok if this fails,
# the table probably doesn't exist).
# Then add the table.
node=`where $dbname | grep reads | awk '{print $9;}'`
if [[ $node ]]; then
  echo `bbhost -m $node`
else
  echo 'local'
fi
