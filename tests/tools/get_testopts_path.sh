#!/usr/bin/env bash

# take a string of the form abc_snapshot or abc_snapshot_generated.test and
# return full path of the testopts from which it was derived ex.
# abc.test/snapshot.testopts

TST=$1

for i in *.test/*.testopts ; do 
    echo $i | sed 's#\.testopts#_generated#g; s#/#_#g; s#\.test##g; s#$#.test#' | xargs echo $i ; 
done | grep $TST | cut -f1 -d' '

