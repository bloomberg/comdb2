#!/bin/sh
if [ "$TOTAL" -gt "100" ] ; then 
    for i in `seq 1 80` ; do 
        echo -n "."  1>&2; 
        sleep 0.02 ; 
    done ; 
    echo
fi
