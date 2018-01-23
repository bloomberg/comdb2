#!/bin/sh
if [ "$TOTAL" -gt "100" ] ; then 
    for i in `seq 1 79` ; do 
        echo -n "."  1>&2; 
        sleep 0.02 ; 
    done ; 
    echo "." 1>&2;
fi
