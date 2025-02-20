#!/bin/bash


LIST="a b c d e"

a=""

for p in $LIST; do
    a+="${p}.t"
done

a=`echo $a | sed 's/.$//'`

echo ${a}
