#!/bin/bash

#usage
if [[ $# != 4 ]]; then
    echo "$0 usage: <source> <cfile> <hfile> <template>"
    exit 1
fi

#executables
s_awk=gawk
s_gen=${SRCHOME}/berkdb/dist/gen_rec_endian.awk
#if [[ ! -x $s_awk ]]; then
#    s_awk=$(whence awk)
#fi

#temp files
t_cfl=/tmp/__db_s.$$
t_hdr=/tmp/__db_h.$$
t_tmp=/tmp/__db_t.$$

#trap directives
trap 'rm -f $t_cfl $t_hdr $t_tmp ; exit 1' 0 1 2 3 13 15
trap 'rm -f $t_cfl $t_hdr $t_tmp ; exit 0' 0

#grab arguments
a_src=$1
a_cfl=$2
a_hdr=$3
a_tmp=$4

#generate tmp files
$s_awk -f $s_gen                                        \
    -v source_file=$t_cfl                               \
    -v header_file=$t_hdr                               \
    -v template_file=$t_tmp                             < $a_src

#move header into place
#cmp $t_hdr $a_hdr > /dev/null 2>&1 ||
(
    echo "Building $a_hdr"
    rm -f $a_hdr
    cp $t_hdr $a_hdr
    chmod 444 $a_hdr
)

#move cfile into place
#cmp $t_cfl $a_cfl > /dev/null 2>&1 ||
(
    echo "Building $a_cfl"
    rm -f $a_cfl
    cp $t_cfl $a_cfl
    chmod 444 $a_cfl
)

#move template file into place
#cmp $t_tmp $a_tmp > /dev/null 2>&1 ||
(
    echo "Building $a_tmp"
    rm -f $a_tmp
    cp $t_tmp $a_tmp
    chmod 444 $a_tmp
)

