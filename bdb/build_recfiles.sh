#!/bin/bash

if [[ $# -ne 3 ]]; then
        echo $*
        echo "$0 usage: srcfile BDBLIBDIR BERKDBLIBDIR"
        exit 1
fi

SRC=$1
BDBLIB=$2
BERKDB=$3
GENREC=${BERKDB}/dist/gen_rec_endian.awk
GENINC=${BERKDB}/dist/gen_inc.awk

e_gawk=gawk

> ${BDBLIB}/llog_auto.c
> ${BDBLIB}/llog_auto.h
> ${BDBLIB}/template
> ${BDBLIB}/extern.list
> ${BDBLIB}/llog_extern.h
> ${BDBLIB}/int.list
> ${BDBLIB}/llog_int.h

[[ $? -ne 0 ]] && exit 1
$e_gawk -f ${SRCHOME}/berkdb/dist/gen_rec_endian.awk \
    -v source_file=${BDBLIB}/llog_auto.c \
    -v header_file=${BDBLIB}/llog_auto.h \
    -v template_file=${BDBLIB}/template < ${BDBLIB}/${SRC}

$e_gawk -f ${SRCHOME}/berkdb/dist/gen_inc.awk \
    -v e_dfile=${BDBLIB}/extern.list   \
    -v e_pfile=${BDBLIB}/llog_extern.h \
    -v i_dfile=${BDBLIB}/int.list      \
    -v i_pfile=${BDBLIB}/llog_int.h < ${BDBLIB}/llog_auto.c
