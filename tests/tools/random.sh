#!/usr/bin/env bash

. ${TESTSROOTDIR}/tools/write_prompt.sh

function random
{
    [[ $debug == "1" ]] && set -x
    typeset max=${1:-1048576}
    typeset raw=0
    if [[ -f /dev/urandom ]]; then
        raw=$(( od -A n -t d -N 3 /dev/urandom ))
    else
        raw=$(( ( RANDOM * RANDOM ) ^ ( RANDOM * RANDOM) ))
    fi
    echo $(( raw % max ))
}
