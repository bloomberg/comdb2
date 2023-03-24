#!/usr/bin/env bash

# Print current epoch-time including milliseconds
function timems
{
    typeset t=${EPOCHREALTIME}
    seconds=${t%%\.*}
    rest=${t##*\.}
    echo "${seconds}${rest:0:3}"
}
