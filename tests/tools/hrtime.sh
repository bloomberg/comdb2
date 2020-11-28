#!/usr/bin/env bash

function timenano
{
    echo $(date +%s.%N)
}

function timems
{
    typeset x=$(timenano)
    typeset secs=${x%*\.*}
    typeset nsecs=${x#*\.*}
    echo "$secs${nsecs:0:3}"
}


