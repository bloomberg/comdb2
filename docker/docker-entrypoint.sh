#!/bin/bash
set -e

# To address if first arg is `-h` or `--some-option`
if [ "${1:0:1}" = '-' ]; then
    set -- comdb2 "$@"
fi

if [ "$1" = 'comdb2' ]; 
then
    # Start portmux
    pmux -l

    mkdir -p "$CDB2_DATA"
    chmod 700 "$CDB2_DATA"
    chown -R comdb2 "$CDB2_DATA"

    CDB2_LRL="$CDB2_DATA"/"$CDB2_DB".lrl

    echo "name ${CDB2_DB}"   > "$CDB2_LRL"
    echo "dbnum 1"          >> "$CDB2_LRL"
    echo "dir ${CDB2_DATA}" >> "$CDB2_LRL"

    chown -R comdb2 "$CDB2_LRL"

    eval "gosu comdb2 comdb2 $CDB2_DB -init $CDB2_DATA"

    eval "gosu comdb2 comdb2 $CDB2_DB -lrl $CDB2_LRL"
fi

eval "$@"
