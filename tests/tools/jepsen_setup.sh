#!/usr/bin/env bash

export CDB2JDBC_STATEMENT_QUERYEFFECTS=1
export CDB2JDBC_VERIFY_RETRY=0
export LEIN="${LEIN:=$TESTSROOTDIR/tools/lein}"

if [[ -n "$COMDB2_LEIN_JVM_OPTS" ]]; then
    export LEIN_JVM_OPTS="$COMDB2_LEIN_JVM_OPTS"
fi

if [[ -n "$COMDB2_LEIN_HTTP_PROXY" ]]; then
    export http_proxy=${COMDB2_LEIN_HTTP_PROXY}
fi

if [[ -n "$COMDB2_LEIN_HTTPS_PROXY" ]]; then
    export https_proxy=${COMDB2_LEIN_HTTPS_PROXY}
fi

