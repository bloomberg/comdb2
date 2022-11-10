#!/bin/sh

dir=${PWD%/*}

cat <<- EOF > testdb.lrl
name testdb
dir ${dir}/testdb
logmsg.level debug
EOF
