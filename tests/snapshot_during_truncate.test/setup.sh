#!/usr/bin/env bash
bash -n "$0" | exit 1

setup_t00() {
	local trunc_lsn_file
	trunc_lsn_file=$(( $(cdb2sql --tabs ${CDB2_OPTIONS} ${DBNAME} default "select logfile from comdb2_cluster") + 2 ))
	readonly trunc_lsn_file

	sed -i -e "s/<trunc_lsn>/$(( trunc_lsn_file )):0/g" t00.req
}

setup_t01() {
	local trunc_lsn trunc_lsn_file trunc_lsn_offset
	trunc_lsn=$(cdb2sql --tabs ${CDB2_OPTIONS} ${DBNAME} default "select logfile, logoffset from comdb2_cluster")
	trunc_lsn_file=$(cut -f 1 <<< ${trunc_lsn})
	trunc_lsn_offset=$(cut -f 2 <<< ${trunc_lsn})
	readonly trunc_lsn trunc_lsn_file trunc_lsn_offset

	sed -i -e "s/<trunc_lsn>/${trunc_lsn_file}:${trunc_lsn_offset}/g" t01.req
}
