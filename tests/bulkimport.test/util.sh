source constants.sh
source ${TESTSROOTDIR}/tools/cluster_utils.sh

function restart_dst_db() {
	# Don't want db to come back up so fast that api can retry instead of failing
	local -r restart_delay_secs=10

	set +e

	for node in ${CLUSTER} ; do
		kill_restart_node ${node} ${restart_delay_secs} &
	done
	sleep 2
	wait_for_cluster

	set -e
}

function echo_err() {
	printf "%s\n" "$*" >&2;
}

function failexit() {
	rc=$1
	msg=$2

	if ((rc != 0)); then
		echo_err $msg
		exit $rc
	fi
}

function query_src_db() {
	query=$1
	cdb2sql ${SRC_CDB2_OPTIONS} $SRC_DBNAME default "$query"
}

function query_dst_db() {
	query=$1
	cdb2sql ${DST_CDB2_OPTIONS} $DST_DBNAME default "$query"
}

function set_src_tunable() {
	local tunable=$1 nodes
	if [[ -z "$CLUSTER" ]]; then nodes=$HOSTNAME; else nodes=$CLUSTER; fi

	for node in $nodes; do
		cdb2sql --host ${node} ${SRC_CDB2_OPTIONS} $SRC_DBNAME "exec procedure sys.cmd.send('${tunable}')"
	done
}

function set_dst_tunable() {
	local tunable=$1 nodes
	if [[ -z "$CLUSTER" ]]; then nodes=$HOSTNAME; else nodes=$CLUSTER; fi

	for node in $nodes; do
		cdb2sql --host ${node} ${DST_CDB2_OPTIONS} $DST_DBNAME "exec procedure sys.cmd.send('${tunable}')"
	done
}

function check_for_src_trace() {
	local -r trace="$1" timestamp="$2"
	echo "Checking for src trace. Trace is $trace. Timestamp is $timestamp"
	awk -v ts="${timestamp}" '$0 >= ts' ${TESTDIR}/logs/${SRC_DBNAME}* | grep "${trace}"
}

function wait_for_src_trace() {
	local -r trace="$1" timestamp="$2"
	while ! check_for_src_trace "$trace" "$timestamp"; do
		sleep .1
	done
}

function downgrade_src_db() {
	local master
	master=$(cdb2sql ${SRC_CDB2_OPTIONS} -tabs $SRC_DBNAME default 'SELECT host FROM comdb2_cluster WHERE is_master="Y"') || return 1
	cdb2sql --host ${master} ${SRC_CDB2_OPTIONS} $SRC_DBNAME "exec procedure sys.cmd.send('downgrade')" || return 1
	local new_master=${master}
	while [[ "${new_master}" == "${master}" ]]; do
		sleep 1
		new_master=$(cdb2sql ${SRC_CDB2_OPTIONS} -tabs $SRC_DBNAME default 'SELECT host FROM comdb2_cluster WHERE is_master="Y"') || return 1
	done
}

function query_in_loop() {
	query_func=$1
	query=$2
	wait_time=$3

	rc=0

	while true
	do
		$query_func "$query" > /dev/null
		rc=$?

		if (( rc != 0 ));
		then
			break
		fi

		sleep $wait_time
	done

	echo_err "ERROR returning $rc"
	return $rc
}

function get_cmp_command() {
	local op=$1 cmp_command

	if (( op == 0 )); then 
		cmp_command="comm -3"
	elif (( op == -1 )); then 
		cmp_command="comm -23"
	else 
		cmp_command="comm -13"
	fi

	echo "$cmp_command"
}

function check_dst_node_has_all_src_schemas() {
	local src_table=$1 dst_table=$2 hostname=$3 rc=0 cmp_command
	cmp_command=$(get_cmp_command 0)

	cdb2sql ${SRC_CDB2_OPTIONS} -tabs $SRC_DBNAME default \
		"select version, csc2 from comdb2_schemaversions where tablename='$src_table'" > src_schemas
	cdb2sql ${DST_CDB2_OPTIONS} -tabs $DST_DBNAME --host $hostname \
		"select version, csc2 from comdb2_schemaversions where tablename='$dst_table'" > dst_schemas

	missing_schemas=$($cmp_command <(sort src_schemas) <(sort dst_schemas))

	if [[ ! "$?" -eq "0" || ! -z $missing_schemas ]];
	then
		echo_err "Schema validation failed."
		echo_err "src schemas:"
		cat src_schemas
		echo_err "dst schemas:"
		cat dst_schemas

		rc=1
	fi

	return $rc
}

function check_dst_node_has_all_src_records() {
	local src_table=$1 dst_table=$2 hostname=$3 op=$4 rc=0 cmp_command
	cmp_command=$(get_cmp_command $op)

	cdb2sql ${SRC_CDB2_OPTIONS} -tabs $SRC_DBNAME default \
		"select * from $src_table" > src_records
	cdb2sql ${DST_CDB2_OPTIONS} -tabs $DST_DBNAME --host $hostname \
		"select * from $dst_table" > dst_records
	
	missing_records=$($cmp_command <(sort src_records) <(sort dst_records))

	if [[ ! "$?" -eq "0" || ! -z $missing_records ]];
	then
		echo_err "Record validation failed. Op $op"
		echo_err "src records"
		cat src_records
		echo_err "dst records:"
		cat dst_records

		rc=1
	fi

	return $rc
}

function verify_node() {
	local src_table=$1 dst_table=$2 hostname=$3 op=$4

	check_dst_node_has_all_src_schemas $src_table $dst_table $hostname && \
	check_dst_node_has_all_src_records $src_table $dst_table $hostname $op
}

function verify_import() {
	local nodes rc=0 src_table=$1 dst_table=$2 op=$3
	if [[ -z "$CLUSTER" ]]; then nodes=$HOSTNAME; else nodes=$CLUSTER; fi

	for node in $nodes; do
		verify_node $src_table $dst_table $node $op || rc=1
		if [[ ! $rc -eq 0 ]]; then break; fi
	done

	return $rc
}

function verify_lt() {
	verify_import $1 $2 "-1"
}

function verify_eq() {
	verify_import $1 $2 "0"
}
