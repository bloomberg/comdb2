source constants.sh

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

function query_src_db_opts() {
	query=$1
	opts=$2

	cdb2sql ${CDB2_OPTIONS} ${opts} $SRC_DBNAME default "$query"
}

function query_dst_db_opts() {
	query=$1
	opts=$2

	cdb2sql ${SECONDARY_CDB2_OPTIONS} ${opts} $DST_DBNAME default "$query"
}

function query_src_db() {
	query_src_db_opts "$1"
}

function query_dst_db() {
	query_dst_db_opts "$1"
}

function query_in_loop() {
	query_func=$1
	query=$2
	wait_time=$3

	rc=0

	while true
	do
		$query_func "$query"
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

function verify_node() {
	src_table=$1
	dst_table=$2
	hostname=$3
	verify_superset=$4

	incorrect_schemas=$(cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME --host $hostname "select version, csc2 from comdb2_schemaversions except select version,csc2 from LOCAL_$SRC_DBNAME.comdb2_schemaversions")

	incorrect_schemas="$incorrect_schemas$(cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME --host $hostname "select version, csc2 from LOCAL_$SRC_DBNAME.comdb2_schemaversions except select version,csc2 from comdb2_schemaversions")"

	incorrect_records=$(cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME --host $hostname "select * from LOCAL_$SRC_DBNAME.$src_table except select * from $dst_table")

	if (( verify_superset == 0 ));
	then
		incorrect_records="$incorrect_records$(cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME --host $hostname "select * from $dst_table except select * from LOCAL_$SRC_DBNAME.$src_table")"
	fi

	[[ -z $incorrect_records && -z $incorrect_schemas ]]
}

function verify_import() {
	local nodes
	rc=0
	src_table=$1
	dst_table=$2
	res_is_superset=$3

	if [[ -z "$CLUSTER" ]]; then nodes=$HOSTNAME; else nodes=$CLUSTER; fi

	for node in $nodes; do
		verify_node "$src_table" "$dst_table" "$node" $res_is_superset

		rc=$?
		if ! [ $rc -eq 0 ]; then break; fi
	done

	return $rc
}

function verify() {
	rc=$1
	src_tbl=$2
	dst_tbl=$3
	superset=$4

	if (( rc == 0 ));
	then
		verify_import $src_tbl $dst_tbl $superset
		rc=$?
	fi

	return $rc
}
