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

function check_node_schema() {
	src_table=$1
	dst_table=$2
	hostname=$3

	incorrect_schemas=$(cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME --host $hostname \
	"select version, csc2 from comdb2_schemaversions except select version,csc2 from LOCAL_$SRC_DBNAME.comdb2_schemaversions union select version, csc2 from LOCAL_$SRC_DBNAME.comdb2_schemaversions except select version,csc2 from comdb2_schemaversions")

	if [[ ! "$?" -eq "0" || ! -z $incorrect_schemas ]];
	then
		echo_err "Schema validation failed"
		return 1
	fi

	return 0
}

function check_node_table() {
	src_table=$1
	dst_table=$2
	hostname=$3
	dst_table_should_be_superset_of_src_table=$4

	if (( dst_table_should_be_superset_of_src_table == 0 ));
	then
		incorrect_records=$(cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME --host $hostname \
		"select * from LOCAL_$SRC_DBNAME.$src_table except select * from $dst_table union select * from $dst_table except select * from LOCAL_$SRC_DBNAME.$src_table")
	else
		incorrect_records=$(cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME --host $hostname \
		"select * from LOCAL_$SRC_DBNAME.$src_table except select * from $dst_table")
	fi

	if [[ ! "$?" -eq "0" || ! -z $incorrect_records ]];
	then
		echo_err "Record validation failed"
		return 1
	fi

	return 0
}

function verify_node() {
	src_table=$1
	dst_table=$2
	hostname=$3
	dst_table_should_be_superset_of_src_table=$4

	# Workaround fdb bug, for now
	cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME --host $hostname \
		"select * from LOCAL_$SRC_DBNAME.$src_table limit 1" > /dev/null
	cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME --host $hostname \
		"select * from LOCAL_$SRC_DBNAME.comdb2_schemaversions limit 1" > /dev/null

	check_node_schema $src_table $dst_table $hostname && \
	check_node_table $src_table $dst_table $hostname \
	$dst_table_should_be_superset_of_src_table
}

function verify_import() {
	local nodes
	rc=0
	src_table=$1
	dst_table=$2
	dst_table_should_be_superset_of_src_table=$3

	if [[ -z "$CLUSTER" ]]; then nodes=$HOSTNAME; else nodes=$CLUSTER; fi

	for node in $nodes; do
		verify_node "$src_table" "$dst_table" "$node" \
		$dst_table_should_be_superset_of_src_table

		rc=$?
		if ! [ $rc -eq 0 ]; then break; fi
	done

	return $rc
}

function verify() {
	rc=$1
	src_tbl=$2
	dst_tbl=$3
	if [[ -z "$4" ]]; then superset=0; else superset="$4"; fi

	if (( rc == 0 ));
	then
		verify_import $src_tbl $dst_tbl $superset
		rc=$?
	fi

	return $rc
}
