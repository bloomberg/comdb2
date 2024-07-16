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

function verify_equal() {
	exp=$1
	res=$2
	what=$3

	rc=0
	
	if [[ "$res" != "$exp" ]]; then
		echo_err "Mismatch in $what -- src: $exp; dst : $res"
		rc=1
	fi

	return $rc
}

function verify_records_are_superset() {
	subset=$1
	superset=$2
	rc=0

	for i in $subset;
	do
		echo "$superset" | grep $i
		rc=$?

		if (( rc != 0 )); then
			echo_err "Failed to find src record $i in res records $superset"
			break
		fi

	done

	return $rc
}

function verify_node() {
	src_table=$1
	dst_table=$2
	exp_records=$3
	exp_schemas=$4
	tier=$5
	verify_superset=$6
	rc=0

	res_records=$(cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME $tier \
	"select * from $dst_table")
	res_schemas=$(cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME $tier \
	"select version, csc2 from comdb2_schemaversions where tablename='$dst_table'")

	if (( verify_superset == 1 ));
	then
		verify_records_are_superset "$exp_records" "$res_records"
	else
		verify_equal "$exp_records" "$res_records" "records"
	fi

	$(exit $?) && verify_equal "$exp_schemas" "$res_schemas" "schemas"

	return $?
}

function verify_import() {
	local nodes res_is_superset
	rc=0
	src_table=$1
	dst_table=$2
	exp_records=$3

	if [[ -z $exp_records ]]; then res_is_superset=0; else res_is_superset=1; fi
	if [[ -z "$CLUSTER" ]]; then nodes=$HOSTNAME; else nodes=$CLUSTER; fi

	exp_records=${exp_records:=$(cdb2sql ${CDB2_OPTIONS} -tabs $SRC_DBNAME default "select * from $src_table")}
	exp_schemas=$(cdb2sql ${CDB2_OPTIONS} -tabs $SRC_DBNAME default "select version, csc2 from comdb2_schemaversions where tablename='$src_table'")

	for node in $nodes; do
		verify_node "$src_table" "$dst_table" "$exp_records" "$exp_schemas" "--host $node" $res_is_superset

		rc=$?
		if ! [ $rc -eq 0 ]; then break; fi
	done

	return $rc
}

function verify() {
	rc=$1
	src_tbl=$2
	dst_tbl=$3
	exp_records=$4

	if (( rc == 0 ));
	then
		verify_import $src_tbl $dst_tbl "$exp_records"
		rc=$?
	fi

	return $rc
}
