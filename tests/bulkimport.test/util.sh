source constants.sh

function failexit() {
	rc=$1
	msg=$2

	if ((rc != 0)); then
		echo $msg
		exit $rc
	fi
}

function query_src_db() {
	query=$1
	cdb2sql ${CDB2_OPTIONS} $SRC_DBNAME default "$query" &> /dev/null
	return $?
}

function query_dst_db() {
	query=$1
	cdb2sql ${SECONDARY_CDB2_OPTIONS} $DST_DBNAME default "$query" &> /dev/null
	return $?
}

function verify_node() {
	src_table=$1
	dst_table=$2
	exp_records=$3
	exp_schemas=$4
	tier=$5
	rc=0

	res_records=$(cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME $tier \
	"select * from $dst_table")

	if [[ "$res_records" != "$exp_records" ]]; then
		echo "Mismatch -- src records: $exp_records; dst records: $res_records"
		rc=1
	fi

	res_schemas=$(cdb2sql ${SECONDARY_CDB2_OPTIONS} -tabs $DST_DBNAME $tier \
	"select version, csc2 from comdb2_schemaversions where tablename='$dst_table'")

	if [[ "$res_schemas" != "$exp_schemas" ]]; then
		echo "Mismatch -- src schemas: $exp_schemas; dst schemas: $res_schemas"
		rc=1
	fi

	return $rc
}

function verify_import() {
	src_table=$1
	dst_table=$2
	rc=0

	exp_records=$(cdb2sql ${CDB2_OPTIONS} -tabs $SRC_DBNAME default \
	"select * from $src_table")
	exp_schemas=$(cdb2sql ${CDB2_OPTIONS} -tabs $SRC_DBNAME default \
	"select version, csc2 from comdb2_schemaversions where tablename='$src_table'")

	if [[ -z "$CLUSTER" ]]; then
		verify_node "$src_table" "$dst_table" "$exp_records" "$exp_schemas" default
		rc=$?
	else
		for node in $CLUSTER; do
			verify_node "$src_table" "$dst_table" "$exp_records" "$exp_schemas" "--host $node"
			rc=$?
			if ((rc != 0)); then
				break
			fi
		done
	fi

	return $rc
}
