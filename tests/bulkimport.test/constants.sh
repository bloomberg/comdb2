vars="TESTCASE DBNAME DBDIR TESTSROOTDIR TESTDIR CDB2_OPTIONS CDB2_CONFIG "\
"SECONDARY_DBNAME SECONDARY_DBDIR SECONDARY_CDB2_CONFIG SECONDARY_CDB2_OPTIONS"
for required in $vars; do
	q=${!required}
	echo "$required=$q"
	if [[ -z "$q" ]]; then
		echo "$required not set" >&2
		exit 1
	fi
done

SRC_DBNAME=$DBNAME
DST_DBNAME=$SECONDARY_DBNAME
