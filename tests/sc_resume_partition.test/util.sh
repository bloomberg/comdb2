restart_cluster() {
	local -r restart_delay_secs=10

	set +e
	if [ -z $CLUSTER ];
	then
		kill_restart_node $1 ${restart_delay_secs} &
	else
		for node in ${CLUSTER} ; do
			kill_restart_node ${node} ${restart_delay_secs} &
		done
	fi

	sleep 2
	wait_for_cluster
	set -e
}

downgrade() {
	local master=$1
	cdb2sql --host ${master} ${CDB2_OPTIONS} ${dbnm} ${tier} "exec procedure sys.cmd.send('downgrade')"

        local new_master=${master}
        while [[ "${new_master}" == "${master}" ]]; do
            sleep 1
            new_master=$(get_master)
        done
}

num_outstanding_scs() {
	cdb2sql --tabs ${CDB2_OPTIONS} ${dbnm} ${tier} "select count(*) from comdb2_sc_status where status!='COMMITTED'"
}

wait_for_outstanding_scs() {
	local num_scs

	num_scs=$(num_outstanding_scs)
	while (( num_scs > 0 )); do
		sleep 1
		num_scs=$(num_outstanding_scs)
	done
}

create_table() {
	local -r tbl_name=$1

	local starttime
	starttime=$(get_timestamp 120)
	#cdb2sql ${CDB2_OPTIONS} ${dbnm} ${tier} "create table ${tbl_name}(i int, j int)"
	if (( op_for_creating_partitioned_table == CREATE )); then
	        cdb2sql ${CDB2_OPTIONS} ${dbnm} ${tier} "create table ${tbl_name}(a int) partitioned by time period 'daily' retention ${num_shards} start '${starttime}'"
	elif (( op_for_creating_partitioned_table == ALTER )); then
	        cdb2sql ${CDB2_OPTIONS} ${dbnm} ${tier} "create table ${tbl_name}(a int)"
	        cdb2sql ${CDB2_OPTIONS} ${dbnm} ${tier} "alter table ${tbl_name} partitioned by time period 'daily' retention ${num_shards} start '${starttime}'"
	else
		echo "Don't recognize op for creating partitioned table '${op_for_creating_partitioned_table}'"
	fi
}

insert_records_into_table() {
	local -r tbl_name=$1 num_shards=$2 num_records_per_shard=$3
	for i in $(seq 0 1 $((num_shards-1)));
	do
		local shard
		shard=$(cdb2sql --tabs ${CDB2_OPTIONS} ${dbnm} ${tier} "select shardname from comdb2_timepartshards limit 1 offset ${i}")
	  	for (( i=1; i<=${num_records_per_shard}; i+=10000 )); do
	      		local min="${i}"
			local max="$(( ((${i} + 9999) < ${num_records_per_shard}) ? (${i} + 9999) : ${num_records_per_shard}))"
	      		cdb2sql ${CDB2_OPTIONS} ${DBNAME} ${tier} "insert into '${shard}' select * from generate_series(${min}, ${max})"
	      		#cdb2sql ${CDB2_OPTIONS} ${DBNAME} ${tier} "insert into '${tbl_name}' select 1, * from generate_series(${min}, ${max})"
	  	done
	done
}

trigger_resume() {
	local -r master=$1 resume_trigger=$2
	if (( resume_trigger == CRASH )); then
		restart_cluster ${master}
	elif (( resume_trigger == DOWNGRADE )); then
		downgrade ${master}
	else
		echo "FAIL: expected resume_trigger to be one of: CRASH, DOWNGRADE"
		return 1
	fi
}
