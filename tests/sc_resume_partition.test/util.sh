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
