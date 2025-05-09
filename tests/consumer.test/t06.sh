source ${TESTSROOTDIR}/tools/runit_common.sh

tier="default"

set -ex

main() {
	cdb2sql ${CDB2_OPTIONS} ${DBNAME} ${tier} "drop table if exists t"
	cdb2sql ${CDB2_OPTIONS} ${DBNAME} ${tier} "create table t(i int)"

	sendtocluster "exec procedure sys.cmd.send('fail_to_create_default_cons 1')"
	if cdb2sql ${CDB2_OPTIONS} ${DBNAME} ${tier} "create default lua consumer t_cons on (table t for insert and update and delete)";
	then
		echo "FAIL: Should have failed to create consumer. Test may be buggy."
		return 1
	fi

	local proc_exists=$(cdb2sql ${CDB2_OPTIONS} --tabs ${DBNAME} ${tier} "select 1 from comdb2_procedures where name='t_cons' limit 1")
	if (( proc_exists == 1 ));
	then
		echo "FAIL: Procedure should not exist"
		return 1
	fi

	sendtocluster "exec procedure sys.cmd.send('fail_to_create_default_cons 0')"
	cdb2sql ${CDB2_OPTIONS} ${DBNAME} ${tier} "create default lua consumer t_cons on (table t for insert and update and delete)"

	proc_exists=$(cdb2sql ${CDB2_OPTIONS} --tabs ${DBNAME} ${tier} "select 1 from comdb2_procedures where name='t_cons' limit 1")
	if (( proc_exists != 1 ));
	then
		echo "FAIL: Procedure should exist."
		return 1
	fi
}

main
