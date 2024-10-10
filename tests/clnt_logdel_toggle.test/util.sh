source ${TESTSROOTDIR}/tools/runit_common.sh

error() {
	err "Failed at line $1"
	exit 1
}
trap 'error $LINENO' ERR

logdel_switch_to_lfn() {
	local -r switch=$1

	if [[ "${switch}" == "ON" ]];
	then
		echo "-1"
	elif [[ "${switch}" == "OFF" ]];
	then
		echo "0"
	fi
}

logdel_lfn_to_switch() {
	local -r lfn=$1

	if (( lfn == -1 ));
	then
		echo "ON"
	elif (( lfn == 0 ));
	then
		echo "OFF"
	fi
}

get_logdel_status() {
	logdel_lfn_to_switch "$(cdb2sql -tabs ${CDB2_OPTIONS} "${DBNAME}" default "select value from comdb2_tunables where name='logdeletelowfilenum'")"
}

# Runs clnt session of form:
# <check logdel value>
# set logdelete off
# <check logdel value>
# sleep(sleeptime)
# set logdelete on
# <check logdel value>
#
# arg1 = sleep time
# arg2 = expected logdel value in 1st check ("ON" or "OFF") or emptystring to omit check
# arg3 = expected logdel value in 2nd check ("ON" or "OFF") or emptystring to omit check
# arg4 = expected logdel value in 3rd check ("ON" or "OFF") or emptystring to omit check
run_query() {
	trap 'error $LINENO' ERR
	local -r s_time=$1

 	local pre_chk_val mid_chk_val post_chk_val
	pre_chk_val="$(logdel_switch_to_lfn $2)"
	mid_chk_val="$(logdel_switch_to_lfn $3)"
	post_chk_val="$(logdel_switch_to_lfn $4)"
	readonly pre_chk_val mid_chk_val post_chk_val

	local pre_chk="" mid_chk="" post_chk=""
	local n_chk=0

	if [ -n "${pre_chk_val}" ];
	then
		pre_chk="select value from comdb2_tunables where name='logdeletelowfilenum' and value='${pre_chk_val}'"
		n_chk=$(( n_chk + 1 ))
	fi

	if [ -n "${mid_chk_val}" ];
	then
		mid_chk="select value from comdb2_tunables where name='logdeletelowfilenum' and value='${mid_chk_val}'"
		n_chk=$(( n_chk + 1 ))
	fi

	if [ -n "${post_chk_val}" ];
	then
		post_chk="select value from comdb2_tunables where name='logdeletelowfilenum' and value='${post_chk_val}'"
		n_chk=$(( n_chk + 1 ))
	fi

	res=$(cdb2sql ${CDB2_OPTIONS} "${DBNAME}" default - <<EOF
		${pre_chk}
		set logdelete off
		select 1
		${mid_chk}
		select sleep(${s_time})
		set logdelete on
		select 1
		${post_chk}
EOF
)
	n_chk_passed=$(echo $res | grep -o "(value=" | wc -l)
	(( n_chk == n_chk_passed ))
}
