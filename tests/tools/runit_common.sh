#!/usr/bin/env bash
# Common bash functions across many of the runit scripts


# exit after displaying error message
failexit()
{
    echo "Failed $@" | tee ${DBNAME}.failexit # runtestcase script looks for this file
    exit -1
}


# assert expected value in $1 is the same as target in $2, optional comment in $3
# assertres (expected, target, comment)
assertres ()
{
    if [[ $# != 2 ]] && [[ $# != 3 ]] ; then 
        failexit "Expecting 2 (opt 3) parameters but instead was passed $#"
    fi
    local expected=$1
    local target=$2
    local comment=${3:+"($3)"}
    if [ "$expected" != "$target" ] ; then
        failexit "Expected is '$expected' but should be '$target' $comment"
    fi
}


# assert that number of rows of table in $1 is targecnt in $2, optional comment in $3
# assertcnt (table, targetcnt, comment)
assertcnt ()
{
    if [[ $# != 2 ]] && [[ $# != 3 ]] ; then 
        failexit "Expecting 2 (opt 3) parameters but instead was passed $#"
    fi
    local tbl=$1
    local target=$2
    local comment=${3:+"($3)"}
    local cnt=$($CDB2SQL_EXE --tabs ${CDB2_OPTIONS} ${DBNAME} default "select count(*) from $tbl")
    if [ $? -ne 0 ] ; then
        echo "assertcnt: select error"
    fi

    #echo "count is now $cnt"
    if [[ $cnt != $target ]] ; then
        failexit "tbl $tbl count is now $cnt but should be $target $comment"
    fi
}


getmaster()
{
    $CDB2SQL_EXE --tabs ${CDB2_OPTIONS} ${DBNAME} default "select host from comdb2_cluster where is_master='Y'"
}

getclusternodes()
{
    $CDB2SQL_EXE --tabs ${CDB2_OPTIONS} ${DBNAME} default "select host from comdb2_cluster"
}

sendtocluster()
{
    msg=$1
    for n in `getclusternodes` ; do
        $CDB2SQL_EXE --tabs ${CDB2_OPTIONS} ${DBNAME} --host $n "$msg"
    done
}


do_verify()
{
    tbl=$1
    $CDB2SQL_EXE --tabs ${CDB2_OPTIONS} ${DBNAME} default "exec procedure sys.cmd.verify('$tbl', 'parallel')" &> verify_$tbl.out

    if ! grep succeeded verify_$tbl.out > /dev/null ; then
        grep succeeded verify_$tbl.out | head -10
        failexit "verify $tbl had errors"
    fi
}
