#!/usr/bin/env bash

# Common bash functions across many of the runit scripts

# exit after displaying error message
failexit()
{
    echo "Failed $1"
    exit -1
}


# assert expected value in $1 is the same as target in $2
# assertres (expected, target)
assertres ()
{
    if [ $# != 2 ] ; then 
        failexit "Expecting 2 parameters but instead was passed $#"
    fi
    local expected=$1
    local target=$2
    if [ "$expected" != "$target" ] ; then
        failexit "Expected is $expected but should be $target"
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
    comment=$3
    local cnt=$(cdb2sql --tabs ${CDB2_OPTIONS} ${DBNAME} default "select count(*) from $tbl")
    if [ $? -ne 0 ] ; then
        echo "assertcnt: select error"
    fi

    #echo "count is now $cnt"
    if [[ $cnt != $target ]] ; then
        failexit "tbl $tbl count is now $cnt but should be $target"
    fi
}


getmaster()
{
    cdb2sql --tabs ${CDB2_OPTIONS} ${DBNAME} default 'exec procedure sys.cmd.send("bdb cluster")' | grep MASTER | cut -f1 -d":" | tr -d '[:space:]'
}

