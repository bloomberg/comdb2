#!/bin/bash
# this tests the ability of the node doing analyze to 
# abort if there is not enough free space on the node.
# To achieve correct testing, we need to connect to 
# that node and fetch the percent freespace so we can
# set it via cdb2sql send.

#set -x
args=$1
dbnm=$2

function process_node() {
    node=$1
    currused=$2

    #set threshold to something low 
    cdb2sql ${CDB2_OPTIONS} $dbnm --host $node  "exec procedure sys.cmd.send('analyze thresh 1000')" &> t04_01.req.res.1

    #set headroom to current disk headroom
    headroom=$((100-currused+1))
    cdb2sql ${CDB2_OPTIONS} $dbnm --host $node "exec procedure sys.cmd.send('analyze headroom $headroom')" &> t04_01.req.res.2

    #run analyze
    cdb2sql --tabs ${CDB2_OPTIONS} $dbnm --host $node "exec procedure sys.cmd.analyze('t1')"  &> t04_01.req.res.3

    if ! diff t04_01.req.out.3 t04_01.req.res.3 ; then
        echo FAIL: diff t04_01.req.out.3 t04_01.req.res.3 not what it should be, node $node, headroom $headroom
        exit 0
    fi

    #set it back to large, in case we have other tests after this one
    cdb2sql ${CDB2_OPTIONS} $dbnm --host $node "exec procedure sys.cmd.send('analyze thresh 1000000')" &> t04_01.req.res.4
    cdb2sql ${CDB2_OPTIONS} $dbnm --host $node "exec procedure sys.cmd.send('analyze headroom 6')" &> t04_01.req.res.5
}

cluster=`cdb2sql --tabs ${CDB2_OPTIONS} $dbnm default 'exec procedure sys.cmd.send("bdb cluster")' | grep lsn | cut -f1 -d':' `

# check with every node in cluster:
for node in $cluster ; do 
    if [ $node == `hostname` ] ; then
        currused=`df ${TESTDIR} | grep -v Filesystem | awk '{print $5 }' | sed 's/%//'`
    else
        currused=`ssh -o StrictHostKeyChecking=no $node "df ${TESTDIR}" | grep -v Filesystem | awk '{print $5 }' | sed 's/%//'`
    fi
    process_node $node $currused
done

echo SUCCESS

