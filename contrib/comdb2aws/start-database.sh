#!/bin/sh

usage="$(basename "$0") -d <database> -c <cluster> [options] -- Start a database

Options
-h, --help                  Show this help information 
-d, --database <database>   Database name
-c, --cluster <cluster>     Target cluster
--lrl <lrl>                 lrl location"

ec2='aws ec2 --output text'
ssh="ssh -o StrictHostKeyChecking=no -l $SSHUSER"

cluster=
database=
lrl=

_set_opt()
{
    while [ "$1" != "" ]; do
        case $1 in
        "-h" | "--help")
            echo "$usage"
            exit 0
            ;;
        "-c" | "--cluster")
            shift
            cluster=$1
            ;;
        "-d" | "--database")
            shift
            database=$1
            ;;
        "-l" | "--lrl")
            shift
            lrl=$1
            ;;
        *)
            echo "$usage" >&2
            exit 1
            ;;
        esac
        shift
    done
}

_set_opt $*

if [ "$cluster" = "" ] || [ "$database" = "" ]; then
    echo "$usage" >&2
    exit 1
fi

set -e

# get nodes
query='Reservations[*].Instances[*].[PrivateDnsName]'
nodes=`$ec2 describe-instances --filters "Name=tag:Cluster,Values=$cluster" \
        --query $query`
if [ "$nodes" = "" ]; then
    echo "Could not find cluster \"$cluster\""
    exit 1
fi

# bring up db on each node
for node in $nodes; do
    if [ "$lrl" = "" ]; then
        lrl=$PREFIX/var/${database}/${database}.lrl
    fi

    # !!!HARDCODED PATHS
    $ssh $node "HOSTNAME=$node $PREFIX/bin/comdb2 $database \
        -lrl $lrl >/var/tmp/${database}.log.txt 2>&1 &"
done

set +e
nwaits=0
prompt='Waiting for all nodes to come up...'
while true; do
    printf "%s\r" "$prompt"
    for node in $nodes; do
        hide=`$PREFIX/bin/cdb2sql $database --host $node 'select 1' 2>&1`
        if [ $? = 0 ]; then
            echo
            echo "OK"
            exit 0
        fi
    done
    nwaits=$((nwaits + 1))
    prompt="${prompt}..."
    if [ "$nwaits" -gt "60" ]; then
        echo "Took too long for the database to come up." >&2
        exit 1
    fi
    sleep 5
done
