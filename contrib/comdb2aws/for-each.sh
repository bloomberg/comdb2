#!/bin/sh

usage="$(basename "$0") -c <cluster> cmd -- Run commands on each node

The IP address is \$NODE.

Options:
-h, --help                  Show this help information 
-c, --cluster <cluster>     Show the cluster"

###########
# globals #
###########
ec2='aws ec2 --output text'
cluster=
cmd=

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
        *)
            cmd="$cmd $1"
            ;;
        esac
        shift
    done
}

_set_opt $*
if [ "$cluster" = "" ]; then
    echo "$usage" >&2
    exit 1
fi

set -e
# get nodes
query='Reservations[*].Instances[*].[PublicIpAddress]'
nodes=`$ec2 describe-instances --filters "Name=tag:Cluster,Values=$cluster" \
        --query $query`
if [ "$nodes" = "" ]; then
    echo "Could not find cluster \"$cluster\""
    exit 1
fi

for NODE in $nodes; do
    sh -c "$(eval "echo $cmd")";
done
