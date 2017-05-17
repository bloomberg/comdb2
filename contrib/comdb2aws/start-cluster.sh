#!/bin/sh

usage="$(basename "$0") <cluster> -- Start a cluster"

ec2='aws ec2 --output text'

cluster=$1
if [ "$cluster" = "" ]; then
    echo "$usage" >&2
    exit 1;
fi

set -e
query='Reservations[*].Instances[*].[InstanceId]'
allnodes=`$ec2 describe-instances \
         --filters "Name=tag:Cluster,Values=$cluster" --query $query`

if [ "$allnodes" = "" ]; then
    echo "Could not find cluster \"${cluster}\"" >&2
    exit 1
fi

expect_count=`echo "$allnodes" | wc -l`
expect_code=16
$ec2 start-instances --instance-ids $allnodes >/dev/null

# Poll the instance status till all say `expect_code`
while true; do
    nrdy=`$ec2 describe-instances \
         --instance-ids $allnodes --output json \
         --query 'Reservations[*].Instances[*].State.Code' \
         | grep $expect_code | wc -l`
    echo "$nrdy node(s) running"
    if [ "$nrdy" = "$expect_count" ]; then
        break
    fi
    sleep 10
done
