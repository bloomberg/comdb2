#!/bin/sh
usage="$(basename "$0") -d <database> -c <cluster> [options]

Options:
-h, --help                  Show this help information 
-d, --database <database>   Database name
-c, --cluster <cluster>     Target cluster
--lrl <lrl>                 Path to the lrl file
--dir <datadir>             Data directory
--gen-conf                  Generate per-db configure on this instance. This is the default.
--no-gen-conf               Do not generate per-db configure on this instance."

###########
# globals #
###########
ec2='aws ec2 --output text'

cluster=
database=
dbdir=
genconf=1
lrl=
dir=
ssh="ssh -o StrictHostKeyChecking=no -l $SSHUSER"

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
            if [ "$dbdir" = "" ]; then
                dbdir="$PREFIX/var/$database"
            fi
            ;;
        "--gen-conf")
            genconf=1
            ;;
        "--no-gen-conf")
            genconf=0
            ;;
        "-l" | "--lrl")
            shift
            lrl=$1
            ;;
        "-i" | "--dir")
            shift
            dir=$1
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

set -xe
# get nodes
query='Reservations[*].Instances[*].[PrivateIpAddress]'
nodes=`$ec2 describe-instances --filters "Name=tag:Cluster,Values=$cluster" \
        --query $query`
if [ "$nodes" = "" ]; then
    echo "Could not find cluster \"$cluster\""
    exit 1
fi

query='Reservations[*].Instances[*].[PrivateDnsName]'
dnsnames=`$ec2 describe-instances --filters "Name=tag:Cluster,Values=$cluster" \
        --query $query`

# init db locally on the 1st node
anode=`echo "$nodes" | head -1`
$ssh $anode "rm -rf $dbdir; \
        $PREFIX/bin/comdb2 --create --dir $dbdir $database ; \
        echo "cluster nodes " `echo $dnsnames` >>${dbdir}/${database}.lrl"

# copy over
for node in $nodes; do
    if [ "$node" != "$anode" ]; then
        if [ "$dir" = "" ]; then
            $ssh $anode "$PREFIX/bin/copycomdb2 ${dbdir}/${database}.lrl ${node}:${lrl}"
        else
            $ssh $anode "$PREFIX/bin/copycomdb2 ${dbdir}/${database}.lrl ${node}:${lrl} ${node}:${dir}"
        fi
    fi
done

# bring up db on each node
for node in $nodes; do
    if [ "$lrl" = "" ]; then
        lrl=${dbdir}/${database}.lrl
    fi

    # !!!HARDCODED PATHS
    $ssh $node "HOSTNAME=$node $PREFIX/bin/comdb2 $database \
        -lrl $lrl >/var/tmp/${database}.log.txt 2>&1 &"
done
set +xe

# wait for all instances to come up
nwaits=0
prompt='Waiting for all nodes to come up...'
while true; do
    printf "%s\r" "$prompt"
    for node in $nodes; do
        hide=`$PREFIX/bin/cdb2sql $database --host $node 'select 1' 2>&1`
        if [ $? = 0 ]; then
            echo
            echo "OK"
            if [ $genconf != 0 ]; then
                set -e
                query='Reservations[*].Instances[*].[PrivateIpAddress]'
                allips=`$ec2 describe-instances --filters \
                       "Name=tag:Cluster,Values=$cluster" --query $query`
                set +e
                if [ ! -d $PREFIX/etc/cdb2/config.d/ ]; then
                    mkdir -p $PREFIX/etc/cdb2/config.d/
                    if [ $? != 0 ]; then
                        echo 'Failed to generate database config.' >&2
                        longline='If it is a permission issue, please run'
                        longline="${longline} \"$PREFIX/bin/comdb2aws gen-db-conf -c ${cluster} -d ${database}\""
                        longline="${longline} as the correct user."
                        echo "$longline" >&2
                        rm -rf $dbdir
                        exit 0
                    fi
                fi
                printf "$database 0" >"$PREFIX/etc/cdb2/config.d/${database}.cfg"
                for ip in $allips; do
                    printf " $ip" >>"$PREFIX/etc/cdb2/config.d/${database}.cfg"
                done
                printf "\n" >>"$PREFIX/etc/cdb2/config.d/${database}.cfg"
                echo "comdb2_config:default_type=$cluster" >>"$PREFIX/etc/cdb2/config.d/${database}.cfg"
                chmod -R 755 $PREFIX/etc/cdb2/config.d/
                echo "Configure generated at $PREFIX/etc/cdb2/config.d/${database}.cfg"
            fi
            exit 0
        fi
    done
    nwaits=$((nwaits + 1))
    prompt="${prompt}..."

    if [ "$nwaits" -gt "60" ]; then
        echo "Too too long for the database to come up." >&2
        exit 1
    fi
    sleep 5
done

rm -rf $dbdir
