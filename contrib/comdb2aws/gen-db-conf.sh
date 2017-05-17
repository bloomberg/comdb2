#!/bin/sh

usage="$(basename "$0") -c <cluster> -d <db> -- Generate database configuration"
ec2='aws ec2 --output text'
cluster=
database=

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
query='Reservations[*].Instances[*].[PrivateIpAddress]'
allips=`$ec2 describe-instances --filters \
       "Name=tag:Cluster,Values=$cluster" --query $query`
set +e
if [ ! -d $PREFIX/etc/cdb2/config.d/ ]; then
    mkdir -p $PREFIX/etc/cdb2/config.d/
fi
printf "$database 0" >"$PREFIX/etc/cdb2/config.d/${database}.cfg"
for ip in $allips; do
    printf " $ip" >>"$PREFIX/etc/cdb2/config.d/${database}.cfg"
done
printf "\n" >>"$PREFIX/etc/cdb2/config.d/${database}.cfg"
echo "comdb2_config:default_type=$cluster" >>"$PREFIX/etc/cdb2/config.d/${database}.cfg"
chmod -R 755 $PREFIX/etc/cdb2/config.d/
echo "Configure generated at $PREFIX/etc/cdb2/config.d/${database}.cfg"
