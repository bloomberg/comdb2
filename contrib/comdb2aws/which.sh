#!/bin/sh

usage="$(basename "$0") [options] -- Locate a cluster

Options:
-h, --help                  Show this help information 
-c, --cluster <cluster>     Show the cluster
-t, --tier <tier>           Show the tier"

###########
# globals #
###########
ec2='aws ec2 --output text'
cluster=
tier=
showdnsonly=0
nl=0
id=
name=
pubip=
pvtip=
state=
bias1=0
bias2=0
margin=0

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
        "-t" | "--tier")
            shift
            tier=$1
            ;;
        "--pvtdns")
            showdnsonly=1
            ;;
        *)
            echo "$usage" >&2
            exit 1
            ;;
        esac
        shift
    done
}

_show_node()
{
    case $nl in
    0)
        id=$1
        ;;
    1)
        state=$1
        ;;
    2)
        pvtip=$1
        ;;
    3)
        pvtdns=$1
        ;;
    4)
        name=$1
        ;;
    esac

    nl=$((nl + 1))
    if [ $nl = 5 ]; then
        nl=0
        _margin=$((margin - bias1))
        printf "%${_margin}.${_margin}s%-20.20s " " " "$name"
        printf "%-12.12s "  "$id"
        printf "%-12.12s "  "$state"
        printf "%-20.20s "  "$pvtip"
        printf "%-40.40s\n" "$pvtdns"
        bias1=0
    fi
}

_show_cluster()
{
    cluster=$1
    query='Reservations[*].Instances[*].[InstanceId,State.Name,PublicIpAddress,PrivateDnsName,Tags[?Key==`Name`].Value]'
    alllines=`$ec2 describe-instances --filters "Name=tag:Cluster,Values=$cluster" \
        --query $query`
    for line in $alllines; do
        _show_node $line
    done
}

_show_tier()
{
    tier=$1
    allclusters=`$ec2 describe-instances --filters "Name=tag:Tier,Values=$tier"\
                | grep "TAGS.*Cluster" | sort | uniq | cut -f3`
    for eachcluster in $allclusters; do
        _margin=$((margin2 - bias2))
        printf "%${_margin}.${_margin}s%-12.12s " " " "$eachcluster"
        bias1=$((bias1 + 13 + _margin))
        _show_cluster $eachcluster
        bias2=0
    done
}

_show_all()
{
    alltiers=`$ec2 describe-instances --filters 'Name=tag-key,Values=Tier'\
             | grep "TAGS.*Tier" | sort | uniq | cut -f3`
    for eachtier in $alltiers; do
        printf "%-12.12s " "$eachtier"
        bias1=$((bias1 + 13))
        bias2=$((bias2 + 13))
        _show_tier $eachtier
    done
}

_show_header()
{
    if [ "$#" = 0 ]; then
        printf "%-12.12s " 'Tier'
        printf "%-12.12s " 'Cluster'
        printf "%-20.20s " "Name"
        printf "%-12.12s " "ID"
        printf "%-12.12s " "State"
        printf "%-20.20s " "Public IP"
        printf "%-40.40s\n" "Private DNS"
        margin=26
        margin2=13
    elif [ "$tier" != "" ]; then
        printf "%-12.12s " 'Cluster'
        printf "%-20.20s " "Name"
        printf "%-12.12s " "ID"
        printf "%-12.12s " "State"
        printf "%-20.20s " "Public IP"
        printf "%-40.40s\n" "Private DNS"
        margin=13
        margin2=0
    else
        printf "%-20.20s " "Name"
        printf "%-12.12s " "ID"
        printf "%-12.12s " "State"
        printf "%-20.20s " "Public IP"
        printf "%-40.40s\n" "Private DNS"
    fi
}

_show_dns()
{
    cluster=$1
    query='Reservations[*].Instances[*].[PrivateDnsName]'
    alllines=`$ec2 describe-instances --filters "Name=tag:Cluster,Values=$cluster" \
        --query $query`
    echo $alllines
}

_set_opt $*

if [ $showdnsonly != 0 ]; then
    if [ "$cluster" = "" ]; then
        echo "Must specify cluster with --pvtdns" >&2
        exit 1
    fi
    _show_dns $cluster
    exit 0
fi

_show_header $*
if [ "$#" = 0 ]; then
    _show_all
elif [ "$tier" != "" ]; then
    _show_tier $tier
else
    _show_cluster $cluster
fi
