#!/bin/sh

usage="$(basename "$0") -c <cluster> -d <db> [options] -- Take a backup

Options:
-h, --help                  Show this help information 
-d, --database <database>   Database name
-c, --cluster <cluster>     Target cluster
--lrl <lrl>                 Path to the lrl on the remote cluster
--dl                        Keep a local copy
-b, --bucket <bucket>       Store backup in the S3 bucket"

ec2='aws ec2 --output text'
s3='aws s3 --output text'
s3api='aws s3api --output text'

cluster=
database=
lrl=
lrldir=
bucket=
hasdboncluster=0
dl=0
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
            ;;
        "--lrl")
            shift
            lrl=$1
            ;;
        "-b" | "--bucket")
            shift
            bucket=$1
            ;;
        "--dl")
            dl=1
            ;;
        *)
            echo "$usage" >&2
            exit 1
            ;;
        esac
        shift
    done
}

_verify_opt()
{
    if [ "$cluster" = "" ] || [ "$database" = "" ]; then
        echo "$usage" >&2
        exit 1
    fi

    query='Reservations[*].Instances[*].[PrivateIpAddress]'
    nodes=`$ec2 describe-instances \
             --filters "Name=tag:Cluster,Values=$cluster" --query $query`

    if [ "$nodes" = "" ]; then
        echo "Could not find cluster \"${cluster}\"" >&2
        exit 1
    fi

    if [ "$bucket" != "" ]; then
        # does the bucket exist?
        exists=`$s3api list-buckets --query 'Buckets[*].Name' \
               | grep -c $bucket`
        if [ $exists = 0 ]; then
            echo "Could not find bucket \"$bucket\""
            exit 1
        fi
    fi

    if [ "$lrl" = "" ]; then
        lrl="$PREFIX/var/cdb2/${database}/${database}.lrl"
    fi
}

_backup_local()
{
    thenode=$1
    backupname="${database}.`date +"%Y%m%dT%H%MZ%3N"`.tar.lz4"
    lrldir=${lrl%.lrl}
    lrldir=${lrldir%/*}
    $ssh $thenode "cd $lrldir && $PREFIX/bin/copycomdb2 -b $lrl > $backupname"

    if [ $? != 0 ]; then
        echo "Error taking the backup on $node" >&2
        exit 1
    else
        echo "Successfully took backup at $lrldir/$backupname on $thenode"
    fi
}

_backup_download()
{
    thenode=$1
    backupname="${database}.${cluster}.`date +"%Y%m%dT%H%MZ%3N"`.tar.lz4"
    $ssh $thenode "$PREFIX/bin/copycomdb2 -b $lrl" >"`pwd`/$backupname"

    if [ $? != 0 ]; then
        echo "Error pulling the backup from $node" >&2
        exit 1
    else
        echo "Successfully took backup at `pwd`/$backupname"
    fi
}

_backup_s3()
{
    thenode=$1
    backupdir="s3://$bucket/$database/$cluster/`date +'%Y%m%dT%H%MZ%3N'`"
    echo "streaming" | $s3 cp - $backupdir/.stream >/dev/null 2>&1
    if [ $? != 0 ]; then
        echo "The AWS CLI does not support streaming."
        $ssh $thenode "$PREFIX/bin/copycomdb2 -b $lrl" | \
        split -d -b 4G --filter='cat >$FILE && aws s3 mv $FILE '$backupdir'/$FILE'
    else
        echo "The AWS CLI supports streaming."
        $ssh $thenode "$PREFIX/bin/copycomdb2 -b $lrl" | \
        split -d -b 4G --filter='aws s3 cp - '$backupdir'/$FILE'
    fi

    if [ $? = 0 ]; then
        echo $database >.dbbackup
        $s3 mv .dbbackup $backupdir/.dbbackup >/dev/null
    fi
}

_set_opt $*
_verify_opt

for node in $nodes; do
    if [ "$bucket" != "" ]; then
        _backup_s3 $node
    elif [ $dl = 1 ]; then
        _backup_download $node
    else
        _backup_local $node
    fi

    hasdboncluster=1
    break
done

if [ $hasdboncluster = 0 ]; then
    echo "Could not find database \"$database\" on cluster \"$cluster\"" >&2
    exit 1
fi
