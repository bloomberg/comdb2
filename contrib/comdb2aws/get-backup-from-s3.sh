#!/bin/sh

usage="$(basename "$0") -b <bucket> -a <archive> [options] -- Restore the database from a S3 backup

Options:
-h, --help                            Show this help information 
-b, --bucket <bucket>                 S3 bucket
-a, --archive <archive>               The backup archive to restore from
-r, --restore <cluster>|localhost     Restore the database on the given cluster or localhost
-l, --lrl <lrldir>                    Directory to place the lrl file
-d, --data <datadir>                  Directory to place the data files"

ec2='aws ec2 --output text'
s3='aws s3 --output text'
s3api='aws s3api --output text'
ssh="ssh -o StrictHostKeyChecking=no -l $SSHUSER"

cluster=
local=0
archive=
bucket=
nodes=
lrldir=
dtadir=
database=
stream=0

_set_opt()
{
    while [ "$1" != "" ]; do
        case $1 in
        "-h" | "--help")
            echo "$usage"
            exit 0
            ;;
        "-r" | "--restore")
            shift
            cluster=$1
            ;;
        "-b" | "--bucket")
            shift
            bucket=$1
            ;;
        "-a" | "--archive")
            shift
            archive=$1
            ;;
        "-l" | "--lrl")
            shift
            lrldir=$1
            ;;
        "-d" | "--data")
            shift
            dtadir=$1
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
    if [ "$cluster" != "" ]; then

        if [ "$cluster" = "localhost" ]; then
            local=1
        else
            # Validate the cluster
            query='Reservations[*].Instances[*].[PrivateIpAddress]'
            nodes=`$ec2 describe-instances \
                     --filters "Name=tag:Cluster,Values=$cluster" --query $query`

            if [ "$nodes" = "" ]; then
                echo "Could not find cluster \"${cluster}\"" >&2
                exit 1
            fi
        fi
    fi

    if [ "$bucket" = "" ]; then
        echo "$usage" >&2
        exit 1
    else
        # does the bucket exist?
        exists=`$s3api list-buckets --query 'Buckets[*].Name' \
               | grep -c $bucket`
        if [ $exists = 0 ]; then
            echo "Could not find bucket \"$bucket\""
            exit 1
        fi
    fi

    if [ "$archive" = "" ]; then
        echo "$usage" >&2
        exit 1
    else
        $s3 cp 's3://'$bucket'/'$archive'/.dbbackup' - | cat >/dev/null 2>&1
        if [ $? = 0 ]; then
            database=`$s3 cp 's3://'$bucket'/'$archive'/.dbbackup' - | cat`
            if [ "$database" = "" ]; then
                echo "Could not find $archive in $bucket" >&2
                exit 1
            fi
            echo "The AWS CLI supports streaming."
            stream=1
        else
            database=`$s3 cp 's3://'$bucket'/'$archive'/.dbbackup' .dbbackup \
                     && cat .dbbackup`
            rm -f .dbbackup
            if [ "$database" = "" ]; then
                echo "Could not find $archive in $bucket" >&2
                exit 1
            fi
            echo "The AWS CLI does not support streaming."
            stream=0
        fi
    fi
}

_get_backup()
{
    backupdir="s3://$bucket/$archive"
    files=`$s3 ls --recursive $backupdir | \
          grep "${archive}/x" | awk '{print $NF}'`

    if [ "$files" = "" ]; then
        echo "Not backup found in $archive" >&2
        exit 1
    fi

    lclname="${database}.tar.lz4"
    rm -f $lclname 2>&1
    touch $lclname

    if [ $stream != 0 ]; then
        for file in $files; do
            $s3 cp "s3://$bucket/$file" - | cat >>$lclname
        done
    else
        for file in $files; do
            $s3 cp "s3://$bucket/$files" ${lclname}.0 && \
                cat ${lclname}.0 >> $lclname
        done
    fi

    if [ "$cluster" = "" ]; then
        echo "Successfully downloaded backup at `pwd`/$lclname"
    else
        # set default values
        if [ "$lrldir" = "" ]; then
            lrldir="$PREFIX/var/cdb2/$database"
        fi

        if [ "$dtadir" = "" ]; then
            dtadir="$PREFIX/var/cdb2/$database"
        fi

        if [ $local = 1 ]; then
            lz4 -d $lclname stdout | $PREFIX/bin/comdb2ar -C strip x $lrldir $dtadir
        else
            for node in $nodes; do
                cat $lclname | \
                    $ssh $node "lz -d stdin stdout | \
                    $PREFIX/bin/comdb2ar x $lrldir $dtadir" &
            done
            wait || exit 1
            # update cluster nodes in lrl file
        fi
        rm -f $lclname
    fi
}

_set_opt $*
_verify_opt
_get_backup
