#!/bin/sh

usage="$(basename "$0") -b <bucket> [options] -- List backups stored in the S3 bucket

Options:
-h, --help                  Show this help information 
-d, --database <database>   Database name
-c, --cluster <cluster>     Target cluster
-b, --bucket <bucket>       S3 bucket"

ec2='aws ec2 --output text'
s3='aws s3 --output text'
s3api='aws s3api --output text'

cluster=
database=
bucket=

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
        "-b" | "--bucket")
            shift
            bucket=$1
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
    if [ "$bucket" = "" ]; then
        echo "$usage" >&2
        exit 1
    fi

    # does the bucket exist?
    exists=`$s3api list-buckets --query 'Buckets[*].Name' \
           | grep -c $bucket`
    if [ $exists = 0 ]; then
        echo "Could not find bucket \"$bucket\""
        exit 1
    fi
}

_set_opt $*
_verify_opt

set -e
all=`$s3 ls s3://$bucket --recursive | grep '.dbbackup' | \
    awk '{print $1, $2, substr($4, 0, length($4)-length(".dbbackup")-1)}'`

if [ "$cluster" != "" ]; then
    all=`echo "$all" | grep $cluster`
fi

if [ "$database" != "" ]; then
    all=`echo "$all" | grep $database`
fi
echo "$all"
