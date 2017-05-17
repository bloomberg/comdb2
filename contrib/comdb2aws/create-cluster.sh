#!/bin/sh -l

usage="$(basename "$0") --cluster <cluster-name> [options] -- Create a cluster

Options:
--help                      Show this help information
--cluster <cluster>         Cluster name
--tier <tier>               Tier of the cluster
--count <number>            Number of instances in the cluster
--instance-type <type>      AWS EC2 instance type
--distro <distro>           Base Linux distro of the Comdb2 AMI
--image-id <id>             Comdb2 AMI ID"

###########
# globals #
###########
ec2='aws ec2 --output text'
ec2vrfy='aws ec2 --dry-run'
key_name=
DEFAULT_distro='debian'
DEFAULT_image_id=ami-8c12649a
distro='debian'
image_id=
tier=
tier_sg=
tier_sgid=
cluster=
instance_type=t2.micro
count=4
creat_tier=0
creat_key=0
has_opt=0
ssh="ssh -o StrictHostKeyChecking=no -l $SSHUSER"

bootstrap='#!/bin/sh
mkdir -p /opt/bb/var 2>&1
echo "hostname `hostname -f`" >'$PREFIX'/etc/cdb2/config/comdb2.d/hostname.lrl
chmod -R 755 '$PREFIX'/etc
mkdir -p '$PREFIX'/etc/cdb2/config.d/
chown -R '$SSHUSER' '$PREFIX'/etc/cdb2/config.d/
chmod -R 777 '$PREFIX'/var
'$PREFIX'/bin/pmux -l &'

_yn()
{
    read -p 'Proceed [y/n]? ' yn
    case $yn in
        [Yy]*)
        return 0
        ;;
        *)
        echo 'Not confirmed. Exiting.'
        exit
        ;;
    esac
}

_recipe()
{
    echo
    echo "cluster: $cluster"
    echo "count: $count"
    echo "tier: $tier"
    echo "instance type: $instance_type"
    echo "Base AMI distro: $distro"
}

_resources()
{
    echo
    echo 'The following resources will be created:'
    if [ $creat_tier != 0 ]; then
        echo "- security group \"${tier_sg}\""
    fi
    if [ $creat_key != 0 ]; then
        echo "- key pair \"${key_name}\""
    fi
    echo "- $count $instance_type instance(s)"

    if [ $has_opt = 0 ]; then
        _yn
    fi
}

_wizard()
{
    while true; do
        read -p "cluster [$cluster]: " user_cluster
        if [ "$user_cluster" != "" ]; then
            cluster=$user_cluster
            tier=$cluster
            tier_sg="comdb2-tier-$user_cluster"
            key_name="comdb2-cluster-key-${cluster}"
            break
        fi
    done

    read -p "count [$count]: " user_count
    if [ "$user_count" != "" ]; then
        count=$user_count
    fi

    read -p "tier [$cluster]: " user_tier
    if [ "$user_tier" != "" ]; then
        tier=$user_tier
        tier_sg="comdb2-tier-$user_tier"
    fi

    read -p "instance-type [$instance_type]: " user_instance_type
    if [ "$user_instance_type" != "" ]; then
        instance_type=$user_instance_type
    fi

    read -p "distro [$distro]: " user_distro
    if [ "$user_distro" != "" ]; then
        distro=$user_distro
    fi

    _recipe
    _yn
}

_set_opt()
{
    while [ "$1" != "" ]; do
        case $1 in
        "-h" | "--help")
            echo "$usage"
            exit 0
            ;;
        "--image-id")
            shift
            image_id=$1
            ;;
        "--tier")
            shift
            tier=$1
            tier_sg="comdb2-tier-$1"
            ;;
        "--cluster")
            shift
            cluster=$1
            key_name="comdb2-cluster-key-${cluster}"
            ;;
        "--instance-type")
            shift
            instance_type=$1
            ;;
        "--count")
            shift
            count=$1
            ;;
        *)
            echo "$usage" >&2
            exit 1
            ;;
        esac
        shift
    done

    if [ "$cluster" = "" ]; then
        echo "$usage" >&2
        exit 1
    fi

    _recipe
}

_verify_opt()
{
    echo
    echo 'Verifying...'

    if [ "$cluster" = "" ]; then
        echo 'Must specify cluster name.' >&2
        echo "$usage" >&2
        exit 1
    fi

    if [ "$tier" = "" ]; then
        tier=$cluster
    fi

    if [ "$tier_sg" = "" ]; then
        tier_sg="comdb2-tier-$cluster"
    fi

    # Must-have options
    exists=`$ec2 describe-tags --filters 'Name=key,Values=cluster'\
          "Name=value,Values=$cluster" --query 'Tags' | wc -l`
    if [ $exists != 0 ]; then
        echo "Cluster ${cluster} already exists. Aborting." >&2
        exit 1
    fi

    if [ "$image_id" = "" ]; then
        image_id=`curl -s --fail \
                 https://raw.githubusercontent.com/bloomberg/comdb2/master/contrib/comdb2aws/${distro}.ami | head -1`
        if [ "$image_id" != "" ]; then
            echo "Found pre-built $distro AMI: $image_id"
        else
            echo "Could not find pre-built $distro AMI."
            echo "Fall back to $DEFAULT_image_id ($DEFAULT_distro)."
            image_id=$DEFAULT_image_id
        fi
    fi
    $ec2 describe-images --image-ids $image_id \
        --query 'Images[*][ImageId, Description]'
    if [ $? != 0 ]; then
        echo "Image $image_id not found." >&2
        exit 1
    fi

    # Create-if-absent options
    tier_sgid=`$ec2 describe-security-groups \
        --group-names $tier_sg --query 'SecurityGroups[*].GroupId'`
    if [ $? != 0 ]; then
        creat_tier=1
    fi

    $ec2 describe-key-pairs --key-names $key_name
    if [ $? != 0 ]; then
        creat_key=1
    fi

    echo "OK"
}

_creat()
{
    echo
    echo "Creating ${cluster} cluster..."
    set -e

    if [ $creat_tier != 0 ]; then
        tier_sgid=`$ec2 create-security-group \
                   --group-name $tier_sg \
                   --description $tier_sg \
                   --query 'GroupId'`
        # Allow all traffic within the tier's security group
        $ec2 authorize-security-group-ingress --group-id $tier_sgid \
            --source-group $tier_sgid --protocol 'all'
        $ec2 authorize-security-group-ingress --group-id $tier_sgid \
            --ip-permissions '[{"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}]'
    fi

    if [ $creat_key != 0 ]; then
        $ec2 create-key-pair \
            --key-name $key_name --query 'KeyMaterial' \
            | sed 's/\\n/\n/g' - | tee ${key_name}.pem
        chmod 400 ${key_name}.pem
        if [ ! -d ~/.ssh ]; then
            mkdir ~/.ssh 2>/dev/null
        fi
        mv ${key_name}.pem ~/.ssh/
        echo "SSH key saved at ~/.ssh/${key_name}.pem"
    fi

    insts=`$ec2 run-instances --security-groups $tier_sg \
          --image-id $image_id --count $count \
          --instance-type $instance_type --key-name $key_name \
          --user-data "$bootstrap"\
          --query 'Instances[*].InstanceId'`

    # Poll the instance status till all '16' (running)
    # We occasionally see "instance id not found" errors from aws,
    # so let's be more tolerant here.
    set +e
    prompt='Waiting for all instances to come up...'
    while true; do
        printf "%s\r" "$prompt"
        nrdy=`$ec2 describe-instances \
             --instance-ids $insts --output json \
             --query 'Reservations[*].Instances[*].State.Code' \
             | grep 16 | wc -l`
        if [ "$nrdy" = "$count" ]; then
            break
        fi
        prompt="${prompt}..."
        sleep 10
    done
    set -e
    echo

    # Give each instance a name.
    echo "Assigning tags to instances..."
    indx=0
    for each in $insts; do
        indx=$((indx + 1))
        aws ec2 create-tags --resources $each \
            --tags Key=Name,Value="${cluster}-$indx" >/dev/null
    done

    aws ec2 create-tags --resources $insts \
        --tags Key=Cluster,Value=$cluster Key=Tier,Value=$tier
    set +e

    if [ -f ~/.ssh/${key_name}.pem ]; then
        echo "Found SSH key at ~/.ssh/${key_name}.pem"
        pgrep ssh-agent
        if [ $? != 0 ]; then
            eval `ssh-agent -s`
        fi
        sleep 2
        ssh-add ~/.ssh/${key_name}.pem
        if [ $? != 0 ]; then
            eval `ssh-agent -s`
            sleep 2
            ssh-add ~/.ssh/${key_name}.pem
        fi

        echo "Setting up SSH keys within the cluster"
        addrs=`$ec2 describe-instances \
              --instance-ids $insts --query \
              'Reservations[*].Instances[*].[PublicIpAddress]'`
        for addr in $addrs; do
            for retry in `seq 1 10`; do
                cat ~/.ssh/${key_name}.pem \
                | $ssh $addr "chmod 700 ~/.ssh/id_rsa 2>/dev/null; cat > ~/.ssh/id_rsa; chmod 700 ~/.ssh/id_rsa"
                if [ $? != 0 ]; then
                    sleep 10;
                    continue;
                fi
                ssh-keygen -y -f ~/.ssh/${key_name}.pem \
                | $ssh $addr "cat >> ~/.ssh/authorized_keys"
                if [ $? != 0 ]; then
                    sleep 10;
                    continue;
                fi
                ssh-keygen -y -f ~/.ssh/${key_name}.pem \
                | $ssh $addr "cat > ~/.ssh/id_rsa.pub"
                if [ $? != 0 ]; then
                    sleep 10;
                    continue;
                fi

                $ssh $addr "echo 'Host *
                StrictHostKeyChecking no'>> ~/.ssh/config"
                if [ $? != 0 ]; then
                    sleep 10;
                    continue;
                fi

                echo "PATH=\"$PATH:$PREFIX/bin\"" \
                | $ssh $addr "cat > ~/.profile"
                if [ $? != 0 ]; then
                    sleep 10;
                    continue;
                fi
                break;
            done
        done
    fi
    echo "OK"

    printf "please assign the instances to "
    printf "\"$tier_sg\" security group to allow "
    printf "access to Comdb2 $cluster cluster.\n"
    if [ $has_opt = 0 ]; then
        which ec2metadata
        if [ $? = 0 ]; then
            myinst=`ec2metadata | grep 'instance-id' | cut -d' ' -f2`

            if [ $? = 0 ]; then
                mygrps=`$ec2 describe-instances --instance-ids $myinst --query \
                       'Reservations[*].Instances[*].SecurityGroups[*].GroupId'`

                # wc -l to suppress errors
                ingrp=`echo $mygrps | grep $tier_sgid | wc -l`
                if [ "$ingrp" = 0 ]; then
                    echo "Assign this instance to $tier_sg..."
                    _yn

                    $ec2 modify-instance-attribute \
                        --instance-id $myinst --groups $mygrps $tier_sgid
                fi
            fi
        fi
    fi

    echo 'Please use the following commands to import the ssh key:'
    echo "eval \`ssh-agent -s\`; ssh-add ~/.ssh/${key_name}.pem"
}

if [ $# = 0 ]; then
    _wizard
else
    has_opt=1
    _set_opt $*
fi

_verify_opt
_resources
_creat
