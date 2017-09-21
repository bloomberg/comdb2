#!/bin/bash

#usage
if [[ "$#" -lt "3" ]]; then
    echo "$0 usage: <root> <hext-file> <cfile> [ <cfile2> <cfile3> .. ]"
    exit 1
fi

#executables
s_awk=/opt/swt/bin/gawk
s_inc=${SRCHOME}/berkdb/dist/gen_inc.awk
if [[ ! -x $s_awk ]]; then
    s_awk=$(which awk)
fi



#temp files
t_edf=/tmp/__db_a.$$
t_epf=/tmp/__db_b.$$
t_idf=/tmp/__db_c.$$
t_ipf=/tmp/__db_d.$$

#arguments
s_rot=$1
a_ext=$2
shift 2
a_cfl=$@

#message
s_msgc="/* DO NOT EDIT: automatically built by berkdb/dist/geninc.sh */"

#header copied from s_include
function s_head
{
	defonly=0
	while :
		do case "$1" in
		space)
			echo ""; shift;;
		defonly)
			defonly=1; shift;;
		*)
			name="$1"; break;;
		esac
	done

	echo "$s_msgc"
	echo "#ifndef	$name"
	echo "#define	$name"
	echo ""
	if [ $defonly -eq 0 ]; then
		echo "#if defined(__cplusplus)"
		echo "extern \"C\" {"
		echo "#endif"
		echo ""
	fi
}

#tailer copied from s_include
function s_tail
{
	defonly=0
	while :
		do case "$1" in
		defonly)
			defonly=1; shift;;
		*)
			name="$1"; break;;
		esac
	done

	echo ""
	if [ $defonly -eq 0 ]; then
		echo "#if defined(__cplusplus)"
		echo "}"
		echo "#endif"
	fi
	echo "#endif /* !$name */"
}

#source some useful variables
. ${SRCHOME}/berkdb/dist/RELEASE

#trap directive
trap 'rm -f $t_edf $t_epf $t_idf $t_ipf ; exit 0' 0 1 2 3 13 15

#external definitions (ext_def.in)
s_head defonly space _DB_EXT_DEF_IN_ > $t_edf

#external prototypes (ext_prot.in)
s_head space _DB_EXT_PROT_IN_ > $t_epf

#internal definitions (int_def.in)
s_head defonly _DB_INT_DEF_IN_ > $t_idf

#internal prototypes
s_head "_${s_rot}_ext_h" > $t_ipf

#generate file
$s_awk -f $s_inc                                        \
    -v db_version_unique_name=$DB_VERSION_UNIQUE_NAME   \
    -v e_dfile=$t_edf                                   \
    -v e_pfile=$t_epf                                   \
    -v i_dfile=$t_idf                                   \
    -v i_pfile=$t_ipf $a_cfl

#add tail
s_tail "_${s_rot}_ext_h" >> $t_ipf

#move into place
cmp $t_ipf $a_ext > /dev/null 2>&1 ||
(
    echo "Building $a_ext"
    rm -f $a_ext
    cp $t_ipf $a_ext
    chmod 444 $a_ext
)
