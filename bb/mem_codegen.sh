#!/bin/sh

placeholder_number='__HIMIKE__'
placeholder_name='__HIGHMIKE__'
placeholder_inc='__HAIMIKE__'
prefix='mem_'
newmallocdir=${SRCHOME}/bb
tempfile=".mem.output.$$.tmp"
static_header="$newmallocdir/${prefix}int.h"

mkfiles() ## mkheader <modulename> <moduledir>
{
    module=$1
    moduledir=$2
    uppercasemodule=`echo $module | awk '{print toupper($0);}'`
    modulefilename="${prefix}${module}"
    moduleheader="${moduledir}${modulefilename}.h"
    modulesrc="${moduledir}${modulefilename}.c"

    echo "Generating memory files for module $module..."

    ## header file
    rm -f $tempfile
    echo "#ifndef INCLUDED_COMDB2MA_STATIC_${uppercasemodule}_H" > $tempfile
    echo "#define INCLUDED_COMDB2MA_STATIC_${uppercasemodule}_H" >> $tempfile
    grep "COMDB2MA_STATIC_${uppercasemodule}" $static_header >> $tempfile
    sed "s/$placeholder_number/COMDB2MA_STATIC_${uppercasemodule}/g; s/$placeholder_name/$module/g"  $newmallocdir/mem.h.template >> $tempfile
    echo "#endif" >> $tempfile

    if [ ! -f $moduleheader ]; then
        mkdir -p $moduleinc >/dev/null 2>&1
        cp $tempfile $moduleheader
    fi

    ## clear up
    rm -f $tempfile
}

if [ $# -eq 0 ]; then
    # generate static mspace header
    # some shell implementations will signal an error on IO redirection if the file exists. so we remove the file first.
    rm -f $tempfile
    awk -f $newmallocdir/mem_mkstaticmspaces.awk $newmallocdir/mem_subsystems.txt > $tempfile

    if [ ! -f $static_header ]; then
        mkdir -p ./$newmallocdir/$1 >/dev/null 2>&1
        cp $tempfile $static_header
    else
        diff $tempfile $static_header >/dev/null 2>&1
        if [ $? -ne 0 ]; then ## copy only when content has been modified.
            cp $tempfile $static_header
        fi
    fi

    ## clear up
    rm -f $tempfile

elif [ $# -eq 1 ]; then
    mkfiles $1
else
    mkfiles $1 "$2/"
fi

# generate mspace headers in each subsystem folder

exit 0
