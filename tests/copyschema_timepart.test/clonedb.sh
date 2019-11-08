#!/usr/bin/env bash

newdb=srcdb$dbname
newdbdir=$TESTDIR/$newdb
newlrl=$newdbdir/$newdb.lrl

echo "Creating newdb $dbname in $newdbdir"

# copy the schema of the old db
echo "$COMDB2AR_EXE -s c $DBDIR/$dbname.lrl | $COMDB2AR_EXE  -C strip -u 95 x $newdbdir $newdbdir"
$COMDB2AR_EXE -s c $DBDIR/$dbname.lrl | $COMDB2AR_EXE  -C strip -u 95 x $newdbdir $newdbdir
echo mv $newdbdir/$dbname.lrl $newlrl
mv $newdbdir/$dbname.lrl $newlrl
echo sed -i "/^name /s/$dbname/$newdb/g" $newlrl
sed -i "/^name /s/$dbname/$newdb/g" $newlrl
sed -i "/^ssl_client_mode /s/REQUIRE/ALLOW/g" $newlrl

# recreate the db
echo $COMDB2_EXE $newdb --create --lrl $newlrl
$COMDB2_EXE $newdb --create --lrl $newlrl
if (( $? != 0 )) ; then
   echo "FAILURE"
   exit 1
fi
