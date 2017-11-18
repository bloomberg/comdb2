#!/usr/bin/env bash

db=$1
db=$2

##################################
# alter db without any compression
##################################
cdb2sql ${CDB2_OPTIONS} $db default "drop table compress_alter"
cdb2sql ${CDB2_OPTIONS} $db default "create table compress_alter { `cat compress_alter0.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(0)"
#comdb2sc -y $db alter compress_alter compress_alter0.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter { `cat compress_alter0.csc2 ` }"

#comdb2sc -y $db alter compress_alter compress_alter1.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter { `cat compress_alter1.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter { `cat compress_alter1.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(1)"

#comdb2sc -y $db alter compress_alter compress_alter2.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter { `cat compress_alter2.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter { `cat compress_alter2.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(2)"
echo "alter db without any compression"
cdb2sql ${CDB2_OPTIONS} $db default "select * from compress_alter"

#comdb2sc -y $db alter compress_alter compress_alter3.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter { `cat compress_alter3.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter { `cat compress_alter3.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(3)"
cdb2sql ${CDB2_OPTIONS} $db default "select * from compress_alter"

################################
# alter db with data compression
################################
cdb2sql ${CDB2_OPTIONS} $db default "drop table compress_alter"
cdb2sql ${CDB2_OPTIONS} $db default "create table compress_alter { `cat compress_alter0.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(0)"

#comdb2sc -y $db alter compress_alter compress_alter1.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter { `cat compress_alter1.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter { `cat compress_alter1.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(1)"

#comdb2sc -y $db alter compress_alter compress_alter2.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter { `cat compress_alter2.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter { `cat compress_alter2.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(2)"
echo "alter db with data compression"
cdb2sql ${CDB2_OPTIONS} $db default "select * from compress_alter"

#comdb2sc -y -zrec:rle $db alter compress_alter compress_alter3.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter options rec rle { `cat compress_alter3.csc2 ` }"
#comdb2sc -zrec:rle $db alter compress_alter compress_alter3.csc2
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter options rec rle { `cat compress_alter3.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(3)"
cdb2sql ${CDB2_OPTIONS} $db default "select * from compress_alter"

################################
# alter db with blob compression
################################
cdb2sql ${CDB2_OPTIONS} $db default "drop table compress_alter"
cdb2sql -s ${CDB2_OPTIONS} $db default "create table compress_alter { `cat compress_alter0.csc2 ` }" > /dev/null
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(0)"

#comdb2sc -y $db alter compress_alter compress_alter1.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter { `cat compress_alter1.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter { `cat compress_alter1.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(1)"

#comdb2sc -y $db alter compress_alter compress_alter2.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter { `cat compress_alter2.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter { `cat compress_alter2.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(2)"
echo "alter db with blob compression"
cdb2sql ${CDB2_OPTIONS} $db default "select * from compress_alter"

#comdb2sc -y -zblob:zlib $db alter compress_alter compress_alter3.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter options blobfield zlib { `cat compress_alter3.csc2 ` }"
#comdb2sc -zblob:zlib $db alter compress_alter compress_alter3.csc2
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter options blobfield zlib { `cat compress_alter3.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(3)"
cdb2sql ${CDB2_OPTIONS} $db default "select * from compress_alter"

################################
# alter db & compress dta + blob
################################
cdb2sql ${CDB2_OPTIONS} $db default "drop table compress_alter"
cdb2sql -s ${CDB2_OPTIONS} $db default "create table compress_alter { `cat compress_alter0.csc2 ` }" > /dev/null
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(0)"

#comdb2sc -y $db alter compress_alter compress_alter1.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter { `cat compress_alter1.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter { `cat compress_alter1.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(1)"

#comdb2sc -y $db alter compress_alter compress_alter2.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter { `cat compress_alter2.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter { `cat compress_alter2.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(2)"
echo "alter db compress data and blobs"
cdb2sql ${CDB2_OPTIONS} $db default "select * from compress_alter"

#comdb2sc -y -zrec:rle -zblob:zlib $db alter compress_alter compress_alter3.csc2
cdb2sql -tabs ${CDB2_OPTIONS} $db default "dryrun alter table compress_alter options rec rle, blobfield zlib { `cat compress_alter3.csc2 ` }"
#comdb2sc -zrec:rle -zblob:zlib $db alter compress_alter compress_alter3.csc2
cdb2sql ${CDB2_OPTIONS} $db default "alter table compress_alter options rec rle, blobfield zlib { `cat compress_alter3.csc2 ` }"
cdb2sql ${CDB2_OPTIONS} $db default "insert into compress_alter(a) values(3)"
cdb2sql ${CDB2_OPTIONS} $db default "select * from compress_alter"
