DROP TABLE IF EXISTS t4;

CREATE TABLE t4 {
// Schema for table testtag

constants
{
    F1  = 2,
    F2 = 8
}

tag ondisk
{
    int        c1       // a comment
    byte       c2[F2]
    datetime   c3
    datetime   c4
    datetime   c5
    double     c6
    double     c7
    double     c8
    double     c9 null=yes
    double     c10
    double     c11
    double     c12
    double     c13
    short      c14
    cstring    c15[F1]
    short      c16 dbstore=0
    short      c17 dbstore=0
    double     c18 dbstore=-999998.00
}

tag default
{
    int        c1       // a comment
    byte       c2[F2]
    datetime   c3
    datetime   c4
    datetime   c5
    double     c6
    double     c7
    double     c8
}

tag "all"
{
    int        c1
    byte       c2[F2]
    datetime   c3
    datetime   c4
    datetime   c5
    double     c6
    double     c7
    double     c8
    double     c9
    double     c10
    double     c11
    double     c12
    double     c13
    short      c14
    cstring    c15[F1]
    short      c16
    short      c17
}

tag "all_v1"
{
    int        c1
    byte       c2[F2]
    datetime   c3
    datetime   c4
    datetime   c5
    double     c6
    double     c7
    double     c8
    double     c9
    double     c10
    double     c11
    double     c12
    double     c13
    short      c14
    cstring    c15[F1]
    short      c16
    short      c17
    double     c18
}

keys
{
      "KEY_1"  = c1
      "KEY_2"  = c2 + c3
  dup "KEY_3"  = c2 + c5
  dup "KEY_4"  = c3 + c2
  dup "KEY_5"  = c2 + c4 + c16
  dup "KEY_6"  = c2 + c3 + c16
  dup "KEY_7"  = c3 + c16
  dup "KEY_8"  = c4 + c16
  dup "KEY_9"  = c5 + c16
  dup "KEY_10" = c2 + c5 + c16
  dup "KEY_11" = c3 + c16 + c2
  dup "KEY_12" = c3 + c4 + c16
}
}; $$

SELECT * FROM comdb2_tags order by tablename, ixnum
SELECT * FROM comdb2_tag_columns order by tablename, tagname, indx
EXEC PROCEDURE sys.cmd.send('dumptags')
