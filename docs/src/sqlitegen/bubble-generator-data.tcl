
# This file contains the data used by the three syntax diagram rendering
# programs:
#
#   bubble-generator.tcl
#   bubble-generator-bnf.tcl
#   bubble-generator-text.tcl
#

# Graphs:
#
#
set lbrc "}"       
set rbrc "{"

set all_graphs {
  sql-stmt {
    line
      {opt EXPLAIN {opt QUERY PLAN}}
      {or
         begin-stmt
         commit-stmt
         delete-stmt-limited
         insert-stmt
         rollback-stmt
         select-stmt
         update-stmt-limited
         create-table
         alter-table
         create-proc
         create-lua-func
         create-time-part
         drop
         truncate
         analyze
         grant-revoke
         rebuild
         get
         put
         set-stmt
         exec-procedure
         host-config
      }
  }
  begin-stmt {
     line BEGIN {optx TRANSACTION AS OF DATETIME /datetime-literal}
  }
  commit-stmt {
     line COMMIT
  }
  rollback-stmt {
     line ROLLBACK
  }
  type-name {
     line {loop /name {}} {or {}
        {line ( signed-number )}
        {line ( signed-number , signed-number )}
     }
  }
  signed-number {
     line {or nil + -} /numeric-literal
  }
  with-clause {
    line
      WITH {opt RECURSIVE} {loop {line cte-table-name AS ( select-stmt )} ,}
  }
  cte-table-name {
    line qualified-table-name {optx ( {loop /column-name ,} )}
  }
  recursive-cte {
    line cte-table-name AS 
       ( /initial-select {or UNION {line UNION ALL}} /recursive-select )
  }
  common-table-expression {
    line  /qualified-table-name {optx ( {loop /column-name ,} )} AS ( select-stmt )
  }
  delete-stmt-limited {
    stack
        {line {opt with-clause} DELETE FROM qualified-table-name}
        {optx WHERE expr}
        {optx
            {stack
              {optx ORDER BY {loop ordering-term ,}}
              {line LIMIT expr {optx {or OFFSET ,} expr}}
            }
        }
  }

  expr {
    or
     {line literal-value}
     {line bind-parameter}
     {line {optx /qualified-table-name .} /column-name}
     {line /unary-operator expr}
     {line expr /binary-operator expr}
     {line /function-name ( {or {line {optx DISTINCT} {toploop expr ,}} {} *} )}
     {line ( expr )}
     {line CAST ( expr AS type-name )}
     {line expr COLLATE /collation-name}
     {line expr {optx NOT} {or LIKE GLOB REGEXP MATCH} expr
           {opt ESCAPE expr}}
     {line expr {or ISNULL NOTNULL {line NOT NULL}}}
     {line expr IS {optx NOT} expr}
     {line expr {optx NOT} BETWEEN expr AND expr}
     {line expr {optx NOT} IN 
            {or
               {line ( {or {} select-stmt {loop expr ,}} )}
               {line /qualified-table-name}
               {line {optx /schema-name .} /table-function
                         ( {or {toploop expr ,} {}} ) }
            }
     }
     {line {optx {optx NOT} EXISTS} ( select-stmt )}
     {line CASE {optx expr} {loop {line WHEN expr THEN expr} {}}
           {optx ELSE expr} END}
     {line raise-function}
  }
  literal-value {
    or
     {line /numeric-literal}
     {line /string-literal}
     {line /blob-literal}
     {line NULL}
  }
  numeric-literal {
    or
     {line {or
              {line {loop /digit nil} {opt /decimal-point {loop nil /digit}}}
              {line /decimal-point {loop /digit nil}}
           }
           {opt E {or nil + -} {loop /digit nil}}}
     {line \"0x\" {loop /hexdigit nil}}
  }
  insert-stmt {
    stack
       {line {opt with-clause}
          INSERT INTO
       }
       {line {optx /db-name .} /qualified-table-name
             {optx ( {loop /column-name ,} )}}
       {or
         {line VALUES {loop {line ( {loop expr ,} )} ,}}
         select-stmt
       }
  }
  select-stmt {
   stack
     {opt {line WITH {opt RECURSIVE} {loop common-table-expression ,}}}
     {loop 
       {or
          {indentstack 2
              {line {or SELECT SELECTV} {or nil DISTINCT ALL}
                                             {loop result-column ,}}
              {optx FROM {or {loop table-or-subquery ,} join-clause}}
              {optx WHERE expr}
              {optx GROUP BY {loop expr ,} {optx HAVING expr}}
          }
          {line VALUES {loop {line ( {toploop expr ,} )} ,}}
       }
       compound-operator
     }
     {optx ORDER BY {loop ordering-term ,}}
     {optx LIMIT expr {optx {or OFFSET ,} expr}}
  }
  join-clause {
    line
      table-or-subquery
      {opt {loop {line join-operator table-or-subquery join-constraint}}}
  }
  select-core {
     or
        {indentstack 2
            {line SELECT {or nil DISTINCT ALL}
                                           {loop result-column ,}}
            {optx FROM {or {loop table-or-subquery ,} join-clause}}
            {optx WHERE expr}
            {optx GROUP BY {loop expr ,} {optx HAVING expr}}
        }
        {line VALUES {loop {line ( {toploop expr ,} )} ,}}
  }
  factored-select-stmt {
    stack
      {opt {line WITH {opt RECURSIVE} {loop common-table-expression ,}}}
      {line {loop select-core compound-operator}}
      {optx ORDER BY {loop ordering-term ,}}
      {optx LIMIT expr {optx {or OFFSET ,} expr}}
  }
  simple-select-stmt {
    stack
      {opt {line WITH {opt RECURSIVE} {loop common-table-expression ,}}}
      {line select-core {stack
                           {optx ORDER BY {loop ordering-term ,}}
                           {optx LIMIT expr {optx {or OFFSET ,} expr}}}}
  }
  compound-select-stmt {
    stack
      {opt {line WITH {opt RECURSIVE} {loop common-table-expression ,}}}
      {line select-core {loop
                    {line {or UNION {line UNION ALL} INTERSECT EXCEPT}
                          select-core} nil}}
      {optx ORDER BY {loop ordering-term ,}}
      {optx LIMIT expr {optx {or OFFSET ,} expr}}
  }
  table-or-subquery {
     or
       {stack
          qualified-table-name
       }
       {rightstack
          {line
             {opt /database .} /table-function-name
             ( {or {toploop expr ,} {}} )
          }
          {line
            {opt {optx AS} /table-alias}
          }
       }
       {line ( {or {loop table-or-subquery ,} join-clause} )}
       {line
          ( select-stmt ) {opt {optx AS} /table-alias}
       }
  }
  result-column {
     or
        {line expr {opt {optx AS} /column-alias}}
        *
        {line qualified-table-name . *}
  }
  join-operator {
     or
        {line ,}
        {line
            {opt NATURAL}
            {or  nil {line LEFT {or OUTER nil}} INNER CROSS}
            JOIN
        }
  }
  join-constraint {
     or
        {line ON expr}
        {line USING ( {loop /column-name ,} )}
        nil
  }
  ordering-term {
      line expr {opt COLLATE /collation-name} {or nil ASC DESC} 
  }
  compound-operator {
     or UNION {line UNION ALL} INTERSECT EXCEPT
  }
  update-stmt-limited {
     stack
        {line {opt with-clause} UPDATE qualified-table-name}
        {line SET {loop {line /column-name = expr} ,} {optx WHERE expr}}
        {optx
            {stack
              {optx ORDER BY {loop ordering-term ,}}
              {line LIMIT expr {optx {or OFFSET ,} expr}}
            }
        }
  }
  qualified-table-name {
     line {optx /database .} /table-name
  }
  comment-syntax {
    or
      {line -- {loop nil /anything-except-newline} 
           {or /newline /end-of-input}}
      {line /* {loop nil /anything-except-*/}
           {or */ /end-of-input}}
  }
  create-table {stack
    {line CREATE TABLE {opt IF NOT EXISTS}}
    {line /table-name {opt table-options}}
    {line lbrc /table-schema rbrc }
  }
  alter-table {stack
    {line ALTER TABLE /table-name {opt table-options}}
    {line lbrc /table-schema rbrc }
  }

  table-options {
    line OPTIONS {loop {or 
      {line ODH OFF}
      {line IPU OFF}
      {line ISC OFF}
      {line REBUILD}
      {line REC {or CRLE LZ4 RLE ZLIB}}
      {line BLOBFIELD {or LZ4 RLE ZLIB}}
    } ,} 
  }

  create-proc {stack
    {line CREATE PROCEDURE /procedure-name}
    {opt {line VERSION /string-literal }}
    {line lbrc /lua-src rbrc}
  }
  create-trigger {stack
    {line CREATE LUA {or TRIGGER CONSUMER}}
    {line /procedure-name ON {loop {line ( table-event )} ,}}
  }
  table-event {stack
    {line TABLE /table-name FOR }
    {loop {line
      {or INSERT UPDATE DELETE}
      {opt {line OF {loop /column-name ,}}}} AND}
  }
  create-lua-func {
    line CREATE LUA {or {SCALAR} {AGGREGATE}} FUNCTION /procedure-name
  }
  create-time-part {stack
    {line CREATE TIME PARTITION ON /table-name}
    {line AS /partition-name PERIOD {or DAILY WEEKLY MONTHLY YEARLY}}
    {line RETENTION /numeric-literal START /datetime-literal}
  }
  drop {
    line DROP {or
      {line TABLE {opt IF EXISTS} /table-name}
      {line PROCEDURE /procedure-name {or /string-literal /numeric-literal}}
      {line LUA {or
        {line {or TRIGGER CONSUMER} /procedure-name}
        {line {or SCALAR AGGREGATE} FUNCTION /procedure-name}}}
      {line TIME PARTITION /partition-name}
      {line SEQUENCE /sequence-name}
    }
  }
  truncate {
    line TRUNCATE TABLE /table-name
  }
  analyze {stack
    {line ANALYZE {or /table-name ALL} {opt /percent-coverage}}
    {opt {line OPTIONS {loop {or 
      {line THREADS /num-of-table-threads}
      {line SUMMARIZE /num-of-summarize-threads}
    } ,} }}
  }
  grant-revoke {stack
    {line {or GRANT REVOKE}}
    {or
      {line {or READ WRITE} ON /table-name TO /user-name}
      {line OP TO /user-name}}
  }
  rebuild {
    line REBUILD {or {} {line INDEX /index-name} DATA DATABLOB } /table-name
  }
  get {
    line GET {or
      {line ALIAS /table-name}
      {line ANALYZE {or COVERAGE THRESHOLD} /table-name}
    }
  }
  put {
    line PUT {or
      {line ANALYZE {or COVERAGE THRESHOLD} /table-name /numeric-literal}
      {line DEFAULT PROCEDURE /procedure-name {or /string-literal /numeric-literal}}
      {line ALIAS /local-table-name /remote-table-name}
      {line PASSWORD {or OFF /string-literal} FOR /user-name}
      {line AUTHENTICATION {or ON OFF}}
      {line TIME PARTITION /partition-name RETENTION /numeric-literal}
    }
  }
  set-stmt {
      line SET {or
          {line TRANSACTION {or
              {line READ COMMITTED} 
              {line SNAPSHOT}
              {line SERIALIZABLE}
          }}
          {line TIMEZONE /timezone-name} 
          {line DATETIME PRECISION {or M U}}
          {line USER /user-name}
          {line PASSWORD /password}
          {line SPVERSION /procedure-name /default-version}
          {line READONLY}
          {line HASQL {or ON OFF}}
          {line REMOTE /database {or READ WRITE EXECUTE} HUH}
          {line GETCOST {or ON OFF}}
          {line MAXTRANSIZE /numeric-literal}
          {line PLANNEREFFORT
      }
    }
  }
  exec-procedure {
      line EXEC PROCEDURE /procedure-name ( {opt {line
              {loop
              {line /argument } ,
      }}} ) 
  }
  host-config {
      line "@" {loop {line
          /machine-name {opt
          {line : {loop
                      {or {line dc = /datacenter-name}
                          {line port = /port-number}
                      } : }}}}  ,}
  }

  constant-section {
      loop
      {line
          /name = /integer-literal
      }
  }

  schema {
      line SCHEMA lbrc schema-section rbrc {loop
          {opt
              {or
                  {line KEYS lbrc key-section rbrc}
                  {line CONSTANTS lbrc constant-section rbrc}
                  {line CONSTRAINTS lbrc constraint-section rbrc}
              }
          }
      }
  }
  schema-section {
      loop
      {line
          {or 
              {line
                  {or
                      {line u_short}
                      {line short}
                      {line u_int}
                      {line int}
                      {line longlong}
                      {line datetime}
                      {line datetimeus}
                      {line intervalds}
                      {line intervaldsus}
                      {line intervalym}
                      {line decimal32}
                      {line decimal64}
                      {line decimal128}
                  }
                  {line /column-name}
              }
              {line cstring /column-name [ size ] }
              {line
                  {or
                      {line vutf8}
                      {line blob}
                      {line byte}
                  }
                  {line /column-name {opt [ size ] }}
              }
          }
          {line {opt DBSTORE = /literal-value}}
      }
  }

  key-section {
      loop
      {line
          {opt dup}
          {opt datacopy}
          {line
              /string-literal =
              {loop {line
                        {opt <DESCEND>}
                        {or
                            {line column-name}
                            {line ( idxexpr-type ) '' expr ''}
                        }
                    }
                    +
              }
              {opt lbrc where expr rbrc}
          }
      }
  }

  idxexpr-type {
      line
      {or
          {line u_short}
          {line short}
          {line u_int}
          {line int}
          {line longlong}
          {line datetime}
          {line datetimeus}
          {line intervalds}
          {line intervaldsus}
          {line intervalym}
          {line decimal32}
          {line decimal64}
          {line decimal128}
          {line byte [ size ]}
          {line cstring [ size ] }
      }
  }

  constraint-section {
      loop {line /keyname -> /table-name : /keyname}
  }

  table-event {
      line
          ( TABLE /table-name FOR 
            {loop
               {or {line INSERT {opt {line OF ID {opt {loop , ID}}}}}
                   {line UPDATE {opt {line OF ID {opt {loop , ID}}}}}
                   {line DELETE {opt {line OF ID {opt {loop , ID}}}}}
                   }
            }
          ) 

          {opt {loop {line
              ,
              more-table-events
          }
          }
          }
  }

  create-table-ddl {
      stack
      {line CREATE TABLE {opt IF NOT EXISTS}}
      {line {opt db-name .} table-name}
      {line (
          {loop
              {line column-name column-type
                  {opt {loop { column-constraint } { , } } } }
              { , }
          }
          {line ) }
      }
      {loop {line table-constraint } { , } }
      {line {opt table-options }}
  }

  column-constraint {
      or
      {line DEFAULT expr }
      {line NULL }
      {line NOT NULL }
      {line PRIMARY KEY {opt {or {line ASC } {line DESC } } } }
      {line UNIQUE }
      {line REFERENCES ref-table-name ( ref-column-name ) }
      {line WITH DBPAD = signed-number }
  }

  table-constraint {
      or
      {line PRIMARY KEY ( column-list ) }
      {line UNIQUE ( column-list ) }
      {stack
          {line FOREIGN KEY ( column-list ) }
          {line REFERENCES ref-table-name ( ref-column-list ) }
      }
  }

  column-list {
      loop
      {line column-name {opt {or {line ASC } {line DESC } } } }
      { , }
  }

  alter-table-ddl {
      stack
      {line ALTER TABLE {opt db-name .} table-name }
      {opt
          {loop
              {or
                  {line ADD column-name column-type
                      {opt {loop {line column-constraint } { , } } }
                  }
                  {line DROP {opt COLUMN} column-name }
              }
              { , }
          }
      }
  }

  create-index {
      stack
      {line CREATE {opt UNIQUE } INDEX {opt IF NOT EXISTS } }
      {line {opt db-name } index-name ON table-name
          ( {loop {line column-name } { , } } ) }
      {line {opt WITH DATACOPY } {opt WHERE expr } }
  }

  drop-index {
      stack
      {line DROP INDEX {opt {line IF EXISTS } } }
      {line index-name {opt {line ON table-name } } }
  }

  create-sequence {
      stack
      {line CREATE SEQUENCE {opt IF NOT EXISTS} sequence-name}
      {opt
            {loop
                {or
                    {line INCREMENT {opt BY} increment}
                    {or
                        {line MINVALUE minimum-value}
                        {line NO MINVALUE}
                    }
                    {or
                        {line MAXVALUE maximum-value}
                        {line NO MAXVALUE}
                    }
                    {line START {opt WITH} start-value}
                    {line CHUNK chunk-size}
                    {line {opt NO} CYCLE}
                }
            }
      }
  }

  alter-sequence {
      stack
      {line ALTER SEQUENCE sequence-name}
      {opt
            {loop
                {or
                    {line INCREMENT {opt BY} increment}
                    {or
                        {line MINVALUE minimum-value}
                        {line NO MINVALUE}
                    }
                    {or
                        {line MAXVALUE maximum-value}
                        {line NO MAXVALUE}
                    }
                    {line START {opt WITH} start-value}
                    {line CHUNK chunk-size}
                    {line {opt NO} CYCLE}
                    {or
                        {line RESTART}
                        {line RESTART {opt WITH} restart-value}
                    }  
                }
            }
      }
  }
}
