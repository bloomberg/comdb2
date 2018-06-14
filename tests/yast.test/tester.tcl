# 2001 September 15
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements some common TCL routines used for regression
# testing the SQLite library
#
# $Id: tester.tcl,v 1.143 2009/04/09 01:23:49 drh Exp $

#-------------------------------------------------------------------------
# The commands provided by the code in this file to help with creating 
# test cases are as follows:
#
# Commands to manipulate the db and the file-system at a high level:
#
#      copy_file              FROM TO
#      drop_all_tables        ?DB?
#      forcedelete            FILENAME
#
# Test the capability of the SQLite version built into the interpreter to
# determine if a specific test can be run:
#
#      ifcapable              EXPR
#
# Calulate checksums based on database contents:
#
#      dbcksum                DB DBNAME
#      allcksum               ?DB?
#      cksum                  ?DB?
#
# Commands to execute/explain SQL statements:
#
#      stepsql                DB SQL
#      execsql2               SQL
#      explain_no_trace       SQL
#      explain                SQL ?DB?
#      catchsql               SQL ?DB?
#      execsql                SQL ?DB?
#
# Commands to run test cases:
#
#      do_ioerr_test          TESTNAME ARGS...
#      crashsql               ARGS...
#      integrity_check        TESTNAME ?DB?
#      do_test                TESTNAME SCRIPT EXPECTED
#      do_execsql_test        TESTNAME SQL EXPECTED
#      do_catchsql_test       TESTNAME SQL EXPECTED
#
# Commands providing a lower level interface to the global test counters:
#
#      set_test_counter       COUNTER ?VALUE?
#      omit_test              TESTNAME REASON
#      fail_test              TESTNAME
#      incr_ntest
#
# Command run at the end of each test file:
#
#      finish_test
#
# Commands to help create test files that run with the "WAL" and other
# permutations (see file permutations.test):
#
#      wal_is_wal_mode
#      wal_set_journal_mode   ?DB?
#      wal_check_journal_mode TESTNAME?DB?
#      permutation
#      presql
#

# Set the precision of FP arithmatic used by the interpreter. And 
# configure SQLite to take database file locks on the page that begins
# 64KB into the database file instead of the one 1GB in. This means
# the code that handles that special case can be tested without creating
# very large database files.
#
set tcl_precision 15
#sqlite3_test_control_pending_byte 0x0010000

set test_name [string map {".test" ""} $argv0]
set comdb2_name [lindex $argv 0]
set file_path [lindex $argv 1]
set cdb2_tcl [lindex $argv 2]
set cdb2_config [lindex $argv 3]
set cdb2_log_file [lindex $argv 4]
set cdb2_debug [string is true -strict [lindex $argv 5]]
set cdb2_trace [string is true -strict [lindex $argv 6]]
set cdb2_trace_to_log [string is true -strict [lindex $argv 7]]
set cdb2_trace_raw_values [string is true -strict [lindex $argv 8]]
set current_test_name ""
set cluster ""
set gbl_scan -99
set gbl_sort -99
set gbl_count -99
set gbl_find -99
set gbl_schemachange_delay 10

proc try_for_tclcdb2_package {} {
    set directory $::cdb2_tcl
    if {![info exists ::auto_path] || [lsearch -exact $::auto_path $directory] == -1} {
        lappend ::auto_path $directory
    }
    package require tclcdb2
}

try_for_tclcdb2_package

proc maybe_null_value { value null } {
    #
    # WARNING: Please do not "simplify" this to use the
    #          [expr] command with the ternary operator
    #          as that may cause bogus type conversions.
    #
    if {$null} {return NULL} else {return $value}
}

proc maybe_quote_value { db index format } {
    if {[catch {cdb2 colvalue $db $index} value] == 0} {
        set null false
    } elseif {[string trim $value] eq "invalid column value"} {
        set null true
    } else {
        error $value; # FAIL: Unknown error.
    }
    set type [cdb2 coltype $db $index]
    if {$::cdb2_trace_raw_values} {
        maybe_trace "\{[info level [info level]]\} has type \{$type\} and [expr {$null ? {NULL } : {}}]value \{$value\}..."
    }
    switch -exact $type {
        integer {
            return [expr {$null ? "NULL" : $value}]
        }
        real {
            if {$null} {
                return NULL
            } else {
                return [format %f $value]
            }
        }
        datetime -
        datetimeus -
        intervalds -
        intervaldsus -
        intervalym {
            set wrap ""
            switch -exact $format {
                csv -
                tabs {
                    #
                    # NOTE: Apparently, these types require the
                    #       value be wrapped in quotes.
                    #
                    set value [string map [list \" \\\"] $value]
                    set wrap \"
                }
                list {
                    # do nothing, handled below.
                }
                default {
                    error "unknown struct value format \"$format\""
                }
            }
            if {$format eq "list"} {
                return [maybe_null_value $value $null]
            } else {
                return [maybe_null_value $wrap$value$wrap $null]
            }
        }
        default {
            set wrap ""
            switch -exact $format {
                csv {
                    set value [string map [list ' ''] $value]
                    set wrap '
                }
                tabs -
                list {
                    # do nothing, handled below.
                }
                default {
                    error "unknown string/blob value format \"$format\""
                }
            }
            if {$format eq "list"} {
                return [maybe_null_value $value $null]
            } else {
                return [maybe_null_value $wrap$value$wrap $null]
            }
        }
    }
}

proc delay_for_schema_change {} {
    after $::gbl_schemachange_delay
}

proc isBinary { value } {
    return [regexp -- {[^\t\n\v\f\r[:print:]]} $value]
}

proc maybe_append_to_log_file { message } {
    set fileName $::cdb2_log_file
    if {[string length $fileName] > 0} {
        set channel [open $fileName {WRONLY APPEND CREAT}]
        fconfigure $channel -encoding binary -translation binary
        puts -nonewline $channel $message
        close $channel
    }
    return ""
}

proc maybe_append_query_to_log_file { sql dbName tier } {
    set formatted "SQL \{$sql\}"
    if {$dbName ne $::comdb2_name} {
        append formatted " against NON-DEFAULT database \"$::comdb2_name\""
    }
    if {$tier ne "default"} {
        append formatted " on NON-DEFAULT tier \"$tier\""
    }
    append formatted \n
    return [maybe_append_to_log_file $formatted]
}

proc maybe_trace { message } {
    if {$::cdb2_trace} {
        set formatted "\[TCL_CDB2_TRACE\]: $message\n"
        if {$::cdb2_trace_to_log} {
            maybe_append_to_log_file $formatted
        } else {
            puts -nonewline stdout $formatted
        }
    }
}

proc grab_cdb2_results { db varName {format csv} } {
    set list [expr {$format eq "list"}]
    set csv [expr {$format eq "csv"}]
    set tabs [expr {$format eq "tabs"}]
    if {[string length $varName] > 0} {upvar 1 $varName result}
    set once false
    while {[cdb2 next $db]} {
        if {$list} {
            set row [list]
            for {set index 0} {$index < [cdb2 colcount $db]} {incr index} {
                lappend row [maybe_quote_value $db $index $format]
            }
            lappend result $row
        } else {
            if {$once || [string length $result] > 0} {append result \n}
            if {$csv} {append result \(}
            for {set index 0} {$index < [cdb2 colcount $db]} {incr index} {
                if {$index > 0} {append result [expr {$csv ? ", " : "\t"}]}
                set value [maybe_quote_value $db $index $format]
                if {$csv} {
                    #
                    # WARNING: String append here, not list element append.
                    #
                    append result [cdb2 colname $db $index]=$value
                } else {
                    #
                    # WARNING: String append here, not list element append.
                    #
                    append result $value
                }
            }
            if {$csv} {append result \)}
        }
        set once true
    }
}

proc do_cdb2_defquery { sql {format csv} {costVarName ""} } {
    return [uplevel 1 [list do_cdb2_query $::comdb2_name $sql default $format $costVarName]]
}

proc do_cdb2_query { dbName sql {tier default} {format csv} {costVarName ""} } {
    if {[string index $sql 0] eq "#"} {return}
    maybe_append_query_to_log_file $sql $dbName $tier
    set doCost [expr {[string length $costVarName] > 0}]

    cdb2 configure $::cdb2_config true
    set db [cdb2 open $dbName $tier]
    if {$::cdb2_debug} {cdb2 debug $db}
    if {$doCost} {cdb2 run $db "SET GETCOST ON"}

    set sql [string map [list \r\n \n] [string trim $sql]]

    set result ""; cdb2 run $db $sql; grab_cdb2_results $db result $format
    set effects [cdb2 effects $db]

    if {$doCost} {
        upvar 1 $costVarName cost
        set cost ""; cdb2 run $db "SELECT comdb2_prevquerycost() AS Cost"
        grab_cdb2_results $db cost tabs
    }

    if {$::cdb2_trace} {
        if {[isBinary $result]} {set trace_result <binary>} else {set trace_result $result}
        maybe_trace "\{[info level [info level]]\} had effects \{$effects\}, returning \{$trace_result\}..."
    }

    cdb2 close $db
    return $result
}

# If the pager codec is available, create a wrapper for the [sqlite3] 
# command that appends "-key {xyzzy}" to the command line. i.e. this:
#
#     sqlite3 db test.db
#
# becomes
#
#     sqlite3 db test.db -key {xyzzy}
#
#if {[info command sqlite_orig]==""} {
#  rename sqlite3 sqlite_orig
#  proc sqlite3 {args} {
#    if {[llength $args]>=2 && [string index [lindex $args 0] 0]!="-"} {
#      # This command is opening a new database connection.
#      #
#      if {[info exists ::G(perm:sqlite3_args)]} {
#        set args [concat $args $::G(perm:sqlite3_args)]
#      }
#      if {[sqlite_orig -has-codec] && ![info exists ::do_not_use_codec]} {
#        lappend args -key {xyzzy}
#      }
#
#      set res [uplevel 1 sqlite_orig $args]
#      if {[info exists ::G(perm:presql)]} {
#        [lindex $args 0] eval $::G(perm:presql)
#      }
#      set res
#    } else {
#      # This command is not opening a new database connection. Pass the 
#      # arguments through to the C implemenation as the are.
#      #
#      uplevel 1 sqlite_orig $args
#    }
#  }
#}

# Do an SQL statement.  Append the search count to the end of the result.
#
proc count {sql} {
  return [execsql $sql count]
}

# This procedure executes the SQL.  Then it checks to see if the OP_Sort
# opcode was executed.  If an OP_Sort did occur, then "sort" is appended
# to the result.  If no OP_Sort happened, then "nosort" is appended.
#
# This procedure is used to check to make sure sorting is or is not
# occurring as expected.
#
proc cksort {sql} {
  return [execsql $sql cksort]
}

proc do_it_to_the_index_name {origidx} {
  set idx ""
  set found [regexp -nocase {^\$(sqlite_autoindex_[a-z0-9]+_[0-9]+)_.+$} $origidx _ idx]
  if {$found == 0} {
   regexp {^\$([[:alnum:]]+)_.+} $origidx _ idx
  }
  return [string tolower $idx]
}

proc patch_query_plan {idx orig} {
  if {[string match "sqlite_autoindex_*" $idx]} {
    return "\{\}"
  }
  return $orig
}

proc get_prefix {} {
  global file_path
  global test_name
  return "$file_path/$test_name"
}

proc get_file_name {name} {
  global current_test_name
  set prefix [get_prefix]
  set timestamp [clock click -milliseconds]
  return "$prefix/$name.$current_test_name.$timestamp"
}

proc get_csc2_file_name {name} {
  set f [get_file_name $name]
  return "$f.csc2"
}

# This procedure executes the SQL.  Then it appends to the result the
# "sort" or "nosort" keyword (as in the cksort procedure above) then
# it appends the ::sqlite_query_plan variable.
proc queryplan {sql} {
  set data [execsql $sql cksort]
  set plans [do_cdb2_defquery "explain query plan $sql"]
  set plans [split $plans "\n"]

  foreach plan $plans {
    set as ""
    set idx ""
    set table ""
    set detail ""

    regexp {^\(.*detail='(.+)'\)$} $plan _ detail
    #puts stderr "detail... $detail"
    set found [regexp {^SEARCH TABLE ([[:alnum:]]+) AS ([[:alnum:]]+) USING(?: COVERING)?? INDEX ([$_[:alnum:]]+) .*$} $detail _ table as idx]
    if {$found} {
      set idx [do_it_to_the_index_name $idx]
      set as [patch_query_plan $idx $as]
      set data [concat $data $as $idx]
      continue
    }

    set found [regexp {^USE TEMP B-TREE FOR(?: RIGHT PART OF)?? ORDER BY$} $detail ]
    if {$found} {
      continue
    }

    set found [regexp {^COMPOUND SUBQUERIES [[:alnum:]]+ .*$} $detail ]
    if {$found} {
      continue
    }

    set found [regexp {^EXECUTE LIST SUBQUERY [[:alnum:]]+$} $detail ]
    if {$found} {
      continue
    }

    set found [regexp {^SEARCH TABLE ([[:alnum:]]+) USING(?: COVERING)?? INDEX ([$_[:alnum:]]+) .*$} $detail _ table idx]
    if {$found} {
      set idx [do_it_to_the_index_name $idx]
      #AZ matches more without this: set table [patch_query_plan $idx $table]
      set data [concat $data $table $idx]
      continue
    }

    set found [regexp {^SEARCH TABLE ([[:alnum:]]+) USING INTEGER PRIMARY KEY.*$} $detail _ table]
    if {$found} {
      set data [concat $data $table "*"]
      continue
    }

    set found [regexp {^SCAN TABLE ([[:alnum:]]+) USING INTEGER PRIMARY KEY .*$} $detail _ table]
    if {$found} {
      set data [concat $data $table "*"]
      continue
    }

    set found [regexp {^SCAN TABLE ([[:alnum:]]+) AS ([[:alnum:]]+) .*$} $detail _ table as]
    if {$found} {
      set data [concat $data $as "*"]
      continue
    }

    set found [regexp {^SCAN TABLE ([[:alnum:]]+)$} $detail _ table]
    if {$found} {
      set data [concat $data $table "*"]
      continue
    }

    set found [regexp {^SCAN TABLE ([[:alnum:]]+) .*$} $detail _ table]
    if {$found} {
      set data [concat $data $table "*"]
      continue
    }

    set data [concat $data $plan]
  }
  return $data
}

proc eqp {sql} {
  queryplan $sql
}

# Evaluate SQL.  Return the result set followed by the
# and the number of full-scan steps.
#
proc count_steps {sql} {
  global gbl_scan
  global gbl_sort
  set r [execsql $sql count_steps]
  lappend r scan $gbl_scan sort $gbl_sort
}

proc count_steps_sort {sql} {
  global gbl_scan
  global gbl_sort
  set r [lsort -integer [execsql $sql count_steps]]
  return "$r scan $gbl_scan sort $gbl_sort"
}

proc execsql_status {sql} {
  global gbl_scan
  global gbl_sort
  #set r [lsort [execsql $sql count_steps]]
  set r [execsql $sql count_steps]
  return "$r $gbl_scan $gbl_sort"
}

proc execsql_status2 {sql} {
  global gbl_scan
  global gbl_sort
  global gbl_count
  set r [execsql $sql count_steps]
  return "$r $gbl_scan $gbl_sort $gbl_count"
}

proc db {e sql} {
  execsql $sql list_results
}
proc execpresql {handle args} {
  trace remove execution $handle enter [list execpresql $handle]
  if {[info exists ::G(perm:presql)]} {
    $handle eval $::G(perm:presql)
  }
}

# This command should be called after loading tester.tcl from within
# all test scripts that are incompatible with encryption codecs.
#
proc do_not_use_codec {} {
  set ::do_not_use_codec 1
  reset_db
}

# The following block only runs the first time this file is sourced. It
# does not run in slave interpreters (since the ::cmdlinearg array is
# populated before the test script is run in slave interpreters).
#
if {[info exists cmdlinearg]==0} {

  # Parse any options specified in the $argv array. This script accepts the 
  # following options: 
  #
  #   --pause
  #   --soft-heap-limit=NN
  #   --maxerror=NN
  #   --malloctrace=N
  #   --backtrace=N
  #   --binarylog=N
  #   --soak=N
  #
  set cmdlinearg(soft-heap-limit)    0
  set cmdlinearg(maxerror)        1000
  set cmdlinearg(malloctrace)        0
  set cmdlinearg(backtrace)         10
  set cmdlinearg(binarylog)          0
  set cmdlinearg(soak)               0

  set leftover [list]
  foreach a $argv {
    switch -regexp -- $a {
      {^-+pause$} {
        # Wait for user input before continuing. This is to give the user an 
        # opportunity to connect profiling tools to the process.
        puts -nonewline "Press RETURN to begin..."
        flush stdout
        gets stdin
      }
      {^-+soft-heap-limit=.+$} {
        foreach {dummy cmdlinearg(soft-heap-limit)} [split $a =] break
      }
      {^-+maxerror=.+$} {
        foreach {dummy cmdlinearg(maxerror)} [split $a =] break
      }
      {^-+malloctrace=.+$} {
        foreach {dummy cmdlinearg(malloctrace)} [split $a =] break
        if {$cmdlinearg(malloctrace)} {
          sqlite3_memdebug_log start
        }
      }
      {^-+backtrace=.+$} {
        foreach {dummy cmdlinearg(backtrace)} [split $a =] break
        sqlite3_memdebug_backtrace $value
      }
      {^-+binarylog=.+$} {
        foreach {dummy cmdlinearg(binarylog)} [split $a =] break
      }
      {^-+soak=.+$} {
        foreach {dummy cmdlinearg(soak)} [split $a =] break
        set ::G(issoak) $cmdlinearg(soak)
      }
      default {
        lappend leftover $a
      }
    }
  }
  set argv $leftover

  # Install the malloc layer used to inject OOM errors. And the 'automatic'
  # extensions. This only needs to be done once for the process.
  #
#sqlite3_shutdown 
#install_malloc_faultsim 1 
#sqlite3_initialize
#autoinstall_test_functions

  # If the --binarylog option was specified, create the logging VFS. This
  # call installs the new VFS as the default for all SQLite connections.
  #
  if {$cmdlinearg(binarylog)} {
    vfslog new binarylog {} vfslog.bin
  }

  # Set the backtrace depth, if malloc tracing is enabled.
  #
  if {$cmdlinearg(malloctrace)} {
    sqlite3_memdebug_backtrace $cmdlinearg(backtrace)
  }
}



proc comdb2dumpcsctcl {tbl} {
    if {$tbl == 0} {
        return [do_cdb2_defquery "select name from sqlite_master where type='table' and name not like '%sqlite_stat%'" tabs]
    } else {
        return [do_cdb2_defquery "select csc2 from sqlite_master where type='table' and name='$tbl'" tabs]
    }
}


# Update the soft-heap-limit each time this script is run. In that
# way if an individual test file changes the soft-heap-limit, it
# will be reset at the start of the next test file.
#
#sqlite3_soft_heap_limit $cmdlinearg(soft-heap-limit)

# Create a test database
#
proc reset_db {} {
  #catch {db close}
  #file delete -force test.db
  #file delete -force test.db-journal
  #file delete -force test.db-wal
  #sqlite3 db ./test.db
  #set ::DB [sqlite3_connection_pointer db]
  #if {[info exists ::SETUP_SQL]} {
  #  db eval $::SETUP_SQL
  #}

  global file_path
  global test_name
  global cluster


  set tables [comdb2dumpcsctcl 0]
  foreach table $tables {
      set output ""
      set query "DROP TABLE $table"
      set rc [catch {do_cdb2_defquery $query} output]
      if {$rc != 0} {
        puts "failed to drop table: $table \[$output\]"
      }
  }
  set fastinit_stat1 "TRUNCATE sqlite_stat1"
  set fastinit_stat2 "TRUNCATE sqlite_stat2"
  catch {do_cdb2_defquery $fastinit_stat1}

  catch {do_cdb2_defquery $fastinit_stat2}
  exec mkdir -p $file_path/$test_name
}
reset_db

# Abort early if this script has been run before.
#
if {[info exists TC(count)]} return

# Initialize the test counters and set up commands to access them.
# Or, if this is a slave interpreter, set up aliases to write the
# counters in the parent interpreter.
#
if {0==[info exists ::SLAVE]} {
  set TC(errors)    0
  set TC(count)     0
  set TC(fail_list) [list]
  set TC(omit_list) [list]

  proc set_test_counter {counter args} {
    if {[llength $args]} {
      set ::TC($counter) [lindex $args 0]
    }
    set ::TC($counter)
  }
}

proc comdb2_omit_test {_ name args} {
  omit_test $name ""
}

# Record the fact that a sequence of tests were omitted.
#
proc omit_test {name reason} {
  set omitList [set_test_counter omit_list]
  lappend omitList [list $name $reason]
  set_test_counter omit_list $omitList
}

# Record the fact that a test failed.
#
proc fail_test {name} {
  set f [set_test_counter fail_list]
  lappend f $name
  set_test_counter fail_list $f
  set_test_counter errors [expr [set_test_counter errors] + 1]

  set nFail [set_test_counter errors]
  if {$nFail>=$::cmdlinearg(maxerror)} {
    puts "*** Giving up..."
    finalize_testing
  }
}

# Increment the number of tests run
#
proc incr_ntest {} {
  set_test_counter count [expr [set_test_counter count] + 1]
}


# Invoke the do_test procedure to run a single test 
#
proc do_test {name cmd expected} {

  global argv cmdlinearg

#sqlite3_memdebug_settitle $name

#  if {[llength $argv]==0} { 
#    set go 1
#  } else {
#    set go 0
#    foreach pattern $argv {
#      if {[string match $pattern $name]} {
#        set go 1
#        break
#      }
#    }
#  }

  if {[info exists ::G(perm:prefix)]} {
    set name "$::G(perm:prefix)$name"
  }

  incr_ntest

  maybe_append_to_log_file "\n==================================== [file tail [info script]] / $name ====================================\n"

  #puts -nonewline $name...
  #flush stdout
  puts $name...
  global current_test_name
  set current_test_name $name

  if {[catch {uplevel #0 "$cmd;\n"} result]} {
    puts "\nError: $result $cmd"
    fail_test $name
  #AZ
  flush stdout
  #exit 1
  } elseif {[string compare $result $expected]} {
    puts "\nExpected: \[$expected\]\n     Got: \[$result\]"
    fail_test $name
  #AZ
  flush stdout
  #exit 1
  } else {
    puts " Ok"
  }
  flush stdout
}
    
proc do_execsql_test {testname sql result} {
  set r {}
  foreach x $result {lappend r $x}
  uplevel do_test $testname [list "execsql {$sql}"] [list $r]
}
proc do_catchsql_test {testname sql result} {
  uplevel do_test $testname [list "catchsql {$sql}"] [list $result]
}
proc do_eqp_test {name sql res} {
  set r {}
  foreach x $res {lappend r $x}
  uplevel do_execsql_test $name [list "EXPLAIN QUERY PLAN $sql"] [list $r]
}


# Run an SQL script.  
# Return the number of microseconds per statement.
#
proc speed_trial {name numstmt units sql} {
  puts -nonewline [format {%-21.21s } $name...]
  flush stdout
  set speed [time {sqlite3_exec_nr db $sql}]
  set tm [lindex $speed 0]
  if {$tm == 0} {
    set rate [format %20s "many"]
  } else {
    set rate [format %20.5f [expr {1000000.0*$numstmt/$tm}]]
  }
  set u2 $units/s
  puts [format {%12d uS %s %s} $tm $rate $u2]
  global total_time
  set total_time [expr {$total_time+$tm}]
}
proc speed_trial_tcl {name numstmt units script} {
  puts -nonewline [format {%-21.21s } $name...]
  flush stdout
  set speed [time {eval $script}]
  set tm [lindex $speed 0]
  if {$tm == 0} {
    set rate [format %20s "many"]
  } else {
    set rate [format %20.5f [expr {1000000.0*$numstmt/$tm}]]
  }
  set u2 $units/s
  puts [format {%12d uS %s %s} $tm $rate $u2]
  global total_time
  set total_time [expr {$total_time+$tm}]
}
proc speed_trial_init {name} {
  global total_time
  set total_time 0
  sqlite3 versdb :memory:
  set vers [versdb one {SELECT sqlite_source_id()}]
  versdb close
  puts "SQLite $vers"
}
proc speed_trial_summary {name} {
  global total_time
  puts [format {%-21.21s %12d uS TOTAL} $name $total_time]
}

# Run this routine last
#
proc finish_test {} {
  #catch {db close}
  #catch {db2 close}
  #catch {db3 close}
  #if {0==[info exists ::SLAVE]} { finalize_testing }

  finalize_testing 
}
proc finalize_testing {} {
  #global sqlite_open_file_count

  set omitList [set_test_counter omit_list]

  #catch {db close}
  #catch {db2 close}
  #catch {db3 close}

  #vfs_unlink_test
  #sqlite3 db {}
  # sqlite3_clear_tsd_memdebug
  #db close
  #sqlite3_reset_auto_extension

  #sqlite3_soft_heap_limit 0
  set nTest [incr_ntest]
  set nErr [set_test_counter errors]

  puts "$nErr errors out of $nTest tests"
  if {$nErr>0} {
    puts "Failures on these tests: [set_test_counter fail_list]"
  }
  #run_thread_tests 1
  if {[llength $omitList]>0} {
    puts "Omitted test cases:"
    set prec {}
    foreach {rec} [lsort $omitList] {
      if {$rec==$prec} continue
      set prec $rec
      puts [format {  %-12s %s} [lindex $rec 0] [lindex $rec 1]]
    }
  }
  #if {$nErr>0 && ![working_64bit_int]} {
  #  puts "******************************************************************"
  #  puts "N.B.:  The version of TCL that you used to build this test harness"
  #  puts "is defective in that it does not support 64-bit integers.  Some or"
  #  puts "all of the test failures above might be a result from this defect"
  #  puts "in your TCL build."
  #  puts "******************************************************************"
  #}
  #if {$::cmdlinearg(binarylog)} {
  #  vfslog finalize binarylog
  #}
  #if {$sqlite_open_file_count} {
  #  puts "$sqlite_open_file_count files were left open"
  #  incr nErr
  #}
  #if {[lindex [sqlite3_status SQLITE_STATUS_MALLOC_COUNT 0] 1]>0 ||
  #            [sqlite3_memory_used]>0} {
  #  puts "Unfreed memory: [sqlite3_memory_used] bytes in\
  #       [lindex [sqlite3_status SQLITE_STATUS_MALLOC_COUNT 0] 1] allocations"
  #  incr nErr
  #  ifcapable memdebug||mem5||(mem3&&debug) {
  #    puts "Writing unfreed memory log to \"./memleak.txt\""
  #    sqlite3_memdebug_dump ./memleak.txt
  #  }
  #} else {
  #  puts "All memory allocations freed - no leaks"
  #  ifcapable memdebug||mem5 {
  #    sqlite3_memdebug_dump ./memusage.txt
  #  }
  #}
  #show_memstats
  #puts "Maximum memory usage: [sqlite3_memory_highwater 1] bytes"
  #puts "Current memory usage: [sqlite3_memory_highwater] bytes"
  #if {[info commands sqlite3_memdebug_malloc_count] ne ""} {
  #  puts "Number of malloc()  : [sqlite3_memdebug_malloc_count] calls"
  #}
  #if {$::cmdlinearg(malloctrace)} {
  #  puts "Writing mallocs.sql..."
  #  memdebug_log_sql
  #  sqlite3_memdebug_log stop
  #  sqlite3_memdebug_log clear

  #  if {[sqlite3_memory_used]>0} {
  #    puts "Writing leaks.sql..."
  #    sqlite3_memdebug_log sync
  #    memdebug_log_sql leaks.sql
  #  }
  #}
  #foreach f [glob -nocomplain test.db-*-journal] {
  #  file delete -force $f
  #}
  #foreach f [glob -nocomplain test.db-mj*] {
  #  file delete -force $f
  #}
  exit [expr {$nErr>0}]
}

# Display memory statistics for analysis and debugging purposes.
#
proc show_memstats {} {
  set x [sqlite3_status SQLITE_STATUS_MEMORY_USED 0]
  set y [sqlite3_status SQLITE_STATUS_MALLOC_SIZE 0]
  set val [format {now %10d  max %10d  max-size %10d} \
              [lindex $x 1] [lindex $x 2] [lindex $y 2]]
  puts "Memory used:          $val"
  set x [sqlite3_status SQLITE_STATUS_MALLOC_COUNT 0]
  set val [format {now %10d  max %10d} [lindex $x 1] [lindex $x 2]]
  puts "Allocation count:     $val"
  set x [sqlite3_status SQLITE_STATUS_PAGECACHE_USED 0]
  set y [sqlite3_status SQLITE_STATUS_PAGECACHE_SIZE 0]
  set val [format {now %10d  max %10d  max-size %10d} \
              [lindex $x 1] [lindex $x 2] [lindex $y 2]]
  puts "Page-cache used:      $val"
  set x [sqlite3_status SQLITE_STATUS_PAGECACHE_OVERFLOW 0]
  set val [format {now %10d  max %10d} [lindex $x 1] [lindex $x 2]]
  puts "Page-cache overflow:  $val"
  set x [sqlite3_status SQLITE_STATUS_SCRATCH_USED 0]
  set val [format {now %10d  max %10d} [lindex $x 1] [lindex $x 2]]
  puts "Scratch memory used:  $val"
  set x [sqlite3_status SQLITE_STATUS_SCRATCH_OVERFLOW 0]
  set y [sqlite3_status SQLITE_STATUS_SCRATCH_SIZE 0]
  set val [format {now %10d  max %10d  max-size %10d} \
               [lindex $x 1] [lindex $x 2] [lindex $y 2]]
  puts "Scratch overflow:     $val"
  ifcapable yytrackmaxstackdepth {
    set x [sqlite3_status SQLITE_STATUS_PARSER_STACK 0]
    set val [format {               max %10d} [lindex $x 2]]
    puts "Parser stack depth:    $val"
  }
}

proc create_table_as {origquery} {
  global cluster

  regexp -nocase {^CREATE TABLE ([[:alnum:]]+) AS (.*)$} $origquery _ dest srcquery
  regexp -nocase {^.* FROM ([[:alnum:]]+).*$} $srcquery _ src

  set schema [comdb2dumpcsctcl $src]
  set schema [split $schema "\n"]
  
  set csc2schema ""

  set csc2name [get_csc2_file_name $dest]
  set csc2 [open $csc2name w]

  for {set i 0} {$i < [llength $schema]} {incr i} {
    set k [lindex $schema $i]
    if {[string equal $k "keys"]} {
      while {[string equal $k "\}"] == 0} {
        incr i; if {$i >= [llength $schema]} break
        set k [lindex $schema $i]
      }
    } else {
      set csc2schema "$csc2schema $k"
      puts $csc2 $k
    }
  }
  close $csc2

  set rc [catch {do_cdb2_defquery "CREATE TABLE $dest \{$csc2schema\}"} output]
  if {$rc != 0} {
    puts "add table failed for $table rc: $rc $output"
  }

  delay_for_schema_change

  set rc [catch {do_cdb2_defquery "insert into $dest $srcquery"} output]
  if {$rc != 0} {
    puts "failed to populate $dest from $srcquery rc: $rc $output"
  }
}

proc create_table {origquery} {
  global cluster 

  if {[string match -nocase "CREATE TABLE * AS *" $origquery]} {
    return [create_table_as $origquery]
  }

  set uniquekey ""
  set primarykey ""

  #CREATE TABLE t(x, y, UNIQUE(x,y))
  set found [regexp -nocase {,( )*UNIQUE\([^\)]+\)} $origquery uniquekey]
  if {$found} {
    regsub {,( )*UNIQUE\([^\)]+\)} $origquery "" origquery
    regsub {^,( )*UNIQUE\(} $uniquekey "" uniquekey
    regsub {\)$} $uniquekey "" uniquekey
    regsub -all {,} $uniquekey " + " uniquekey
  }

  #CREATE TABLE t(a, b, c, PRIMARY KEY(a,b))
  set found [regexp -nocase {,( )*PRIMARY KEY\([^\)]+\)} $origquery primarykey]
  if {$found} {
    regsub {,( )*PRIMARY KEY\([^\)]+\)} $origquery "" origquery
    regsub {^,( )*PRIMARY KEY\(} $primarykey "" primarykey
    regsub {\)$} $primarykey "" primarykey
    regsub -all {,} $primarykey " + " primarykey
  }

  #CREATE TABLE t(a,b)
  set found [regexp -nocase {^CREATE TABLE ([[:alnum:]]+).*\((.+)\)$} $origquery _ table query]
  if {$found == 0} {
    puts "Can't process: \"$origquery\""
    return
  }
  set fields [split $query ","]

  set csc2name [get_csc2_file_name $table]
  set csc2 [open $csc2name w]

  set csc2schema ""

  append csc2schema "schema\n\{"
  set primary ""
  set unique [list]
  foreach field $fields {
    set field [string trim $field]
    set field [split $field " "]

    set name [lindex $field 0]
    set type [lindex $field 1]
    set strlen ""

    switch [string toupper $type] {
      "" {
        switch $name {
          default {
            set type "int"
          }
        }
      }
      "INTEGER" {
        set type "int"
      }
      "REAL" {
        set type "double"
      }
      "TEXT" {
        set type "cstring"
        set strlen "\[64\]"
      }
      "BLOB" {
        set type "blob"
      }
      "LONGLONG" {
        set type "longlong"
      }
      "PRIMARY" {
        set type "int"
        set field [linsert $field 1 ""]
      }
      "UNIQUE" {
        set type "int"
        set field [linsert $field 1 ""]
      }
      "NVARCHAR" {
        set type "cstring"
        set strlen "\[64\]"
      }
      default {
        set type [string tolower $type]
      }
    }

    set extra0 [lindex $field 2]
    set extra1 [lindex $field 3]
    set extra2 [lindex $field 4]

    set null "null=yes"
    switch [string toupper $extra0] {
      "NOT" {
        set null "null=no"
      }
      "PRIMARY" {
        set null "null=no"
        set primary $name
      }
      "UNIQUE" {
        lappend unique $name
      }
    }
    set line "    $type $name$strlen $null"
    append csc2schema $line "\n"
  }
  append csc2schema "\}"

  if {[string compare $primary ""] != 0 || [llength $unique] > 0 || [string compare $primarykey ""] != 0 || [string compare $uniquekey ""] != 0} {
    append csc2schema "keys\n\{"
    if {[string compare $primary ""] != 0} {
      append csc2schema "    \"PRIMARY\" = $primary"
    }
    set uniq_cnt 1
    foreach u $unique {
      append csc2schema "    \"sqlite_autoindex_${table}_${uniq_cnt}\" = $u"
      incr uniq_cnt
    }
    if {[string compare $uniquekey ""] != 0} {
      append csc2schema "    \"sqlite_autoindex_${table}_${uniq_cnt}\" = $uniquekey"
    }
    if {[string compare $primarykey ""] != 0} {
      append csc2schema "    \"sqlite_autoindex_${table}_${uniq_cnt}\" = $primarykey"
    }
    append csc2schema "\}"
  }
  puts $csc2 $csc2schema
  close $csc2

  set rc [catch {do_cdb2_defquery "CREATE TABLE $table \{$csc2schema \}" } output]
  if {$rc != 0} {
    puts "add table failed for $table rc: $rc $output"
  } 
}

proc create_index {origquery} {
  global cluster
  set index ""
  set table ""
  set query ""
  set where ""
  set dup "    "

  set found [regexp -nocase {^CREATE UNIQUE INDEX ([[:alnum:]_]+) ON ([[:alnum:]_]+)[ ]*\((.+)\)[ ]*(.*)$} $origquery _ index table query where]
  if {$found == 0} {
    set found [regexp -nocase {^CREATE INDEX ([[:alnum:]_]+) ON ([[:alnum:]_]+)[ ]*\((.+)\)[ ]*(.*)$} $origquery _ index table query where]
    if {$found == 0} {
      puts "Can't process: \"$origquery\""
      return
    }
    set dup "dup "
  }
  set where [string map {[ (} $where]
  set where [string map {] )} $where]

  regsub -all {(\{)([^\{]+)(\}),?|([^,\"]+),?|,} $query {{\2\4} } fields
  #set fields [split $query ","]

  set key "$dup\"$index\" = "
  set first 1
  foreach f $fields {
    if {$first == 1} {
      set first 0
      append key $f
    } else {
      append key " + " $f
    }
  }
  if {[string compare $where ""] != 0} {
      append key " {"
      append key $where
      append key "}"
  }

  set schema [comdb2dumpcsctcl $table]
  set schema [split $schema "\n"]
  set in_keys 0
  set done 0

  set csc2name [get_csc2_file_name $table]
  set csc2 [open $csc2name w]
  set csc2schema "\n"

  for {set i 0} {$i < [llength $schema]} {incr i} {
    set k [lindex $schema $i]
    if {[string compare $k ""] == 0} {
      continue
    } elseif {[string equal $k "keys"]} {
      set in_keys 1
      set done 1
    } 
    if {[string equal $k "\}"] && $in_keys} {
      append csc2schema "\n"
      append csc2schema $key
      append csc2schema "\n"
      set in_keys 0
    }
    append csc2schema "\n"
    append csc2schema $k
    append csc2schema "\n"
  }

  if {$done == 0} {
    append csc2schema "keys\n\{\n\t$key\n\}"
  }
  puts $csc2 $csc2schema
  close $csc2

  return [do_cdb2_defquery "ALTER TABLE $table \{$csc2schema\}"]
  # set rc [catch {do_cdb2_defquery "ALTER TABLE $table \{$csc2schema\}"} output]
  # if {$rc != 0} {
  #   puts "failed to add index $index to $table rc:$rc $output"
  # }
}

proc drop_index {origquery} {
  global cluster
  set index ""
  set table ""
  set query ""
  set dup "    "

  # find which table has this index
  set found [regexp -nocase {^DROP INDEX(?: IF EXISTS)?? ([[:alnum:]]+)$} $origquery _ index]
  set find_index [string toupper $index]

  set _ [catch {do_cdb2_defquery "select tbl_name from sqlite_master where type='index' and name like '\$$find_index%'"} output]
  set found [regexp -nocase {^\(tbl_name='([[:alnum:]]+)'\)$} $output _ table]
  if {$found == 0} {
    return
  }

  set schemaorig [comdb2dumpcsctcl $table]
  set schema [split $schemaorig "\n"]
  set in_keys 0
  set done 0
  
  set csc2schema ""

  set csc2name [get_csc2_file_name $table]
  set csc2 [open $csc2name w]
  set keycount 0
  for {set i 0} {$i < [llength $schema]} {incr i} {
    set k [lindex $schema $i]
    if {[string equal $k "keys"]} {
      set in_keys 1
    } 
    if {[string equal $k "\}"] && $in_keys} {
      set in_keys 0
    }
    if {$in_keys} {
      set found [regexp {\"([[:alnum:]]+)\"} $k _ found_index]
      if {$found} {
        if {[string equal $found_index $index]} {
          continue
        }
        incr keycount
      }
    }
    append csc2schema "\n"
    append csc2schema $k
    puts $csc2 $k
  }
  puts $csc2 $csc2schema
  close $csc2

  if {$keycount == 0} {
    set csc2schema ""
    # this table does not have any indexes
    # need to get rid of empty "keys" section
    set in_keys 0
    set csc2 [open $csc2name w]
    for {set i 0} {$i < [llength $schema]} {incr i} {
      set k [lindex $schema $i]
      if {[string compare $k ""] == 0} {
        continue
      } elseif {[string equal $k "keys"]} {
        set in_keys 1
      }

      if {$in_keys} {
        if {[string equal $k "\}"]} {
          set in_keys 0
        }
        continue
      }
      append csc2schema "\n"
      append csc2schema $k
    }
    puts $csc2 $csc2schema 
    #puts "DROPPING no keys so schema should be same as $k and is $csc2schema , but is not $schemaorig"
    close $csc2
  }
  set rc [catch {do_cdb2_defquery "ALTER TABLE $table \{$csc2schema\}"} output]
  if {$rc != 0} {
    puts "failed to drop index $index from $table rc:$rc $output. Schema:\n$schemaorig\n$csc2schema"
  }
}

proc is_sqlite_stat {table} {
  if {[string equal $table "sqlite_stat1"]} {
    return 1
  }
  if {[string equal $table "sqlite_stat2"]} {
    return 1
  }
  return 0
}

# A procedure to execute SQL
#
proc execsql {sql {options ""}} {
  global cluster
  global gbl_schemachange_delay

  set sql [string map [list \n ""] $sql]
  set queries [split $sql \;]
  set r [list]
  variable rc
  variable err
  variable cost

  foreach query $queries {
    set query [string trim $query]
    if {[string compare $query ""] == 0} {
      continue
    }

    if {[string match -nocase "CREATE TABLE*" $query]} {
      create_table $query
      delay_for_schema_change
      continue
    }

    if {[regexp -nocase {^CREATE(?: UNIQUE)?? INDEX.*$} $query]} {
      set rc [catch {create_index $query} err]
      if {$rc != 0} {
        lappend r $rc
        # IBM: failed with rc -3 ORDER BY without LIMIT on DELETE
        # Linux: failed with rc -3 ORDER BY without LIMIT on DELETE
        set found [regexp {failed with rc (?:-)??[[:digit:]]+ (?:OP \#.*\)\: )*(.*)$} $err _ errmsg]
        if {$found} {
          lappend r $errmsg
        } else {
          lappend r $err
        }
      }
      delay_for_schema_change
      continue
    }

    if {[regexp -nocase {^DROP TABLE(?: IF EXISTS)?? ([[:alnum:]]+)$} $query _ table]} {
      set rc [catch {do_cdb2_defquery "DROP TABLE $table"} err]
      delay_for_schema_change
      continue
    }

    if {[regexp -nocase {^DELETE FROM ([[:alnum:]]+)$} $query _ table]} {
      do_cdb2_defquery "TRUNCATE $table"
      delay_for_schema_change
      continue
    }

    if {[regexp -nocase {^DROP INDEX(?: IF EXISTS)?? ([[:alnum:]]+)$} $query]} {
      drop_index $query
      delay_for_schema_change
      continue
    }

    if {[lsearch -exact $options want_results] == -1} {
      if {[regexp -nocase "^(?:DELETE|UPDATE|INSERT|BEGIN|COMMIT|ROLLBACK).*" $query]} {
        set rc [catch {do_cdb2_defquery $query} outputs]
        if {$rc != 0} {lappend r $rc $outputs}
        continue
      }
    }

    set format tabs
    if {[lsearch -exact $options list_results] != -1} {set format list}

    #
    # NOTE: Uncomment this to enable tracing of raw column values coming back
    #     from the Tcl bindings.
    #
    # if {[string equal $::current_test_name "where-17.4"]} {set ::cdb2_trace_raw_values 1}

    set rc 0

    if {[lsearch -exact $options count] != -1
     || [lsearch -exact $options cksort] != -1
     || [lsearch -exact $options count_steps] != -1} {
      set cost ""
      catch {do_cdb2_defquery $query $format cost} outputs
    } else {
      set rc [catch {do_cdb2_defquery $query $format} outputs]
      if {$rc == 0} {set cost ""}
    }

    if {$rc != 0} {
      lappend r $rc
      # IBM: failed with rc -3 ORDER BY without LIMIT on DELETE
      # Linux: failed with rc -3 ORDER BY without LIMIT on DELETE
      set found [regexp {failed with rc (?:-)??[[:digit:]]+ (?:OP \#.*\)\: )*(.*)$} $outputs _ errmsg]
      if {$found} {
        lappend r $errmsg
      } else {
        lappend r $outputs
      }
      continue
    }

    # Debug a specific test case
    #
    #global current_test_name
    #if {[string equal $::current_test_name "where9-2.3"]} {
    #  puts $query
    #  puts $outputs
    #}

    if {[lsearch -exact $options list_results] != -1} {
      #
      # NOTE: The list format is being used.  Treat the resulting output as
      #       a list of rows, where each row is a list of column values.
      #
      foreach output $outputs {
        foreach o $output {
          if {[string equal $o NULL]} {set o ""}
          lappend r $o
        }
      }
    } else {
      set outputs [split $outputs \n]

      set found [regexp -nocase "^.*(DELETE|BEGIN|COMMIT|ROLLBACK).*" $query _ first]
      if {$found == 0} {
        foreach output $outputs {
          regexp {^\((.*)\)$} $output _ output
          set output [split $output \t]
          if {[string equal $output ""]} {
            lappend r $output
            continue
          }
          foreach o $output {
            if {[string equal $o ""]} {
              lappend r $o
              continue
            }
            regexp {^'(.*)'$} $o _ o
            if {[string equal $o NULL]} {
              set o ""
            }
            lappend r $o
          }
        }
      }
    }

    if {[lsearch -exact $options count] != -1
     || [lsearch -exact $options cksort] != -1
     || [lsearch -exact $options count_steps] != -1} {
      set cost [split $cost \n]
    } else {
      continue
    }

    set tbl_find 0
    set tbl_move 0
    set idx_find 0
    set idx_move 0
    set tmp_find 0
    set tmp_move 0
    set sort 0

    foreach c $cost {
      set c [string trim $c]
      set found [regexp {^table ([[:alnum:]]+) finds ([0-9]+) next/prev ([0-9]+).*$} $c _ table count1 count2]
      if {$found} {
        if {[is_sqlite_stat $table]} {
          continue
        }
        incr tbl_find $count1
        incr tbl_move $count2
        continue
      }
      set found [regexp {^table ([[:alnum:]]+) finds ([0-9]+).*$} $c _ table count1]
      if {$found} {
        if {[is_sqlite_stat $table]} {
          continue
        }
        incr tbl_find $count1
        continue
      }
      set found [regexp {^index ([0-9]+) on table ([[:alnum:]]+) finds ([0-9]+) next/prev ([0-9]+).*$} $c _ ixnum table count1 count2]
      if {$found} {
        incr idx_find $count1
        incr idx_move $count2
        continue
      }
      set found [regexp {^temp index finds ([0-9]+) next/prev ([0-9]+).*$} $c _ count1 count2]
      if {$found} {
        set sort 1
        incr tmp_find $count1
        incr tmp_move $count2
        continue
      }
      #AZ we will consider sort if using temp index
      set found [regexp {^temp index finds ([0-9]+).*$} $c _ count1]
      if {$found} {
        set sort 1
        incr tmp_find $count1
        continue
      }
    }

    if {[lsearch -exact $options cksort] != -1} {
      if {$sort} {
        lappend r sort
      } else {
        lappend r nosort
      }
    }

    if {[lsearch -exact $options count_steps] != -1} {
      global gbl_scan
      global gbl_sort
      set gbl_scan $tbl_move
      set gbl_sort $sort
    }

    global gbl_count
    set gbl_count [expr $tbl_find + $tbl_move + $idx_find + $idx_move]
    if {[lsearch -exact $options count] != -1} {
      lappend r $gbl_count
    }

    global gbl_find
    set gbl_find [expr $tbl_find + $idx_find + $tmp_find]
  }
  return $r
}

# Execute SQL and catch exceptions.
#
proc catchsql {sql} {
  return [execsql $sql want_results]
}

# Do an VDBE code dump on the SQL given
#
proc explain {sql {db db}} {
  puts ""
  puts "addr  opcode        p1      p2      p3      p4               p5  #"
  puts "----  ------------  ------  ------  ------  ---------------  --  -"
  $db eval "explain $sql" {} {
    puts [format {%-4d  %-12.12s  %-6d  %-6d  %-6d  % -17s %s  %s} \
      $addr $opcode $p1 $p2 $p3 $p4 $p5 $comment
    ]
  }
}

# Show the VDBE program for an SQL statement but omit the Trace
# opcode at the beginning.  This procedure can be used to prove
# that different SQL statements generate exactly the same VDBE code.
#
proc explain_no_trace {sql} {
  catch {do_cdb2_defquery "EXPLAIN $sql"} tr
  return [lrange $tr 7 end]
}

# Another procedure to execute SQL.  This one includes the field
# names in the returned list.
#
proc execsql2 {sql} {
  set result {}
  db eval $sql data {
    foreach f $data(*) {
      lappend result $f $data($f)
    }
  }
  return $result
}

# Use the non-callback API to execute multiple SQL statements
#
proc stepsql {dbptr sql} {
  set sql [string trim $sql]
  set r 0
  while {[string length $sql]>0} {
    if {[catch {sqlite3_prepare $dbptr $sql -1 sqltail} vm]} {
      return [list 1 $vm]
    }
    set sql [string trim $sqltail]
#    while {[sqlite_step $vm N VAL COL]=="SQLITE_ROW"} {
#      foreach v $VAL {lappend r $v}
#    }
    while {[sqlite3_step $vm]=="SQLITE_ROW"} {
      for {set i 0} {$i<[sqlite3_data_count $vm]} {incr i} {
        lappend r [sqlite3_column_text $vm $i]
      }
    }
    if {[catch {sqlite3_finalize $vm} errmsg]} {
      return [list 1 $errmsg]
    }
  }
  return $r
}

# Delete a file or directory
#
proc forcedelete {filename} {
  if {[catch {file delete -force $filename}]} {
    exec rm -rf $filename
  }
}

# Do an integrity check of the entire database
#
proc integrity_check {name {db db}} {
  ifcapable integrityck {
    do_test $name [list execsql {PRAGMA integrity_check} $db] {ok}
  }
}

proc fix_ifcapable_expr {expr} {
  set ret ""
  set state 0
  for {set i 0} {$i < [string length $expr]} {incr i} {
    set char [string range $expr $i $i]
    set newstate [expr {[string is alnum $char] || $char eq "_"}]
    if {$newstate && !$state} {
      append ret {$::sqlite_options(}
    }
    if {!$newstate && $state} {
      append ret )
    }
    append ret $char
    set state $newstate
  }
  if {$state} {append ret )}
  return $ret
}

# Evaluate a boolean expression of capabilities.  If true, execute the
# code.  Omit the code if false.
#
proc ifcapable {expr code {else ""} {elsecode ""}} {
  #regsub -all {[a-z_0-9]+} $expr {$::sqlite_options(&)} e2
  #set e2 [fix_ifcapable_expr $expr]
  switch $expr {
    update_delete_limit -
    explain -
    or_opt -
    explain&&subquery -
    compound -
    subquery {
      set c [catch {uplevel 1 $code} r]
    }

    reindex -
    comdb2_skip -
    reverse_unordered_selects -
    indexed_by -
    integrityck -
    tclvar {
      set c [catch {uplevel 1 $elsecode} r]
    }
    default {
      #puts "unknown capability $expr"
      set c [catch {uplevel 1 $elsecode} r]
    }
  }

  return -code $c $r
}

# This proc execs a seperate process that crashes midway through executing
# the SQL script $sql on database test.db.
#
# The crash occurs during a sync() of file $crashfile. When the crash
# occurs a random subset of all unsynced writes made by the process are
# written into the files on disk. Argument $crashdelay indicates the
# number of file syncs to wait before crashing.
#
# The return value is a list of two elements. The first element is a
# boolean, indicating whether or not the process actually crashed or
# reported some other error. The second element in the returned list is the
# error message. This is "child process exited abnormally" if the crash
# occured.
#
#   crashsql -delay CRASHDELAY -file CRASHFILE ?-blocksize BLOCKSIZE? $sql
#
proc crashsql {args} {

  set blocksize ""
  set crashdelay 1
  set prngseed 0
  set tclbody {}
  set crashfile ""
  set dc ""
  set sql [lindex $args end]
  
  for {set ii 0} {$ii < [llength $args]-1} {incr ii 2} {
    set z [lindex $args $ii]
    set n [string length $z]
    set z2 [lindex $args [expr $ii+1]]

    if     {$n>1 && [string first $z -delay]==0}     {set crashdelay $z2} \
    elseif {$n>1 && [string first $z -seed]==0}      {set prngseed $z2} \
    elseif {$n>1 && [string first $z -file]==0}      {set crashfile $z2}  \
    elseif {$n>1 && [string first $z -tclbody]==0}   {set tclbody $z2}  \
    elseif {$n>1 && [string first $z -blocksize]==0} {set blocksize "-s $z2" } \
    elseif {$n>1 && [string first $z -characteristics]==0} {set dc "-c {$z2}" } \
    else   { error "Unrecognized option: $z" }
  }

  if {$crashfile eq ""} {
    error "Compulsory option -file missing"
  }

  # $crashfile gets compared to the native filename in 
  # cfSync(), which can be different then what TCL uses by
  # default, so here we force it to the "nativename" format.
  set cfile [string map {\\ \\\\} [file nativename [file join [pwd] $crashfile]]]

  set f [open crash.tcl w]
  puts $f "sqlite3_crash_enable 1"
  puts $f "sqlite3_crashparams $blocksize $dc $crashdelay $cfile"
  puts $f "sqlite3_test_control_pending_byte $::sqlite_pending_byte"
  puts $f "sqlite3 db test.db -vfs crash"

  # This block sets the cache size of the main database to 10
  # pages. This is done in case the build is configured to omit
  # "PRAGMA cache_size".
  puts $f {db eval {SELECT * FROM sqlite_master;}}
  puts $f {set bt [btree_from_db db]}
  puts $f {btree_set_cache_size $bt 10}
  if {$prngseed} {
    set seed [expr {$prngseed%10007+1}]
    # puts seed=$seed
    puts $f "db eval {SELECT randomblob($seed)}"
  }

  if {[string length $tclbody]>0} {
    puts $f $tclbody
  }
  if {[string length $sql]>0} {
    puts $f "db eval {"
    puts $f   "$sql"
    puts $f "}"
  }
  close $f
  set r [catch {
    exec [info nameofexec] crash.tcl >@stdout
  } msg]
  
  # Windows/ActiveState TCL returns a slightly different
  # error message.  We map that to the expected message
  # so that we don't have to change all of the test
  # cases.
  if {$::tcl_platform(platform)=="windows"} {
    if {$msg=="child killed: unknown signal"} {
      set msg "child process exited abnormally"
    }
  }
  
  lappend r $msg
}

# Usage: do_ioerr_test <test number> <options...>
#
# This proc is used to implement test cases that check that IO errors
# are correctly handled. The first argument, <test number>, is an integer 
# used to name the tests executed by this proc. Options are as follows:
#
#     -tclprep          TCL script to run to prepare test.
#     -sqlprep          SQL script to run to prepare test.
#     -tclbody          TCL script to run with IO error simulation.
#     -sqlbody          TCL script to run with IO error simulation.
#     -exclude          List of 'N' values not to test.
#     -erc              Use extended result codes
#     -persist          Make simulated I/O errors persistent
#     -start            Value of 'N' to begin with (default 1)
#
#     -cksum            Boolean. If true, test that the database does
#                       not change during the execution of the test case.
#
proc do_ioerr_test {testname args} {

  set ::ioerropts(-start) 1
  set ::ioerropts(-cksum) 0
  set ::ioerropts(-erc) 0
  set ::ioerropts(-count) 100000000
  set ::ioerropts(-persist) 1
  set ::ioerropts(-ckrefcount) 0
  set ::ioerropts(-restoreprng) 1
  array set ::ioerropts $args

  # TEMPORARY: For 3.5.9, disable testing of extended result codes. There are
  # a couple of obscure IO errors that do not return them.
  set ::ioerropts(-erc) 0

  set ::go 1
  #reset_prng_state
  save_prng_state
  for {set n $::ioerropts(-start)} {$::go} {incr n} {
    set ::TN $n
    incr ::ioerropts(-count) -1
    if {$::ioerropts(-count)<0} break
 
    # Skip this IO error if it was specified with the "-exclude" option.
    if {[info exists ::ioerropts(-exclude)]} {
      if {[lsearch $::ioerropts(-exclude) $n]!=-1} continue
    }
    if {$::ioerropts(-restoreprng)} {
      restore_prng_state
    }

    # Delete the files test.db and test2.db, then execute the TCL and 
    # SQL (in that order) to prepare for the test case.
    do_test $testname.$n.1 {
      set ::sqlite_io_error_pending 0
      catch {db close}
      catch {db2 close}
      catch {file delete -force test.db}
      catch {file delete -force test.db-journal}
      catch {file delete -force test2.db}
      catch {file delete -force test2.db-journal}
      set ::DB [sqlite3 db test.db; sqlite3_connection_pointer db]
      sqlite3_extended_result_codes $::DB $::ioerropts(-erc)
      if {[info exists ::ioerropts(-tclprep)]} {
        eval $::ioerropts(-tclprep)
      }
      if {[info exists ::ioerropts(-sqlprep)]} {
        execsql $::ioerropts(-sqlprep)
      }
      expr 0
    } {0}

    # Read the 'checksum' of the database.
    if {$::ioerropts(-cksum)} {
      set checksum [cksum]
    }

    # Set the Nth IO error to fail.
    do_test $testname.$n.2 [subst {
      set ::sqlite_io_error_persist $::ioerropts(-persist)
      set ::sqlite_io_error_pending $n
    }] $n
  
    # Create a single TCL script from the TCL and SQL specified
    # as the body of the test.
    set ::ioerrorbody {}
    if {[info exists ::ioerropts(-tclbody)]} {
      append ::ioerrorbody "$::ioerropts(-tclbody)\n"
    }
    if {[info exists ::ioerropts(-sqlbody)]} {
      append ::ioerrorbody "db eval {$::ioerropts(-sqlbody)}"
    }

    # Execute the TCL Script created in the above block. If
    # there are at least N IO operations performed by SQLite as
    # a result of the script, the Nth will fail.
    do_test $testname.$n.3 {
      set ::sqlite_io_error_hit 0
      set ::sqlite_io_error_hardhit 0
      set r [catch $::ioerrorbody msg]
      set ::errseen $r
      set rc [sqlite3_errcode $::DB]
      if {$::ioerropts(-erc)} {
        # If we are in extended result code mode, make sure all of the
        # IOERRs we get back really do have their extended code values.
        # If an extended result code is returned, the sqlite3_errcode
        # TCLcommand will return a string of the form:  SQLITE_IOERR+nnnn
        # where nnnn is a number
        if {[regexp {^SQLITE_IOERR} $rc] && ![regexp {IOERR\+\d} $rc]} {
          return $rc
        }
      } else {
        # If we are not in extended result code mode, make sure no
        # extended error codes are returned.
        if {[regexp {\+\d} $rc]} {
          return $rc
        }
      }
      # The test repeats as long as $::go is non-zero.  $::go starts out
      # as 1.  When a test runs to completion without hitting an I/O
      # error, that means there is no point in continuing with this test
      # case so set $::go to zero.
      #
      if {$::sqlite_io_error_pending>0} {
        set ::go 0
        set q 0
        set ::sqlite_io_error_pending 0
      } else {
        set q 1
      }

      set s [expr $::sqlite_io_error_hit==0]
      if {$::sqlite_io_error_hit>$::sqlite_io_error_hardhit && $r==0} {
        set r 1
      }
      set ::sqlite_io_error_hit 0

      # One of two things must have happened. either
      #   1.  We never hit the IO error and the SQL returned OK
      #   2.  An IO error was hit and the SQL failed
      #
      #puts "s=$s r=$r q=$q"
      expr { ($s && !$r && !$q) || (!$s && $r && $q) }
    } {1}

    set ::sqlite_io_error_hit 0
    set ::sqlite_io_error_pending 0

    # Check that no page references were leaked. There should be 
    # a single reference if there is still an active transaction, 
    # or zero otherwise.
    #
    # UPDATE: If the IO error occurs after a 'BEGIN' but before any
    # locks are established on database files (i.e. if the error 
    # occurs while attempting to detect a hot-journal file), then
    # there may 0 page references and an active transaction according
    # to [sqlite3_get_autocommit].
    #
    if {$::go && $::sqlite_io_error_hardhit && $::ioerropts(-ckrefcount)} {
      do_test $testname.$n.4 {
        set bt [btree_from_db db]
        db_enter db
        array set stats [btree_pager_stats $bt]
        db_leave db
        set nRef $stats(ref)
        expr {$nRef == 0 || ([sqlite3_get_autocommit db]==0 && $nRef == 1)}
      } {1}
    }

    # If there is an open database handle and no open transaction, 
    # and the pager is not running in exclusive-locking mode,
    # check that the pager is in "unlocked" state. Theoretically,
    # if a call to xUnlock() failed due to an IO error the underlying
    # file may still be locked.
    #
    ifcapable pragma {
      if { [info commands db] ne ""
        && $::ioerropts(-ckrefcount)
        && [db one {pragma locking_mode}] eq "normal"
        && [sqlite3_get_autocommit db]
      } {
        do_test $testname.$n.5 {
          set bt [btree_from_db db]
          db_enter db
          array set stats [btree_pager_stats $bt]
          db_leave db
          set stats(state)
        } 0
      }
    }

    # If an IO error occured, then the checksum of the database should
    # be the same as before the script that caused the IO error was run.
    #
    if {$::go && $::sqlite_io_error_hardhit && $::ioerropts(-cksum)} {
      do_test $testname.$n.6 {
        catch {db close}
        catch {db2 close}
        set ::DB [sqlite3 db test.db; sqlite3_connection_pointer db]
        cksum
      } $checksum
    }

    set ::sqlite_io_error_hardhit 0
    set ::sqlite_io_error_pending 0
    if {[info exists ::ioerropts(-cleanup)]} {
      catch $::ioerropts(-cleanup)
    }
  }
  set ::sqlite_io_error_pending 0
  set ::sqlite_io_error_persist 0
  unset ::ioerropts
}

# Return a checksum based on the contents of the main database associated
# with connection $db
#
proc cksum {{db db}} {
  set txt [$db eval {
      SELECT name, type, sql FROM sqlite_master order by name
  }]\n
  foreach tbl [$db eval {
      SELECT name FROM sqlite_master WHERE type='table' order by name
  }] {
    append txt [$db eval "SELECT * FROM $tbl"]\n
  }
  foreach prag {default_synchronous default_cache_size} {
    append txt $prag-[$db eval "PRAGMA $prag"]\n
  }
  set cksum [string length $txt]-[md5 $txt]
  # puts $cksum-[file size test.db]
  return $cksum
}

# Generate a checksum based on the contents of the main and temp tables
# database $db. If the checksum of two databases is the same, and the
# integrity-check passes for both, the two databases are identical.
#
proc allcksum {{db db}} {
  set ret [list]
  ifcapable tempdb {
    set sql {
      SELECT name FROM sqlite_master WHERE type = 'table' UNION
      SELECT name FROM sqlite_temp_master WHERE type = 'table' UNION
      SELECT 'sqlite_master' UNION
      SELECT 'sqlite_temp_master' ORDER BY 1
    }
  } else {
    set sql {
      SELECT name FROM sqlite_master WHERE type = 'table' UNION
      SELECT 'sqlite_master' ORDER BY 1
    }
  }
  set tbllist [$db eval $sql]
  set txt {}
  foreach tbl $tbllist {
    append txt [$db eval "SELECT * FROM $tbl"]
  }
  foreach prag {default_cache_size} {
    append txt $prag-[$db eval "PRAGMA $prag"]\n
  }
  # puts txt=$txt
  return [md5 $txt]
}

# Generate a checksum based on the contents of a single database with
# a database connection.  The name of the database is $dbname.  
# Examples of $dbname are "temp" or "main".
#
proc dbcksum {db dbname} {
  if {$dbname=="temp"} {
    set master sqlite_temp_master
  } else {
    set master $dbname.sqlite_master
  }
  set alltab [$db eval "SELECT name FROM $master WHERE type='table'"]
  set txt [$db eval "SELECT * FROM $master"]\n
  foreach tab $alltab {
    append txt [$db eval "SELECT * FROM $dbname.$tab"]\n
  }
  return [md5 $txt]
}

proc memdebug_log_sql {{filename mallocs.sql}} {

  set data [sqlite3_memdebug_log dump]
  set nFrame [expr [llength [lindex $data 0]]-2]
  if {$nFrame < 0} { return "" }

  set database temp

  set tbl "CREATE TABLE ${database}.malloc(zTest, nCall, nByte, lStack);"

  set sql ""
  foreach e $data {
    set nCall [lindex $e 0]
    set nByte [lindex $e 1]
    set lStack [lrange $e 2 end]
    append sql "INSERT INTO ${database}.malloc VALUES"
    append sql "('test', $nCall, $nByte, '$lStack');\n"
    foreach f $lStack {
      set frames($f) 1
    }
  }

  set tbl2 "CREATE TABLE ${database}.frame(frame INTEGER PRIMARY KEY, line);\n"
  set tbl3 "CREATE TABLE ${database}.file(name PRIMARY KEY, content);\n"

  foreach f [array names frames] {
    set addr [format %x $f]
    set cmd "addr2line -e [info nameofexec] $addr"
    set line [eval exec $cmd]
    append sql "INSERT INTO ${database}.frame VALUES($f, '$line');\n"

    set file [lindex [split $line :] 0]
    set files($file) 1
  }

  foreach f [array names files] {
    set contents ""
    catch {
      set fd [open $f]
      set contents [read $fd]
      close $fd
    }
    set contents [string map {' ''} $contents]
    append sql "INSERT INTO ${database}.file VALUES('$f', '$contents');\n"
  }

  set fd [open $filename w]
  puts $fd "BEGIN; ${tbl}${tbl2}${tbl3}${sql} ; COMMIT;"
  close $fd
}

# Copy file $from into $to. This is used because some versions of
# TCL for windows (notably the 8.4.1 binary package shipped with the
# current mingw release) have a broken "file copy" command.
#
proc copy_file {from to} {
  if {$::tcl_platform(platform)=="unix"} {
    file copy -force $from $to
  } else {
    set f [open $from]
    fconfigure $f -translation binary
    set t [open $to w]
    fconfigure $t -translation binary
    puts -nonewline $t [read $f [file size $from]]
    close $t
    close $f
  }
}

# Drop all tables in database [db]
proc drop_all_tables {{db db}} {
  ifcapable trigger&&foreignkey {
    set pk [$db one "PRAGMA foreign_keys"]
    $db eval "PRAGMA foreign_keys = OFF"
  }
  foreach {idx name file} [db eval {PRAGMA database_list}] {
    if {$idx==1} {
      set master sqlite_temp_master
    } else {
      set master $name.sqlite_master
    }
    foreach {t type} [$db eval "
      SELECT name, type FROM $master
      WHERE type IN('table', 'view') AND name NOT like 'sqlite_%'
    "] {
      $db eval "DROP $type $t"
    }
  }
  ifcapable trigger&&foreignkey {
    $db eval "PRAGMA foreign_keys = $pk"
  }
}

#-------------------------------------------------------------------------
# If a test script is executed with global variable $::G(perm:name) set to
# "wal", then the tests are run in WAL mode. Otherwise, they should be run 
# in rollback mode. The following Tcl procs are used to make this less 
# intrusive:
#
#   wal_set_journal_mode ?DB?
#
#     If running a WAL test, execute "PRAGMA journal_mode = wal" using
#     connection handle DB. Otherwise, this command is a no-op.
#
#   wal_check_journal_mode TESTNAME ?DB?
#
#     If running a WAL test, execute a tests case that fails if the main
#     database for connection handle DB is not currently a WAL database.
#     Otherwise (if not running a WAL permutation) this is a no-op.
#
#   wal_is_wal_mode
#   
#     Returns true if this test should be run in WAL mode. False otherwise.
# 
proc wal_is_wal_mode {} {
  expr {[permutation] eq "wal"}
}
proc wal_set_journal_mode {{db db}} {
  if { [wal_is_wal_mode] } {
    $db eval "PRAGMA journal_mode = WAL"
  }
}
proc wal_check_journal_mode {testname {db db}} {
  if { [wal_is_wal_mode] } {
    $db eval { SELECT * FROM sqlite_master }
    do_test $testname [list $db eval "PRAGMA main.journal_mode"] {wal}
  }
}

proc permutation {} {
  set perm ""
  catch {set perm $::G(perm:name)}
  set perm
}
proc presql {} {
  set presql ""
  catch {set presql $::G(perm:presql)}
  set presql
}

#-------------------------------------------------------------------------
#
proc slave_test_script {script} {

  # Create the interpreter used to run the test script.
  interp create tinterp

  # Populate some global variables that tester.tcl expects to see.
  foreach {var value} [list              \
    ::argv0 $::argv0                     \
    ::argv  {}                           \
    ::SLAVE 1                            \
  ] {
    interp eval tinterp [list set $var $value]
  }

  # The alias used to access the global test counters.
  tinterp alias set_test_counter set_test_counter

  # Set up the ::cmdlinearg array in the slave.
  interp eval tinterp [list array set ::cmdlinearg [array get ::cmdlinearg]]

  # Set up the ::G array in the slave.
  interp eval tinterp [list array set ::G [array get ::G]]

  # Load the various test interfaces implemented in C.
  load_testfixture_extensions tinterp

  # Run the test script.
  interp eval tinterp $script

  # Check if the interpreter call [run_thread_tests]
  if { [interp eval tinterp {info exists ::run_thread_tests_called}] } {
    set ::run_thread_tests_called 1
  }

  # Delete the interpreter used to run the test script.
  interp delete tinterp
}

proc slave_test_file {zFile} {
  set tail [file tail $zFile]

  # Remember the value of the shared-cache setting. So that it is possible
  # to check afterwards that it was not modified by the test script.
  #
  ifcapable shared_cache { set scs [sqlite3_enable_shared_cache] }

  # Run the test script in a slave interpreter.
  #
  unset -nocomplain ::run_thread_tests_called
  reset_prng_state
  set ::sqlite_open_file_count 0
  set time [time { slave_test_script [list source $zFile] }]
  set ms [expr [lindex $time 0] / 1000]

  # Test that all files opened by the test script were closed. Omit this
  # if the test script has "thread" in its name. The open file counter
  # is not thread-safe.
  #
  if {[info exists ::run_thread_tests_called]==0} {
    do_test ${tail}-closeallfiles { expr {$::sqlite_open_file_count>0} } {0}
  }
  set ::sqlite_open_file_count 0

  # Test that the global "shared-cache" setting was not altered by 
  # the test script.
  #
  ifcapable shared_cache { 
    set res [expr {[sqlite3_enable_shared_cache] == $scs}]
    do_test ${tail}-sharedcachesetting [list set {} $res] 1
  }

  # Add some info to the output.
  #
  puts "Time: $tail $ms ms"
  show_memstats
}

# Open a new connection on database test.db and execute the SQL script
# supplied as an argument. Before returning, close the new conection and
# restore the 4 byte fields starting at header offsets 28, 92 and 96
# to the values they held before the SQL was executed. This simulates
# a write by a pre-3.7.0 client.
#
proc sql36231 {sql} {
  set B [hexio_read test.db 92 8]
  set A [hexio_read test.db 28 4]
  sqlite3 db36231 test.db
  catch { db36231 func a_string a_string }
  execsql $sql db36231
  db36231 close
  hexio_write test.db 28 $A
  hexio_write test.db 92 $B
  return ""
}

# If the library is compiled with the SQLITE_DEFAULT_AUTOVACUUM macro set
# to non-zero, then set the global variable $AUTOVACUUM to 1.
#set AUTOVACUUM $sqlite_options(default_autovacuum)

source $testdir/thread_common.tcl

# vim: set sw=2 ts=2:
