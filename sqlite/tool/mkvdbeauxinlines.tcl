#!/usr/bin/tclsh
#
# Use this script to modify C-language source code for a program that needs
# certain functions to be extracted and inlined.
#
# Usage example:
#
#     tclsh mkvdbeauxinlines.tcl ./src/vdbeaux.c .
#
set script [info script]

proc usage { {message ""} } {
  puts stdout [appendArgs $::script " inputFileName outputDirectory"]

  if {[string length $message] > 0} then {
    puts stdout [appendArgs \n $message \n]
  }

  exit 1
}

proc appendArgs { args } {
  set result ""; eval append result $args
}

proc makeBinaryChannel { channel } {
  fconfigure $channel -encoding binary -translation binary; # BINARY DATA
}

proc readFile { fileName } {
  set channel [open $fileName RDONLY]
  makeBinaryChannel $channel
  set result [read $channel]
  close $channel
  return $result
}

proc writeFile { fileName data } {
  set channel [open $fileName {WRONLY CREAT TRUNC}]
  makeBinaryChannel $channel
  puts -nonewline $channel $data
  close $channel
  return ""
}

proc appendFile { fileName data } {
  set channel [open $fileName {WRONLY CREAT APPEND}]
  makeBinaryChannel $channel
  puts -nonewline $channel $data
  close $channel
  return ""
}

proc readNormalizedFile { fileName } {
  global useCrLf

  set result [readFile $fileName]
  set useCrLf [expr {[string first \r $result] != -1}]

  if {$useCrLf} then {
    set result [string map [list \r\n \n] $result]
  }

  return $result
}

proc writeNormalizedFile { fileName data } {
  global useCrLf

  if {[info exists useCrLf] && $useCrLf} then {
    set data [string map [list \n \r\n] $data]
  }

  return [writeFile $fileName $data]
}

proc appendNormalizedFile { fileName data } {
  global useCrLf

  if {[info exists useCrLf] && $useCrLf} then {
    set data [string map [list \n \r\n] $data]
  }

  return [appendFile $fileName $data]
}

if {[llength $argv] != 2} then {usage}
set inputFileName [lindex $argv 0]

if {![file exists $inputFileName] || \
    ![file isfile $inputFileName] || \
    ![file readable $inputFileName]} then {
  usage "input file is not readable"
}

set outputDirectory [lindex $argv 1]

if {![file exists $outputDirectory] || \
    ![file isdirectory $outputDirectory] || \
    ![file writable $outputDirectory]} then {
  usage "output directory is not writable"
}

set pattern(start) [string trim {
#if defined\(SQLITE_BUILDING_FOR_COMDB2\)
#define START_INLINE_(\w+?)
#endif /\* defined\(SQLITE_BUILDING_FOR_COMDB2\) \*/
}]

set pattern(end) [string trim {
#if defined\(SQLITE_BUILDING_FOR_COMDB2\)
#define END_INLINE_(\w+?)
#endif /\* defined\(SQLITE_BUILDING_FOR_COMDB2\) \*/
}]

set pattern(function) {^(?:int|u32|i64)\s+(?:sqlite3|compare|vdbe)}

set outputFileNames [list \
    [file join $outputDirectory vdbeaux.c] \
    [file join $outputDirectory serialget.c] \
    [file join $outputDirectory memcompare.c] \
    [file join $outputDirectory vdbecompare.c]]

foreach outputFileName $outputFileNames {
  catch {file delete $outputFileName}
}

set data [readNormalizedFile $inputFileName]; set start 0

set indexes(start) [regexp \
    -all -inline -indices -start $start $pattern(start) $data]

set indexes(end) [regexp \
    -all -inline -indices -start $start $pattern(end) $data]

set length [llength $indexes(start)]

if {$length % 2 != 0} then {
  error [appendArgs \
      "index list length mismatch, " $length \
      " is not an even number"]
}

if {$length != [llength $indexes(end)]} then {
  error [appendArgs \
      "index list length mismatch, " $length \
      " versus " [llength $indexes(end)]]
}

set outputData $data

for {set index 0} {$index < $length} {incr index} {
  set id [expr {$index + 1}]

  set startIndex [lindex [lindex $indexes(start) $index] end]
  incr startIndex

  set endIndex [lindex [lindex $indexes(end) $index] 0]
  incr endIndex -1

  set chunk($id,data) [string range $data $startIndex $endIndex]

  regsub -all -line -- \
      $pattern(function) $chunk($id,data) {static inline \0} \
      chunk($id,data)

  incr index

  set startIndex [lindex [lindex $indexes(start) $index] 0]
  set endIndex [lindex [lindex $indexes(start) $index] end]

  set outputName(start) \
      [string tolower [string range $data $startIndex $endIndex]]

  set startIndex [lindex [lindex $indexes(end) $index] 0]
  set endIndex [lindex [lindex $indexes(end) $index] end]

  set outputName(end) \
      [string tolower [string range $data $startIndex $endIndex]]

  if {$outputName(end) ne $outputName(start)} then {
    error [appendArgs \
        "ending output name \"" $outputName(end) \
        "\" does not match starting output name \"" \
        $outputName(start) \"]
  }

  set chunk($id,name) $outputName(start)

  set chunk($id,fileName) [file join \
      $outputDirectory [appendArgs $chunk($id,name) .c]]

  if {![file exists $chunk($id,fileName)]} then {
    if {[file tail $chunk($id,fileName)] eq "memcompare.c"} then {
      set chunk($id,data) [appendArgs \
          "#include <serialget.c>\n\n" \
          $chunk($id,data)]
    } elseif {[file tail $chunk($id,fileName)] eq "vdbecompare.c"} then {
      set chunk($id,data) [appendArgs \
          "#include <memcompare.c>\n\n" \
          $chunk($id,data)]
    } elseif {[file tail $chunk($id,fileName)] eq "serialget.c"} then {
      set chunk($id,data) [appendArgs \
          "#ifndef SERIALGET_C\n" \
          "#define SERIALGET_C\n\n" \
          $chunk($id,data) \
          "\n\n#endif /* SERIALGET_C */\n"]
    }
  }

  appendNormalizedFile $chunk($id,fileName) $chunk($id,data)

  #
  # TODO: Replace chunks of data with something else?
  #
  set outputData [string map \
      [list $chunk($id,data) "\n chunk #$id\n"] $outputData]
}

writeNormalizedFile [lindex $outputFileNames 0] $outputData
