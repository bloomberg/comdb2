###############################################################################
#
#   Copyright 2015 Bloomberg Finance L.P.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
###############################################################################

namespace eval ::tclcdb2 {
  #
  # NOTE: This procedure is designed to query the value of a specific column
  #       and set zero, one, or two variables in the context of the caller.
  #       The connection and index arguments are used to specify the column
  #       to be queried.  The nullVarName argument is the variable name, in
  #       the context of the caller, where the SQL NULL boolean flag should
  #       be stored.  The valueVarName argument is the variable name, in the
  #       context of the caller, where the value should be stored.  It will
  #       not be stored when it is a SQL NULL.  If either of the variable
  #       name arguments are an empty string, the associated value will not
  #       be stored in the context of the caller.  This procedure may raise
  #       script errors.
  #
  proc getNullableValue { connection index nullVarName valueVarName } {
    if {[string length $nullVarName] > 0} then {
      upvar 1 $nullVarName null
    }

    if {[string length $valueVarName] > 0} then {
      upvar 1 $valueVarName value
    }

    if {[catch {
      cdb2 colvalue $connection $index
    } localValue] == 0} then {
      set null false; set value $localValue
    } elseif {[isNullValueError $localValue]} then {
      set null true
    } else {
      error $localValue; # FAIL: Unexpected error.
    }
  }

  #
  # NOTE: This procedure is designed to query the value of a specific column
  #       and return it.  The connection and index arguments are used to
  #       specify the column to be queried.  The nullValue argument is the
  #       value to return in the event that the column value is SQL NULL.
  #       This procedure may raise script errors.
  #
  proc getValue { connection index {nullValue ""} } {
    if {[catch {
      cdb2 colvalue $connection $index
    } value] == 0} then {
      return $value
    } elseif {[isNullValueError $value]} then {
      return $nullValue
    } else {
      error $value; # FAIL: Unexpected error.
    }
  }

  #
  # NOTE: This procedure returns non-zero if the specified script error
  #       message indicates a column value of SQL NULL was encountered.
  #
  proc isNullValueError { error } {
    return [expr {$error eq "invalid column value\n"}]
  }

  #
  # NOTE: Invoke the Tcl shared library loader on our associated library
  #       using the file name appropriate for the platform.  This could
  #       have been done directly in the package index file; however, by
  #       doing it here we get the associated script library procedures
  #       (i.e. in this file) loaded automatically.
  #
  if {$::tcl_platform(platform) eq "windows"} then {
    load [file join [file dirname \
        [info script]] tclcdb2[info sharedlibextension]] tclcdb2
  } else {
    load [file join [file dirname \
        [info script]] libtclcdb2[info sharedlibextension]] tclcdb2
  }
}
