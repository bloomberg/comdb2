#!/usr/bin/env bash

declare -A quarantined_test_names_to_types
declare -A quarantine_types_to_descriptions

quarantine_types_to_descriptions["FLAKEY"]="it is flakey"
quarantine_types_to_descriptions["DB_BUG"]="it exposes a database bug"
quarantine_types_to_descriptions["UNKNOWN"]="it has failures we don't understand"

test_is_quarantined() {
    local -r test_name="$1"
    if [[ -n "${quarantined_test_names_to_types[$test_name]}" ]]; then
        return 0
    else
        return 1
    fi
}

is_valid_quarantine_type() {
    local -r type="$1"
    if [[ -n "${quarantine_types_to_descriptions[$type]}" ]]; then
        return 0
    else
        return 1
    fi
}

get_quarantined_test_description() {
    local -r test_name="$1"
    local -r type="${quarantined_test_names_to_types[$test_name]}"
    if [[ -n "$type" ]]; then
        echo "Test is quarantined because ${quarantine_types_to_descriptions[$type]}"
    else
        echo "No description available for $test_name"
        return 1
    fi
}

read_quarantine_csv() {
    local -r csv_file="${TESTSROOTDIR}/quarantine.csv"

    if [ ! -f "$csv_file" ]; then
        echo "File $csv_file not found."
        exit 1
    fi

    while IFS=, read -r name type ticket; do
        # Skip comment lines (starting with #)
        if [[ "$name" == \#* ]]; then
            continue
        fi
        if ! is_valid_quarantine_type $type; then
            echo "Invalid quarantine type '$type' for test '$name'."
            exit 1
        fi
        quarantined_test_names_to_types["$name"]="$type" 
    done < "$csv_file"
}

read_quarantine_csv

