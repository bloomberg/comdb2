#!/usr/bin/env bash


# uses: tests/test-utils/systables.txt
# Usage: inside .expected files: @inject_systables ((pattern))
# Pattern placeholders: /table/ or /pattern/ (replaced with actual systable names)

inject_systables_in_expected_files() {
    local specific_file="$1"
    local systables_file="${TESTSROOTDIR}/test-utils/systables.txt"
    
    if [[ ! -f "${systables_file}" ]]; then
        return
    fi

    # If a specific file is provided, process only that file; otherwise process all .expected files
    local files_to_process
    if [[ -n "$specific_file" ]]; then
        files_to_process="$specific_file"
    else
        files_to_process=$(find . -maxdepth 1 -name "*.expected" -type f)
    fi

    for expected_file in $files_to_process; do
        if grep -q "@inject_systables" "${expected_file}" 2>/dev/null; then
            cp "${expected_file}" "${expected_file}.backup"
            
            local temp_file="${expected_file}.tmp"

            while IFS= read -r line; do
                if [[ "$line" =~ @inject_systables[[:space:]]*\( ]]; then
                    local temp="${line#*@inject_systables*\(}"
                    local pattern="${temp%\)*}"
                    

                    while IFS= read -r table; do
                        [[ -z "$table" || "$table" =~ ^# ]] && continue
                        local formatted="${pattern//\/table\//$table}"
                        formatted="${formatted//\/pattern\//$table}"
                        echo "$formatted"
                    done < "${systables_file}"
                else
                    echo "$line"
                fi
            done < "${expected_file}" > "${temp_file}"
            
            mv "${temp_file}" "${expected_file}"
        fi
    done
}

restore_expected_files() {
    for backup_file in $(find . -maxdepth 1 -name "*.expected.backup" -type f); do
        original_file="${backup_file%.backup}"
        mv "${backup_file}" "${original_file}"
    done
}

