# If the line starts with a timestamp in the format "MM/DD HH:MM:SS",
match($0, /^([0-9]{2}\/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})/, a) {
    # Parse it into Unix time (e.g. `date -d "YYYY/MM/DD HH:MM:SS" +%s`).
    cmd = "date -d \"" strftime("%Y") "/" a[1] "\" +%s"
    cmd | getline timestamp
    close(cmd)

    # If the line contains "SCHEMA CHANGE STATS", emit the timestamp.
    if ($0 ~ /SCHEMA CHANGE STATS/) {
        print timestamp
    }
}
