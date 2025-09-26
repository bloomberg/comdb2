# If the line starts with a timestamp in the format "MM/DD HH:MM:SS",
match($0, /^([0-9]{2}\/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})/, a) {
    # Parse it into Unix time (e.g. `date -d "YYYY/MM/DD HH:MM:SS" +%s`).
    cmd = "date -d \"" strftime("%Y") "/" a[1] "\" +%s"
    cmd | getline timestamp
    close(cmd)

    # Emit just the timestamp.
    print timestamp
}
