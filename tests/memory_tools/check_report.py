#!/usr/bin/env python3
"""Classify memory-tool output and decide whether a probe met its expectation.

check:      one probe run -> report.json, exit 0 only if the expectation holds
summarize:  per-probe report.json files -> summary.json + summary.txt

Fails closed: a missing, empty or unrecognised detector log, an unexpected
category, or an exit status the detector does not use is a failure.
Standard library only.
"""

import argparse
import json
import os
import re
import sys

CATEGORIES = (
    "heap-use-after-free",
    "leak",
    "undefined-behavior",
    "data-race",
    "invalid-read",
    "invalid-write",
    "uninitialized-read",
    "unknown-error",
)

# (category, regex). Matched line by line; first match wins for a line.
SIGNATURES = (
    ("heap-use-after-free", re.compile(r"ERROR: AddressSanitizer: heap-use-after-free")),
    ("unknown-error", re.compile(r"ERROR: AddressSanitizer\b.*")),
    ("leak", re.compile(r"ERROR: LeakSanitizer: detected memory leaks")),
    ("unknown-error", re.compile(r"ERROR: LeakSanitizer\b.*")),
    ("undefined-behavior", re.compile(r"runtime error: .*")),
    ("data-race", re.compile(r"WARNING: ThreadSanitizer: data race")),
    ("unknown-error", re.compile(r"(WARNING|ERROR|FATAL): ThreadSanitizer\b.*")),
    ("invalid-read", re.compile(r"Invalid read of size \d+")),
    ("invalid-write", re.compile(r"Invalid write of size \d+")),
    ("uninitialized-read", re.compile(r"Use of uninitialised value\b.*")),
    ("uninitialized-read",
     re.compile(r"Conditional jump or move depends on uninitialised value")),
    ("uninitialized-read", re.compile(r"Syscall param .* uninitialised byte")),
    ("unknown-error", re.compile(r"Invalid free\(\)|Mismatched free\(\)|Process terminating")),
)

VALGRIND_LOST = re.compile(r"(definitely|indirectly|possibly) lost: ([\d,]+) bytes")
VALGRIND_ERRORS = re.compile(r"ERROR SUMMARY: ([\d,]+) errors?")
VALGRIND_HEADER = re.compile(r"Memcheck, a memory error detector")
# Lines that show a sanitizer runtime produced the file even without findings.
SANITIZER_HEADER = re.compile(r"(Sanitizer|SUMMARY: \w+Sanitizer)")


def _num(text):
    return int(text.replace(",", ""))


def scan_text(text, kind):
    """Return (categories, signatures, recognized, valgrind_errors)."""
    categories = set()
    signatures = []
    recognized = False
    valgrind_errors = None

    for line in text.splitlines():
        for category, regex in SIGNATURES:
            m = regex.search(line)
            if m:
                categories.add(category)
                sig = m.group(0).strip()
                if sig not in signatures:
                    signatures.append(sig)
                recognized = True
                break

        if kind == "valgrind":
            if VALGRIND_HEADER.search(line):
                recognized = True
            m = VALGRIND_LOST.search(line)
            if m and _num(m.group(2)) > 0:
                categories.add("leak")
                sig = m.group(1) + " lost:"
                if sig not in signatures:
                    signatures.append(sig)
            m = VALGRIND_ERRORS.search(line)
            if m:
                recognized = True
                count = _num(m.group(1))
                valgrind_errors = count if valgrind_errors is None else valgrind_errors + count
        elif SANITIZER_HEADER.search(line):
            recognized = True

    if kind == "valgrind" and valgrind_errors and not categories:
        # Valgrind counted errors we have no signature for.
        categories.add("unknown-error")
        signatures.append("ERROR SUMMARY: %d errors" % valgrind_errors)

    return categories, signatures, recognized, valgrind_errors


def evaluate(args):
    reasons = []
    categories = set()
    signatures = []
    reports = []
    valgrind_summary_seen = False
    valgrind_errors = 0

    for path in args.report:
        reports.append(path)
        try:
            with open(path, errors="replace") as f:
                text = f.read()
        except OSError as e:
            reasons.append("cannot read detector log %s: %s" % (path, e))
            continue
        if not text.strip():
            reasons.append("empty detector log %s" % path)
            continue
        cats, sigs, recognized, verrs = scan_text(text, args.kind)
        if not recognized:
            reasons.append("unrecognised detector log format in %s" % path)
            cats.add("unknown-error")
        if verrs is not None:
            valgrind_summary_seen = True
            valgrind_errors += verrs
        categories |= cats
        signatures += [s for s in sigs if s not in signatures]

    # Program streams: GCC's combined ASan+UBSan runtime prints UBSan findings
    # to stderr regardless of log_path. Streams need not be recognised.
    for path in args.stream:
        try:
            with open(path, errors="replace") as f:
                text = f.read()
        except OSError as e:
            reasons.append("cannot read %s: %s" % (path, e))
            continue
        cats, sigs, _, _ = scan_text(text, "sanitizer")
        categories |= cats
        signatures += [s for s in sigs if s not in signatures]

    expected = args.expect
    if expected == "clean":
        if args.exit_code != 0:
            reasons.append("clean probe exited with %d" % args.exit_code)
        if categories:
            reasons.append("clean probe produced findings: %s" % ", ".join(sorted(categories)))
        if args.kind == "valgrind":
            if not valgrind_summary_seen:
                reasons.append("no Valgrind ERROR SUMMARY found; Valgrind did not run or log is missing")
            elif valgrind_errors != 0:
                reasons.append("Valgrind reported %d errors" % valgrind_errors)
    else:
        if args.exit_code == 0:
            reasons.append("faulty probe exited 0")
        elif args.failure_exit_code and args.exit_code not in args.failure_exit_code:
            reasons.append("exit code %d is not a detector exit code %s"
                           % (args.exit_code, args.failure_exit_code))
        if expected not in categories:
            reasons.append("expected finding '%s' not found" % expected)
        unexpected = sorted(categories - {expected})
        if unexpected:
            reasons.append("unexpected findings: %s" % ", ".join(unexpected))

    return {
        "variant": args.variant,
        "probe": args.probe,
        "expected": expected,
        "matched": not reasons,
        "process_exit_code": args.exit_code,
        "categories": sorted(categories),
        "reports": reports,
        "signatures": signatures,
        "failure_reasons": reasons,
    }


def cmd_check(args):
    result = evaluate(args)
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)
        f.write("\n")
    status = "PASS" if result["matched"] else "FAIL"
    print("%s %s/%s: expected %s, found [%s]%s" % (
        status, args.variant, args.probe, result["expected"],
        ", ".join(result["categories"]),
        "" if result["matched"] else " -- " + "; ".join(result["failure_reasons"])))
    return 0 if result["matched"] else 1


def cmd_summarize(args):
    probes = []
    reasons = []
    for probe in args.expected_probe:
        if args.stage_failure:
            probes.append({"probe": probe, "matched": False, "status": "not-run",
                           "failure_reasons": ["not run: " + args.stage_failure]})
            continue
        path = os.path.join(args.artifact_dir, "probes", probe, "report.json")
        try:
            with open(path) as f:
                probes.append(json.load(f))
        except (OSError, ValueError) as e:
            reasons.append("missing or unreadable result for probe %s: %s" % (probe, e))
            probes.append({"probe": probe, "matched": False,
                           "failure_reasons": ["no report.json"]})
    if args.stage_failure:
        reasons.append(args.stage_failure)
    failed = [p["probe"] for p in probes if not p.get("matched") and p.get("status") != "not-run"]
    if failed:
        reasons.append("probes failed: %s" % ", ".join(failed))
    if not args.expected_probe:
        reasons.append("no probes were expected")

    summary = {
        "variant": args.variant,
        "passed": not reasons,
        "source_sha": args.source_sha,
        "leak_checking": args.leak_checking,
        "failure_reasons": reasons,
        "probes": probes,
    }
    with open(os.path.join(args.artifact_dir, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
        f.write("\n")

    lines = ["variant: %s" % args.variant,
             "result: %s" % ("PASS" if summary["passed"] else "FAIL"),
             "source: %s" % args.source_sha,
             "leak checking: %s" % args.leak_checking]
    for p in probes:
        if p.get("status") == "not-run":
            lines.append("  --   %-22s not run" % p["probe"])
            continue
        lines.append("  %-4s %-22s expected=%-20s found=[%s] exit=%s" % (
            "ok" if p.get("matched") else "FAIL", p.get("probe"), p.get("expected", "?"),
            ", ".join(p.get("categories", [])), p.get("process_exit_code", "?")))
    for r in reasons:
        lines.append("reason: %s" % r)
    text = "\n".join(lines) + "\n"
    with open(os.path.join(args.artifact_dir, "summary.txt"), "w") as f:
        f.write(text)
    sys.stdout.write(text)
    return 0 if summary["passed"] else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command")
    sub.required = True  # not a constructor argument before Python 3.7

    c = sub.add_parser("check", help="evaluate one probe run")
    c.add_argument("--variant", required=True)
    c.add_argument("--probe", required=True)
    c.add_argument("--expect", required=True, choices=CATEGORIES + ("clean",))
    c.add_argument("--kind", required=True, choices=("sanitizer", "valgrind"))
    c.add_argument("--exit-code", required=True, type=int)
    c.add_argument("--failure-exit-code", type=int, action="append", default=[],
                   help="exit code the detector uses on a finding (repeatable)")
    c.add_argument("--report", action="append", default=[], help="detector log file (repeatable)")
    c.add_argument("--stream", action="append", default=[], help="program stdout/stderr capture")
    c.add_argument("--output", required=True)
    c.set_defaults(func=cmd_check)

    s = sub.add_parser("summarize", help="aggregate per-probe results of one variant")
    s.add_argument("--variant", required=True)
    s.add_argument("--artifact-dir", required=True)
    s.add_argument("--expected-probe", action="append", default=[])
    s.add_argument("--leak-checking", default="")
    s.add_argument("--source-sha", default="")
    s.add_argument("--stage-failure", default="", help="set when configure/build/linkage failed")
    s.set_defaults(func=cmd_summarize)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
