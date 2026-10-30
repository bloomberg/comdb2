#!/usr/bin/env python3
"""Classify memory-tool output and decide whether it is acceptable.

check:      one probe run -> report.json, exit 0 only if the expectation holds
summarize:  per-probe report.json files -> summary.json + summary.txt
scan:       every server process of one database test (tests/memory_tools/
            lifecycle.sh) -> summary.json + summary.txt

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
    "lock-order-inversion",
    "thread-leak",
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
    ("unknown-error", re.compile(r"LeakSanitizer has encountered a fatal error")),
    ("unknown-error", re.compile(r"AddressSanitizer:DEADLYSIGNAL")),
    # UBSan's own format; server logs are scanned too, so bare text is not enough.
    ("undefined-behavior", re.compile(r"\S+:\d+(:\d+)?: runtime error: .*")),
    ("data-race", re.compile(r"WARNING: ThreadSanitizer: data race")),
    ("lock-order-inversion", re.compile(r"WARNING: ThreadSanitizer: lock-order-inversion")),
    ("thread-leak", re.compile(r"WARNING: ThreadSanitizer: thread leak")),
    ("unknown-error", re.compile(r"(WARNING|ERROR|FATAL): ThreadSanitizer\b.*")),
    ("invalid-read", re.compile(r"Invalid read of size \d+|Syscall param .* points to unaddressable byte")),
    ("invalid-write", re.compile(r"Invalid write of size \d+")),
    ("uninitialized-read", re.compile(r"Use of uninitialised value\b.*")),
    ("uninitialized-read",
     re.compile(r"Conditional jump or move depends on uninitialised value")),
    ("uninitialized-read", re.compile(r"Syscall param .* uninitialised byte")),
    ("unknown-error", re.compile(r"Invalid free\(\)|Mismatched free\(\)|Process terminating")),
)

VALGRIND_LOST = re.compile(r"(definitely|indirectly|possibly) lost: ([\d,]+) bytes")
VALGRIND_ERRORS = re.compile(r"ERROR SUMMARY: ([\d,]+) errors?")
VALGRIND_CONTEXTS = re.compile(r"ERROR SUMMARY: [\d,]+ errors? from ([\d,]+) contexts?")
# One line per error context Valgrind prints: an error kind we know, or a
# leak loss record of a kind that counts as an error.
VALGRIND_KNOWN_CONTEXT = re.compile(
    r"==\d+== (Invalid (read|write) of size|Invalid free\(\)|Mismatched free\(\)"
    r"|Conditional jump or move depends|Use of uninitialised value|Syscall param .* (uninitialised|unaddressable)"
    r"|.*are (definitely|indirectly|possibly) lost in loss record)")
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
    contexts = known_contexts = 0

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
            if VALGRIND_KNOWN_CONTEXT.search(line):
                known_contexts += 1
            m = VALGRIND_CONTEXTS.search(line)
            if m:
                contexts += _num(m.group(1))
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
    elif kind == "valgrind" and contexts > known_contexts:
        # more error contexts than the ones we recognised: report the rest too
        categories.add("unknown-error")
        signatures.append("%d Valgrind error contexts without a known signature"
                          % (contexts - known_contexts))

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


def _read(path):
    try:
        with open(path, errors="replace") as f:
            return f.read()
    except OSError:
        return None


LEAKCHECK_LINE = re.compile(r"^(?:leaks (\d+)|unavailable)$")


def scan_process(inv_dir, kind, leak_checking, run_id=None):
    """Evaluate one recorded server process (an exec.sh invocation dir)."""
    proc = {"dir": inv_dir, "findings": [], "signatures": [], "notes": []}
    gaps = []
    launch = _read(os.path.join(inv_dir, "launch.json"))
    try:
        meta = json.loads(launch) if launch is not None else None
    except ValueError:
        meta = None
    if meta is None:
        gaps.append("unreadable launch.json")
        meta = {}
    proc.update({k: meta.get(k) for k in ("pid", "node", "argv")})
    if run_id is not None and meta and meta.get("run_id") != run_id:
        gaps.append("record from another run (run_id %s)" % meta.get("run_id"))

    categories = set()
    det_dir = os.path.join(inv_dir, "detector")
    logs = sorted(os.listdir(det_dir)) if os.path.isdir(det_dir) else []
    valgrind_summary = False
    for name in logs:
        text = _read(os.path.join(det_dir, name))
        if not text or not text.strip():
            gaps.append("empty or unreadable detector log %s" % name)
            categories.add("unknown-error")
            continue
        cats, sigs, recognized, verrs = scan_text(text, kind)
        if not recognized:
            gaps.append("unrecognised detector log %s" % name)
            cats.add("unknown-error")
        if verrs is not None:
            valgrind_summary = True
        categories |= cats
        proc["signatures"] += [x for x in sigs if x not in proc["signatures"]]
    stderr = _read(os.path.join(inv_dir, "stderr.log")) or ""
    if not os.path.exists(os.path.join(inv_dir, "stderr-complete")):
        gaps.append("stderr copy incomplete")
    cats, sigs, _, _ = scan_text(stderr, "sanitizer")
    categories |= cats
    proc["signatures"] += [x for x in sigs if x not in proc["signatures"]]

    # pre-kill checkpoints written by the server: "leaks <n>" or "unavailable"
    checkpoints = []
    for line in (_read(os.path.join(inv_dir, "leakcheck")) or "").splitlines():
        m = LEAKCHECK_LINE.match(line.strip())
        if not m:
            gaps.append("malformed leakcheck record %r" % line)
            continue
        checkpoints.append(line.strip())
        if m.group(1) and int(m.group(1)) > 0:
            # a leak even if its report did not reach the detector log
            categories.add("leak")
            sig = "pre-kill LeakSanitizer check: %s" % line.strip()
            if sig not in proc["signatures"]:
                proc["signatures"].append(sig)
    checked = any(c.startswith("leaks ") for c in checkpoints)
    proc["findings"] = sorted(categories)

    # clean-exit: end of Comdb2's clean exit; exited: any exit()/return from
    # main (also runs the exit-time leak check), e.g. a server meant to fail
    proc["clean_exit"] = os.path.exists(os.path.join(inv_dir, "clean-exit"))
    proc["normal_exit"] = proc["clean_exit"] or os.path.exists(os.path.join(inv_dir, "exited"))
    clean_exit = proc["normal_exit"]
    proc["intentional_kill_checkpoint"] = bool(checkpoints) and not clean_exit

    if kind == "valgrind" and not any(n.startswith("valgrind.") for n in logs):
        gaps.append("no Valgrind log")
    if clean_exit:
        if kind == "valgrind" and not valgrind_summary:
            gaps.append("exited but no Valgrind ERROR SUMMARY")
    elif checkpoints:
        # Intentionally killed after a pre-kill checkpoint. Only asan-system
        # owes leak coverage, which the recoverable LSan check provides.
        if kind == "sanitizer" and leak_checking == "authoritative" and not checked:
            gaps.append("pre-kill checkpoint ran without LeakSanitizer")
        if kind == "valgrind":
            # Valgrind is authoritative for leaks, but a killed process never
            # reaches its leak summary; errors before the kill still count.
            gaps.append("killed before Valgrind's leak summary: leak coverage incomplete")
    else:
        gaps.append("no normal exit and no pre-kill checkpoint")
    proc["coverage_complete"] = not gaps
    proc["coverage_gaps"] = gaps
    return proc


def _node_matches(expected, recorded):
    # exec.sh names node directories after `hostname` with unsafe characters replaced
    expected = re.sub(r"[^A-Za-z0-9._-]", "_", expected)
    return (expected == recorded or recorded.startswith(expected + ".")
            or expected.startswith(recorded + "."))


def cmd_scan(args):
    root = args.root
    processes = []
    nodes_dir = os.path.join(root, "nodes")
    recorded_nodes = sorted(n for n in os.listdir(nodes_dir)
                            if not n.startswith(".")) if os.path.isdir(nodes_dir) else []
    for node in recorded_nodes:
        inv_root = os.path.join(nodes_dir, node, "invocations")
        if not os.path.isdir(inv_root):
            continue
        for pid in sorted(os.listdir(inv_root), key=lambda p: (len(p), p)):
            processes.append(scan_process(os.path.join(inv_root, pid), args.kind,
                                          args.leak_checking, args.run_id))

    incomplete = [line.strip() for line in
                  (_read(os.path.join(root, "incomplete.txt")) or "").splitlines()
                  if line.strip()]
    # written by servers that a test started without exec.sh (db/memdebug.c)
    uncovered = os.path.join(root, "uncovered")
    for name in sorted(os.listdir(uncovered)) if os.path.isdir(uncovered) else []:
        incomplete.append("server %s started without the memory launcher: %s"
                          % (name, (_read(os.path.join(uncovered, name)) or "").strip()))
    expect_server = args.expect_server == "yes"
    if expect_server and not processes:
        incomplete.append("no memory-tool server process recorded")
    if expect_server:
        seen = {p.get("node") or "" for p in processes}
        for node in args.expected_node:
            if not any(_node_matches(node, s) for s in seen):
                incomplete.append("no memory-tool server process recorded for node %s" % node)
    for p in processes:
        incomplete += ["%s pid %s: %s" % (p.get("node"), p.get("pid"), g)
                       for g in p["coverage_gaps"]]

    findings = sorted({f for p in processes for f in p["findings"]})
    if findings:
        memory_status = "fail"
    elif incomplete:
        memory_status = "incomplete"
    elif not processes:
        memory_status = "not-run"
    else:
        memory_status = "pass"
    overall = ("pass" if args.functional_status == "pass"
               and memory_status in ("pass", "not-run") else "fail")

    summary = {
        "variant": args.variant,
        "testcase": args.testcase,
        "functional_status": args.functional_status,
        "memory_status": memory_status,
        "overall_status": overall,
        "coverage_complete": not incomplete,
        "leak_checking": args.leak_checking,
        "findings": findings,
        "incomplete_reasons": incomplete,
        "processes": processes,
    }
    with open(os.path.join(root, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
        f.write("\n")

    lines = ["variant: %s" % args.variant, "testcase: %s" % args.testcase,
             "functional: %s" % args.functional_status, "memory: %s" % memory_status,
             "overall: %s" % overall, "leak checking: %s" % args.leak_checking]
    for p in processes:
        lines.append("  %-4s %s pid %s clean_exit=%s checkpoint=%s findings=[%s]" % (
            "ok" if p["coverage_complete"] and not p["findings"] else "FAIL",
            p.get("node"), p.get("pid"), p["clean_exit"], p["intentional_kill_checkpoint"],
            ", ".join(p["findings"])))
    lines += ["incomplete: %s" % r for r in incomplete]
    with open(os.path.join(root, "summary.txt"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print("memory-tools: %s %s: memory %s, overall %s (%d processes%s%s)" % (
        args.testcase, args.variant, memory_status, overall, len(processes),
        "; findings: " + ", ".join(findings) if findings else "",
        "; %d coverage gaps" % len(incomplete) if incomplete else ""))
    return 0 if memory_status in ("pass", "not-run") else 1


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

    sc = sub.add_parser("scan", help="scan every server process of one database test")
    sc.add_argument("--root", required=True, help="the test's memory-tool artifact root")
    sc.add_argument("--variant", required=True)
    sc.add_argument("--testcase", required=True)
    sc.add_argument("--kind", required=True, choices=("sanitizer", "valgrind"))
    sc.add_argument("--leak-checking", required=True)
    sc.add_argument("--functional-status", required=True, choices=("pass", "fail", "not-run"))
    sc.add_argument("--expected-node", action="append", default=[])
    sc.add_argument("--run-id", default=None, help="records of other runs are rejected")
    sc.add_argument("--expect-server", choices=("yes", "no"), default="yes",
                    help="no only for tests that start no server (COMDB2_UNITTEST)")
    sc.set_defaults(func=cmd_scan)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
