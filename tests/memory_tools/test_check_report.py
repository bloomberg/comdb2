#!/usr/bin/env python3
"""Unit tests for check_report.py. Run: python3 -m unittest tests/memory_tools/test_check_report.py"""

import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_report  # noqa: E402

ASAN_UAF = """=================================================================
==4242==ERROR: AddressSanitizer: heap-use-after-free on address 0x506000000e4 at pc 0x4011bb bp 0x7ffc sp 0x7ffc
READ of size 4 at 0x506000000e4 thread T0
    #0 0x4011ba in read_slot probes/heap_use_after_free.c:9
SUMMARY: AddressSanitizer: heap-use-after-free probes/heap_use_after_free.c:9 in read_slot
==4242==ABORTING
"""

ASAN_OVERFLOW = """==4243==ERROR: AddressSanitizer: heap-buffer-overflow on address 0x502 at pc 0x401
SUMMARY: AddressSanitizer: heap-buffer-overflow t.c:3 in main
"""

LSAN_LEAK = """
=================================================================
==4244==ERROR: LeakSanitizer: detected memory leaks

Direct leak of 64 byte(s) in 1 object(s) allocated from:
    #0 0x7f in malloc
    #1 0x401 in leak_block probes/leak.c:16

SUMMARY: AddressSanitizer: 64 byte(s) leaked in 1 allocation(s).
"""

UBSAN_STDERR = """probes/signed_overflow.c:12:14: runtime error: signed integer overflow: 2147483647 + 1 cannot be represented in type 'int'
    #0 0x40115f in add probes/signed_overflow.c:12
"""

TSAN_RACE = """==================
WARNING: ThreadSanitizer: data race (pid=4245)
  Write of size 4 at 0x000000404048 by thread T2:
    #0 racer probes/data_race.c:15
SUMMARY: ThreadSanitizer: data race probes/data_race.c:15 in racer
==================
ThreadSanitizer: reported 1 warnings
"""

VG_HEADER = """==4246== Memcheck, a memory error detector
==4246== Copyright (C) 2002-2022, and GNU GPL'd, by Julian Seward et al.
==4246== Command: ./memprobe
==4246==
"""

VG_CLEAN_HEAP = """==4246== HEAP SUMMARY:
==4246==     in use at exit: 0 bytes in 0 blocks
==4246==   total heap usage: 3 allocs, 3 frees, 1,280 bytes allocated
==4246==
==4246== All heap blocks were freed -- no leaks are possible
==4246==
==4246== ERROR SUMMARY: 0 errors from 0 contexts (suppressed: 0 from 0)
"""

VG_INVALID_READ = VG_HEADER + """==4246== Invalid read of size 4
==4246==    at 0x401176: read_slot (invalid_read.c:9)
==4246==  Address 0x4a5b060 is 0 bytes after a block of size 32 alloc'd
""" + VG_CLEAN_HEAP.replace("ERROR SUMMARY: 0 errors from 0", "ERROR SUMMARY: 1 errors from 1")

VG_UNINIT = VG_HEADER + """==4246== Conditional jump or move depends on uninitialised value(s)
==4246==    at 0x401186: is_positive (uninitialized_read.c:10)
""" + VG_CLEAN_HEAP.replace("ERROR SUMMARY: 0 errors from 0", "ERROR SUMMARY: 1 errors from 1")

VG_DEFINITELY_LOST = VG_HEADER + """==4246== 64 bytes in 1 blocks are definitely lost in loss record 1 of 1
==4246==    at 0x4C3: malloc (vg_replace_malloc.c:446)
==4246==
==4246== LEAK SUMMARY:
==4246==    definitely lost: 64 bytes in 1 blocks
==4246==    indirectly lost: 0 bytes in 0 blocks
==4246==      possibly lost: 0 bytes in 0 blocks
==4246==    still reachable: 0 bytes in 0 blocks
==4246==         suppressed: 0 bytes in 0 blocks
==4246==
==4246== ERROR SUMMARY: 1 errors from 1 contexts (suppressed: 0 from 0)
"""

VG_STILL_REACHABLE = VG_HEADER + """==4246== LEAK SUMMARY:
==4246==    definitely lost: 0 bytes in 0 blocks
==4246==    indirectly lost: 0 bytes in 0 blocks
==4246==      possibly lost: 0 bytes in 0 blocks
==4246==    still reachable: 1,024 bytes in 2 blocks
==4246==         suppressed: 0 bytes in 0 blocks
==4246==
==4246== ERROR SUMMARY: 0 errors from 0 contexts (suppressed: 0 from 0)
"""

VG_POSSIBLY_LOST = VG_STILL_REACHABLE.replace(
    "==4246== LEAK SUMMARY:",
    "==4246== 288 bytes in 1 blocks are possibly lost in loss record 1 of 1\n==4246== LEAK SUMMARY:").replace(
    "possibly lost: 0 bytes in 0 blocks", "possibly lost: 288 bytes in 1 blocks").replace(
    "ERROR SUMMARY: 0 errors from 0", "ERROR SUMMARY: 1 errors from 1")

VG_UNCLASSIFIED_ERROR = VG_HEADER + """==4246== Source and destination overlap in memcpy(0x4a5b040, 0x4a5b044, 8)
==4246== ERROR SUMMARY: 1 errors from 1 contexts (suppressed: 0 from 0)
"""


class CheckTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="check_report_test.")
        self.stderr = self.write("stderr.log", "")
        self.stdout = self.write("stdout.log", "memory-probe: something\n")

    def tearDown(self):
        shutil.rmtree(self.dir)

    def write(self, name, text):
        path = os.path.join(self.dir, name)
        with open(path, "w") as f:
            f.write(text)
        return path

    def check(self, expect, kind, exit_code, reports=(), failure_codes=(), stderr=None):
        out = os.path.join(self.dir, "report.json")
        argv = ["check", "--variant", "test-variant", "--probe", "p", "--expect", expect,
                "--kind", kind, "--exit-code", str(exit_code), "--output", out,
                "--stream", self.stdout, "--stream", stderr or self.stderr]
        for r in reports:
            argv += ["--report", r]
        for c in failure_codes:
            argv += ["--failure-exit-code", str(c)]
        with open(os.devnull, "w") as devnull:
            saved, sys.stdout = sys.stdout, devnull
            try:
                rc = check_report.main(argv)
            finally:
                sys.stdout = saved
        with open(out) as f:
            result = json.load(f)
        self.assertEqual(rc == 0, result["matched"])
        return result

    # Detector recognition

    def test_asan_uaf_recognized(self):
        r = self.check("heap-use-after-free", "sanitizer", 134,
                       [self.write("asan.1", ASAN_UAF)], [134, 1])
        self.assertTrue(r["matched"], r)
        self.assertIn("ERROR: AddressSanitizer: heap-use-after-free", r["signatures"])

    def test_lsan_leak_recognized(self):
        r = self.check("leak", "sanitizer", 134, [self.write("asan.1", LSAN_LEAK)], [134, 23])
        self.assertTrue(r["matched"], r)

    def test_ubsan_recognized_on_stderr(self):
        r = self.check("undefined-behavior", "sanitizer", 1, [], [134, 1],
                       stderr=self.write("err.log", UBSAN_STDERR))
        self.assertTrue(r["matched"], r)

    def test_tsan_race_recognized(self):
        r = self.check("data-race", "sanitizer", 66, [self.write("tsan.1", TSAN_RACE)], [66])
        self.assertTrue(r["matched"], r)

    def test_valgrind_invalid_read_recognized(self):
        r = self.check("invalid-read", "valgrind", 97,
                       [self.write("valgrind.1.log", VG_INVALID_READ)], [97])
        self.assertTrue(r["matched"], r)

    def test_valgrind_uninitialized_recognized(self):
        r = self.check("uninitialized-read", "valgrind", 97,
                       [self.write("valgrind.1.log", VG_UNINIT)], [97])
        self.assertTrue(r["matched"], r)

    def test_valgrind_definitely_lost_recognized(self):
        r = self.check("leak", "valgrind", 97,
                       [self.write("valgrind.1.log", VG_DEFINITELY_LOST)], [97])
        self.assertTrue(r["matched"], r)
        self.assertIn("definitely lost:", r["signatures"])

    def test_valgrind_possibly_lost_is_leak(self):
        r = self.check("leak", "valgrind", 97,
                       [self.write("valgrind.1.log", VG_POSSIBLY_LOST)], [97])
        self.assertTrue(r["matched"], r)

    # Clean control

    def test_valgrind_still_reachable_only_is_clean(self):
        r = self.check("clean", "valgrind", 0, [self.write("valgrind.1.log", VG_STILL_REACHABLE)])
        self.assertTrue(r["matched"], r)
        self.assertEqual(r["categories"], [])

    def test_valgrind_clean_requires_error_summary(self):
        r = self.check("clean", "valgrind", 0, [])
        self.assertFalse(r["matched"])

    def test_valgrind_clean_with_errors_fails(self):
        r = self.check("clean", "valgrind", 0,
                       [self.write("valgrind.1.log", VG_UNCLASSIFIED_ERROR)])
        self.assertFalse(r["matched"])
        self.assertIn("unknown-error", r["categories"])

    def test_sanitizer_clean_without_logs_passes(self):
        r = self.check("clean", "sanitizer", 0, [])
        self.assertTrue(r["matched"], r)

    def test_sanitizer_clean_with_report_fails(self):
        r = self.check("clean", "sanitizer", 0, [self.write("tsan.1", TSAN_RACE)])
        self.assertFalse(r["matched"])

    def test_clean_nonzero_exit_fails(self):
        r = self.check("clean", "sanitizer", 3, [])
        self.assertFalse(r["matched"])

    # Fail closed

    def test_missing_report_fails_when_error_expected(self):
        r = self.check("heap-use-after-free", "sanitizer", 134, [], [134])
        self.assertFalse(r["matched"])

    def test_nonexistent_report_path_fails(self):
        r = self.check("heap-use-after-free", "sanitizer", 134,
                       [os.path.join(self.dir, "asan.missing")], [134])
        self.assertFalse(r["matched"])

    def test_empty_report_fails(self):
        r = self.check("heap-use-after-free", "sanitizer", 134, [self.write("asan.1", "")], [134])
        self.assertFalse(r["matched"])
        self.assertTrue(any("empty" in x for x in r["failure_reasons"]))

    def test_unknown_report_format_fails(self):
        r = self.check("clean", "sanitizer", 0, [self.write("asan.1", "something odd\n")])
        self.assertFalse(r["matched"])
        self.assertIn("unknown-error", r["categories"])

    def test_wrong_signature_fails(self):
        r = self.check("heap-use-after-free", "sanitizer", 134,
                       [self.write("asan.1", ASAN_OVERFLOW)], [134])
        self.assertFalse(r["matched"])
        self.assertIn("unknown-error", r["categories"])

    def test_extra_finding_fails(self):
        r = self.check("heap-use-after-free", "sanitizer", 134,
                       [self.write("asan.1", ASAN_UAF), self.write("asan.2", LSAN_LEAK)], [134])
        self.assertFalse(r["matched"])

    def test_faulty_probe_exit_zero_fails(self):
        r = self.check("data-race", "sanitizer", 0, [self.write("tsan.1", TSAN_RACE)], [66])
        self.assertFalse(r["matched"])

    def test_unexpected_exit_code_fails(self):
        # e.g. SIGSEGV unrelated to the detector
        r = self.check("data-race", "sanitizer", 139, [self.write("tsan.1", TSAN_RACE)], [66])
        self.assertFalse(r["matched"])

    def test_signature_only_in_stdout_counts_as_finding(self):
        # A program cannot pass as clean by printing nothing but a report.
        self.stdout = self.write("stdout.log", TSAN_RACE)
        r = self.check("clean", "sanitizer", 0, [])
        self.assertFalse(r["matched"])


class SummarizeTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="check_report_sum.")

    def tearDown(self):
        shutil.rmtree(self.dir)

    def put(self, probe, matched):
        d = os.path.join(self.dir, "probes", probe)
        os.makedirs(d)
        with open(os.path.join(d, "report.json"), "w") as f:
            json.dump({"probe": probe, "expected": probe, "matched": matched,
                       "categories": [], "process_exit_code": 0, "failure_reasons": []}, f)

    def summarize(self, probes, stage_failure=""):
        argv = ["summarize", "--variant", "v", "--artifact-dir", self.dir]
        for p in probes:
            argv += ["--expected-probe", p]
        if stage_failure:
            argv += ["--stage-failure", stage_failure]
        with open(os.devnull, "w") as devnull:
            saved, sys.stdout = sys.stdout, devnull
            try:
                rc = check_report.main(argv)
            finally:
                sys.stdout = saved
        with open(os.path.join(self.dir, "summary.json")) as f:
            summary = json.load(f)
        self.assertTrue(os.path.exists(os.path.join(self.dir, "summary.txt")))
        self.assertEqual(rc == 0, summary["passed"])
        return summary

    def test_all_pass(self):
        self.put("clean", True)
        self.put("leak", True)
        self.assertTrue(self.summarize(["leak", "clean"])["passed"])

    def test_one_probe_failed(self):
        self.put("clean", True)
        self.put("leak", False)
        self.assertFalse(self.summarize(["leak", "clean"])["passed"])

    def test_missing_probe_result_fails(self):
        self.put("clean", True)
        self.assertFalse(self.summarize(["leak", "clean"])["passed"])

    def test_stage_failure_fails(self):
        self.put("clean", True)  # stale result from an earlier run must not count
        s = self.summarize(["clean"], stage_failure="build failed")
        self.assertFalse(s["passed"])
        self.assertEqual(s["failure_reasons"], ["build failed"])
        self.assertEqual(s["probes"][0]["status"], "not-run")
        self.assertFalse(s["probes"][0]["matched"])

    def test_no_probes_fails(self):
        self.assertFalse(self.summarize([])["passed"])


ASAN_DEADLYSIGNAL = """AddressSanitizer:DEADLYSIGNAL
=================================================================
==4250==ERROR: AddressSanitizer: SEGV on unknown address 0x000000000000
"""

TSAN_FATAL = """FATAL: ThreadSanitizer: unexpected memory mapping 0x7f0000000000-0x7f0000001000
"""

SERVER_STDERR_NOISE = """2026/10/30 18:00:00 [INFO] lua: runtime error: attempt to call a nil value
I AM READY.
"""


class ScanTest(unittest.TestCase):
    """check_report.py scan over exec.sh invocation directories."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="check_report_scan.")

    def tearDown(self):
        shutil.rmtree(self.root)

    def proc(self, node="nodeA", pid=100, detector=None, stderr="", clean=True, leakcheck=None,
             run_id="run1", stderr_complete=True):
        d = os.path.join(self.root, "nodes", node, "invocations", str(pid))
        os.makedirs(os.path.join(d, "detector"))
        with open(os.path.join(d, "launch.json"), "w") as f:
            json.dump({"pid": pid, "node": node, "run_id": run_id, "argv": ["comdb2", "db"]}, f)
        with open(os.path.join(d, "stderr.log"), "w") as f:
            f.write(stderr)
        for name, text in (detector or {}).items():
            with open(os.path.join(d, "detector", name), "w") as f:
                f.write(text)
        if clean:
            open(os.path.join(d, "clean-exit"), "w").close()
        if stderr_complete:
            open(os.path.join(d, "stderr-complete"), "w").close()
        if leakcheck is not None:
            with open(os.path.join(d, "leakcheck"), "w") as f:
                f.write(leakcheck)
        return d

    def scan(self, kind="sanitizer", leak="authoritative", functional="pass", nodes=("nodeA",),
             expect_server="yes"):
        argv = ["scan", "--root", self.root, "--variant", "v", "--testcase", "t", "--kind", kind,
                "--leak-checking", leak, "--functional-status", functional, "--run-id", "run1",
                "--expect-server", expect_server]
        for n in nodes:
            argv += ["--expected-node", n]
        with open(os.devnull, "w") as devnull:
            saved, sys.stdout = sys.stdout, devnull
            try:
                rc = check_report.main(argv)
            finally:
                sys.stdout = saved
        with open(os.path.join(self.root, "summary.json")) as f:
            summary = json.load(f)
        self.assertTrue(os.path.exists(os.path.join(self.root, "summary.txt")))
        self.assertEqual(rc == 0, summary["memory_status"] in ("pass", "not-run"))
        return summary

    def test_clean_sanitizer_server_without_detector_file(self):
        self.proc(stderr=SERVER_STDERR_NOISE)
        s = self.scan()
        self.assertEqual((s["memory_status"], s["overall_status"]), ("pass", "pass"), s)

    def test_asan_uaf(self):
        self.proc(detector={"asan.100": ASAN_UAF}, clean=False)
        s = self.scan()
        self.assertEqual(s["memory_status"], "fail")
        self.assertIn("heap-use-after-free", s["findings"])

    def test_lsan_leak_overrides_functional_pass(self):
        self.proc(detector={"asan.100": LSAN_LEAK})
        s = self.scan(functional="pass")
        self.assertEqual((s["functional_status"], s["memory_status"], s["overall_status"]),
                         ("pass", "fail", "fail"))
        self.assertEqual(s["findings"], ["leak"])

    def test_functional_failure_keeps_memory_findings(self):
        self.proc(detector={"asan.100": LSAN_LEAK})
        s = self.scan(functional="fail")
        self.assertEqual((s["memory_status"], s["overall_status"]), ("fail", "fail"))

    def test_ubsan_from_stderr(self):
        self.proc(stderr=SERVER_STDERR_NOISE + UBSAN_STDERR)
        self.assertEqual(self.scan()["findings"], ["undefined-behavior"])

    def test_tsan_race(self):
        self.proc(detector={"tsan.100": TSAN_RACE})
        self.assertEqual(self.scan(leak="disabled")["findings"], ["data-race"])

    def test_asan_deadlysignal(self):
        self.proc(detector={"asan.100": ASAN_DEADLYSIGNAL}, clean=False)
        s = self.scan()
        self.assertEqual((s["memory_status"], s["findings"]), ("fail", ["unknown-error"]))

    def test_tsan_lock_order_inversion_and_thread_leak(self):
        log = ("WARNING: ThreadSanitizer: lock-order-inversion (potential deadlock) (pid=1)\n"
               "WARNING: ThreadSanitizer: thread leak (pid=1)\n")
        self.proc(detector={"tsan.100": log})
        self.assertEqual(self.scan(leak="disabled")["findings"], ["lock-order-inversion", "thread-leak"])

    def test_valgrind_syscall_unaddressable_is_invalid_read(self):
        log = VG_INVALID_READ.replace("Invalid read of size 4",
                                      "Syscall param write(buf) points to unaddressable byte(s)")
        self.proc(detector={"valgrind.100.log": log})
        self.assertEqual(self.scan(kind="valgrind")["findings"], ["invalid-read"])

    def test_tsan_runtime_fatal(self):
        self.proc(stderr=TSAN_FATAL, clean=False)
        self.assertEqual(self.scan(leak="disabled")["findings"], ["unknown-error"])

    def test_valgrind_clean(self):
        self.proc(detector={"valgrind.100.log": VG_HEADER + VG_CLEAN_HEAP})
        self.assertEqual(self.scan(kind="valgrind")["memory_status"], "pass")

    def test_valgrind_findings(self):
        for pid, log, cat in ((1, VG_INVALID_READ, "invalid-read"), (2, VG_UNINIT, "uninitialized-read"),
                              (3, VG_DEFINITELY_LOST, "leak")):
            self.proc(pid=pid, detector={"valgrind.%d.log" % pid: log})
        s = self.scan(kind="valgrind")
        self.assertEqual(s["findings"], ["invalid-read", "leak", "uninitialized-read"])

    def test_valgrind_missing_log_is_incomplete(self):
        self.proc()
        self.assertEqual(self.scan(kind="valgrind")["memory_status"], "incomplete")

    def test_valgrind_killed_with_checkpoint_is_incomplete(self):
        # valgrind-system is authoritative for leaks; a killed process has no leak summary
        self.proc(detector={"valgrind.100.log": VG_HEADER}, clean=False, leakcheck="unavailable\n")
        s = self.scan(kind="valgrind")
        self.assertEqual(s["memory_status"], "incomplete", s)
        self.assertTrue(s["processes"][0]["intentional_kill_checkpoint"])
        self.assertTrue(any("leak summary" in r for r in s["incomplete_reasons"]))

    def test_valgrind_unrecognised_context_beside_known_one(self):
        log = VG_DEFINITELY_LOST.replace(
            "==4246== LEAK SUMMARY:",
            "==4246== Source and destination overlap in memcpy(0x1, 0x2, 8)\n==4246== LEAK SUMMARY:").replace(
            "ERROR SUMMARY: 1 errors from 1 contexts", "ERROR SUMMARY: 2 errors from 2 contexts")
        self.proc(detector={"valgrind.100.log": log})
        self.assertEqual(self.scan(kind="valgrind")["findings"], ["leak", "unknown-error"])

    def test_leakcheck_with_leaks_but_no_report_is_a_leak(self):
        self.proc(clean=False, leakcheck="leaks 1\n")
        s = self.scan()
        self.assertEqual((s["memory_status"], s["findings"]), ("fail", ["leak"]))

    def test_malformed_leakcheck_is_incomplete(self):
        self.proc(clean=False, leakcheck="leaks\n")
        self.assertEqual(self.scan()["memory_status"], "incomplete")

    def test_missing_stderr_completion_is_incomplete(self):
        self.proc(stderr_complete=False)
        self.assertEqual(self.scan()["memory_status"], "incomplete")

    def test_record_from_another_run_is_incomplete(self):
        self.proc(run_id="older-run")
        s = self.scan()
        self.assertEqual(s["memory_status"], "incomplete")
        self.assertTrue(any("another run" in r for r in s["incomplete_reasons"]))

    def test_killed_asan_with_checkpoint_is_complete(self):
        self.proc(clean=False, leakcheck="leaks 0\n")
        s = self.scan()
        self.assertEqual(s["memory_status"], "pass", s)
        self.assertTrue(s["processes"][0]["intentional_kill_checkpoint"])

    def test_killed_asan_with_checkpoint_leak_fails(self):
        self.proc(detector={"asan.100": LSAN_LEAK}, clean=False, leakcheck="leaks 1\n")
        self.assertEqual(self.scan()["memory_status"], "fail")

    def test_killed_asan_without_lsan_checkpoint_is_incomplete(self):
        self.proc(clean=False, leakcheck="unavailable\n")
        self.assertEqual(self.scan()["memory_status"], "incomplete")

    def test_no_clean_exit_and_no_checkpoint_is_incomplete(self):
        self.proc(clean=False)
        s = self.scan()
        self.assertEqual((s["memory_status"], s["overall_status"]), ("incomplete", "fail"))
        self.assertFalse(s["coverage_complete"])

    def test_missing_remote_node_is_incomplete(self):
        self.proc(node="nodeA")
        s = self.scan(nodes=("nodeA", "nodeB"))
        self.assertEqual(s["memory_status"], "incomplete")
        self.assertTrue(any("nodeB" in r for r in s["incomplete_reasons"]))

    def test_recorded_incomplete_reason(self):
        self.proc()
        with open(os.path.join(self.root, "incomplete.txt"), "w") as f:
            f.write("cannot collect memory artifacts from nodeB\n")
        self.assertEqual(self.scan()["memory_status"], "incomplete")

    def test_one_finding_node_fails_whole_test(self):
        self.proc(node="nodeA")
        self.proc(node="nodeB", pid=200, detector={"asan.200": ASAN_UAF}, clean=False)
        s = self.scan(nodes=("nodeA", "nodeB"))
        self.assertEqual((s["memory_status"], s["overall_status"]), ("fail", "fail"))

    def test_fqdn_node_matches_short_name(self):
        self.proc(node="nodeA.example.com")
        self.assertEqual(self.scan(nodes=("nodeA",))["memory_status"], "pass")

    def test_normal_exit_without_clean_exit_is_covered(self):
        # e.g. a server a negative test expects to fail at startup
        d = self.proc(clean=False)
        open(os.path.join(d, "exited"), "w").close()
        s = self.scan()
        self.assertEqual(s["memory_status"], "pass", s)
        self.assertEqual((s["processes"][0]["clean_exit"], s["processes"][0]["normal_exit"]), (False, True))

    def test_unreadable_launch_record_is_incomplete(self):
        d = self.proc()
        with open(os.path.join(d, "launch.json"), "w") as f:
            f.write("{not json")
        self.assertEqual(self.scan()["memory_status"], "incomplete")

    def test_server_started_without_launcher_is_incomplete(self):
        self.proc()
        os.makedirs(os.path.join(self.root, "uncovered"))
        with open(os.path.join(self.root, "uncovered", "nodeA.4242"), "w") as f:
            f.write("/b/comdb2 db --lrl db.lrl\n")
        s = self.scan()
        self.assertEqual(s["memory_status"], "incomplete")
        self.assertTrue(any("without the memory launcher" in r for r in s["incomplete_reasons"]))

    def test_no_server_process_when_one_was_expected_is_incomplete(self):
        # e.g. the launcher was never used: must not pass
        s = self.scan(functional="pass")
        self.assertEqual((s["memory_status"], s["overall_status"]), ("incomplete", "fail"))

    def test_no_server_process_in_unit_test_is_not_run(self):
        s = self.scan(functional="pass", expect_server="no")
        self.assertEqual((s["memory_status"], s["overall_status"]), ("not-run", "pass"))


if __name__ == "__main__":
    unittest.main()
