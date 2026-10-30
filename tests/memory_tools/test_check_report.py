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


if __name__ == "__main__":
    unittest.main()
