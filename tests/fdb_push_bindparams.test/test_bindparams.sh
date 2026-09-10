#!/usr/bin/env bash

# Reproducer for: fdb push write leaks bound parameters across the statements
# of a client transaction.
#
# handle_fdb_push_write() runs every pushed write of a client transaction on
# the SAME cdb2api handle (tran->fcon.hndl, created once by
# fdb_trans_begin_or_join() and reused until commit/rollback).
# _run_statement() calls set_bound_parameters(), which
# cdb2_bind_param()/cdb2_bind_index() the current statement's parameters onto
# that handle -- and nothing ever calls cdb2_clearbindings().
#
# cdb2_bind_param_helper() APPENDS (hndl->n_bindvars++); it does not replace.
# So statement N of the transaction ships its own parameters PLUS every
# parameter bound by statements 1..N-1:
#
#   begin
#   insert into rem.t  values(10, @b1)     -> remote receives 1 bindvar
#   insert into rem.t2 values(@id2, @b2)   -> remote receives 3 bindvars
#   commit
#
# On the remote, bind_parameters() (db/sqlinterfaces.c) loops over every
# received bindvar.  @b1 has no placeholder in the second statement, so
# sqlite3_bind_parameter_index() returns 0, p.pos <= 0, and the statement
# fails with "bad parameter name:@b1".
#
# The leaked bindvars are also dangling: their value pointers reference
# push->params and the dts array, both freed as soon as the statement that
# created them finished (fdb_push_free() on the next prepare, free(dts) at the
# end of _run_statement()).  Re-sending them is a use-after-free.
#
# Test layout:
#   Test 1  control -- standalone pushed writes with binds (fresh handle each
#                      statement, no leak possible)
#   Test 2  control -- transaction with pushed writes and NO binds
#   Test 2b control -- transaction with pushed READS (bound-param selects)
#                      against a remote table; not affected -- see note below
#   Test 3  control -- shows directly, on the remote, what a leaked bindvar
#                      does to a statement that has no placeholder for it
#   Test 4  BUG     -- transaction, 1 named param then 2 named params
#   Test 5  BUG     -- transaction, 2 positional params then 1
#
# Note on Test 2b (in-transaction pushed reads): fdb_push_setup(), the
# read-side counterpart of fdb_push_write_setup(), explicitly bails out
# (`return -1`) when `clnt->intrans || clnt->in_client_trans`
# (db/fdb_push.c). So a SELECT against a remote table issued inside a client
# transaction never engages the push path at all -- it falls back to the
# legacy fdb_fend.c protocol, which does not use cdb2api bind calls
# (cdb2_bind_param/cdb2_bind_index only appear in fdb_push.c, reached only via
# handle_fdb_push()/handle_fdb_push_write(), both gated on clnt->fdb_push being
# set). So the bindvar-accumulation bug described above cannot occur for reads
# in a transaction; fdb_push_write_setup() has no equivalent intrans guard,
# which is why only pushed writes are affected. Test 2b is a control that
# demonstrates this functionally.

a_dbname=$1
a_cdb2config=$2
a_remdbname=$3
a_remcdb2config=$4
a_dbdir=$5
a_testdir=$6

SRC_OPTS="--cdb2cfg ${a_cdb2config}"
REM_OPTS="--cdb2cfg ${a_remcdb2config}"
SRC="cdb2sql ${SRC_OPTS}"
REM="cdb2sql ${REM_OPTS}"

failures=0

fail()
{
    echo "FAIL: $*" >&2
    failures=$((failures + 1))
}

# Pin to one host so every statement of a transaction lands on the same node
mach=$($SRC --tabs $a_dbname default "select comdb2_host()")
[[ -z $mach ]] && { echo "could not determine local host" >&2; exit 1; }

run_src()
{
    cdb2sql -s ${SRC_OPTS} --host $mach $a_dbname default - 2>&1
}

run_rem()
{
    cdb2sql -s ${REM_OPTS} $a_remdbname default - 2>&1
}

check_row()
{
    local tbl=$1 id=$2 want=$3 who=$4
    local got
    got=$($REM --tabs $a_remdbname default "select b1 from $tbl where id=$id")
    [[ "$got" == "$want" ]] || fail "$who: remote $tbl.b1 for id=$id is '$got', expected '$want'"
}

# ── Test 1 (control): standalone pushed writes with bound parameters ────────
# Outside a transaction each pushed write gets its own short-lived handle, so
# bindings cannot leak between statements.
echo "Test 1 (control): standalone pushed inserts with bound parameters"

out=$(run_src <<EOF
@bind CDB2_CSTRING b1 alpha
insert into LOCAL_${a_remdbname}.t values(1, @b1)
@bind CDB2_INTEGER id2 1
@bind CDB2_CSTRING b2 beta
insert into LOCAL_${a_remdbname}.t2 values(@id2, @b2)
EOF
)
rc=$?
if (( rc != 0 )); then
    fail "Test 1: standalone pushed inserts failed rc=$rc; output: $out"
else
    check_row t 1 alpha "Test 1"
    check_row t2 1 beta "Test 1"
    echo "Test 1 done."
fi

# ── Test 2 (control): transaction with pushed writes, no bound parameters ───
# Establishes that pushing several writes of one client transaction to the
# same remote works when no parameters are involved.
echo "Test 2 (control): transaction with pushed inserts, no bound parameters"

out=$(run_src <<EOF
begin
insert into LOCAL_${a_remdbname}.t values(2, 'noparam1')
insert into LOCAL_${a_remdbname}.t2 values(2, 'noparam2')
commit
EOF
)
rc=$?
if (( rc != 0 )); then
    fail "Test 2: transaction without bound parameters failed rc=$rc; output: $out"
else
    check_row t 2 noparam1 "Test 2"
    check_row t2 2 noparam2 "Test 2"
    echo "Test 2 done."
fi

# ── Test 2b (control): in-transaction pushed reads with bound parameters ────
# fdb_push_setup() (the read-side counterpart of fdb_push_write_setup()) bails
# out whenever clnt->intrans || clnt->in_client_trans, so a SELECT against a
# remote table inside a transaction never engages the push/cdb2api path --
# it runs over the legacy fdb_fend.c protocol instead. Two selects with
# different bound-parameter shapes in the same transaction should both return
# correct results; if push-style bindvar accumulation applied to reads too,
# the second select here would see extra/dangling bindvars just like Test 4/5.
echo "Test 2b (control): transaction with pushed-table reads, bound parameters"

out=$(run_src <<EOF
begin
@bind CDB2_INTEGER id 1
select b1 from LOCAL_${a_remdbname}.t where id=@id
@bind CDB2_INTEGER id2 2
@bind CDB2_CSTRING b2 noparam2
select b1 from LOCAL_${a_remdbname}.t2 where id=@id2 and b1=@b2
commit
EOF
)
rc=$?
echo "$out"
if (( rc != 0 )); then
    fail "Test 2b: in-transaction bound-param reads failed rc=$rc"
fi
if echo "$out" | grep -qi "parameters provided\|bad parameter name"; then
    fail "Test 2b: in-transaction bound-param reads hit a parameter-count/name error -- push read path would be affected too"
fi
if ! echo "$out" | grep -q "b1='alpha'"; then
    fail "Test 2b: first select did not return expected row 'alpha'"
fi
if ! echo "$out" | grep -q "b1='noparam2'"; then
    fail "Test 2b: second select did not return expected row 'noparam2'"
fi

# ── Test 3 (control): what an extra bindvar does on the remote ──────────────
# Run against the remote directly, sending three bindvars to a statement that
# has two placeholders -- exactly the wire content the push handle produces
# for the second statement of Test 4.
echo "Test 3 (control): remote statement with one extra, unreferenced bindvar"

out=$(run_rem <<EOF
@bind CDB2_CSTRING b1 gamma
@bind CDB2_INTEGER id2 3
@bind CDB2_CSTRING b2 delta
insert into t2 values(@id2, @b2)
EOF
)
rc=$?
echo "remote said: $out"
if (( rc == 0 )); then
    echo "  (remote tolerated the extra bindvar)"
else
    echo "  (remote rejected the extra bindvar -- this is what the leak causes)"
fi

# ── Test 4 (BUG): named parameters leak across the transaction ──────────────
# Statement 1 binds @b1.  Statement 2 binds @id2/@b2, but the remote also
# receives the stale @b1, for which statement 2 has no placeholder.
echo "Test 4: transaction with two pushed inserts, named bound parameters"

out=$(run_src <<EOF
begin
@bind CDB2_CSTRING b1 gamma
insert into LOCAL_${a_remdbname}.t values(10, @b1)
@bind CDB2_INTEGER id2 10
@bind CDB2_CSTRING b2 delta
insert into LOCAL_${a_remdbname}.t2 values(@id2, @b2)
commit
EOF
)
rc=$?
echo "$out"
if (( rc != 0 )); then
    fail "Test 4: transaction failed rc=$rc -- stale bindvars leaked onto the transaction handle"
fi
check_row t 10 gamma "Test 4"
check_row t2 10 delta "Test 4"

# ── Test 5 (BUG): positional parameters, shrinking parameter count ──────────
# Statement 1 binds indexes 1 and 2.  Statement 2 has a single placeholder but
# the remote receives 3 bindvars, one of them for a position the statement does
# not have.
echo "Test 5: transaction with positional parameters, second statement binds fewer"

out=$(run_src <<EOF
begin
@bind CDB2_INTEGER 1 20
@bind CDB2_CSTRING 2 epsilon
insert into LOCAL_${a_remdbname}.t values(?, ?)
@bind CDB2_CSTRING 1 zeta
insert into LOCAL_${a_remdbname}.t2 values(20, ?)
commit
EOF
)
rc=$?
echo "$out"
if (( rc != 0 )); then
    fail "Test 5: transaction failed rc=$rc -- stale bindvars leaked onto the transaction handle"
fi
check_row t 20 epsilon "Test 5"
check_row t2 20 zeta "Test 5"

# ── Test 6 (BUG): same as Test 4, with verifyretry off ──────────────────────
# With verifyretry off handle_fdb_push_write() calls cdb2_get_effects() after
# every statement, so the remote's complaint is reported at the failing
# statement instead of being swallowed until commit.  This is where the actual
# "parameters in stmt:N parameters provided:N+M" text shows up.
echo "Test 6: same transaction with verifyretry off (surfaces the remote error)"

out=$(run_src <<EOF
set verifyretry off
begin
@bind CDB2_CSTRING b1 eta
insert into LOCAL_${a_remdbname}.t values(30, @b1)
@bind CDB2_INTEGER id2 30
@bind CDB2_CSTRING b2 theta
insert into LOCAL_${a_remdbname}.t2 values(@id2, @b2)
commit
EOF
)
rc=$?
echo "$out"
if (( rc != 0 )); then
    fail "Test 6: transaction failed rc=$rc -- stale bindvars leaked onto the transaction handle"
fi
if echo "$out" | grep -q "parameters provided"; then
    fail "Test 6: remote reported a parameter count mismatch -- leaked bindvars"
fi
check_row t 30 eta "Test 6"
check_row t2 30 theta "Test 6"

# ── Summary ─────────────────────────────────────────────────────────────────
if (( failures != 0 )); then
    echo "$failures check(s) failed" >&2
    exit 1
fi

echo "All tests passed."
