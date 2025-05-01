select 'make sure we can set in-range values'
exec procedure sys.cmd.send('test_tunable_int_limit 0')
exec procedure sys.cmd.send('test_tunable_int64_limit 0')
exec procedure sys.cmd.send('test_tunable_int_limit 2147483647')
exec procedure sys.cmd.send('test_tunable_int64_limit 9223372036854775807')
exec procedure sys.cmd.send('test_tunable_int_signed_limit -2147483648')
exec procedure sys.cmd.send('test_tunable_int64_signed_limit -9223372036854775808')
exec procedure sys.cmd.send('test_tunable_int_signed_limit 2147483647')
exec procedure sys.cmd.send('test_tunable_int64_signed_limit 9223372036854775807')

select 'make sure we cant set out-of-range values'
exec procedure sys.cmd.send('test_tunable_int_limit -1')
exec procedure sys.cmd.send('test_tunable_int64_limit -1')
exec procedure sys.cmd.send('test_tunable_int_limit 2147483648')
exec procedure sys.cmd.send('test_tunable_int64_limit 9223372036854775808')
exec procedure sys.cmd.send('test_tunable_int_signed_limit -2147483649')
exec procedure sys.cmd.send('test_tunable_int64_signed_limit -9223372036854775809')
exec procedure sys.cmd.send('test_tunable_int_signed_limit 2147483648')
exec procedure sys.cmd.send('test_tunable_int64_signed_limit 9223372036854775808')
