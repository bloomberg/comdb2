-- Upstream SQLite regexp tests live in yast.test/regexp1.test and regexp2.test;
-- these cover what they don't.
select ' ' regexp '[ ]' as match;
select ' ' regexp '[^ ]' as match;
select regexp('^.$', 'é') as match;
select null regexp 'a' as match;
select 'a' regexp null as match;
select 'a' regexp 'a{0,0}' as match;
select 'a' regexp 'a{0}' as match;
select 'a' regexp '{2,}' as match;
select 'a' regexp 'a{2' as match;
select 'a' regexp '(a' as match;
-- From SQLite test/regexp1.sql, testcase 100. Testcases 110/120 need
-- sqlite3_limit(SQLITE_LIMIT_LIKE_PATTERN_LENGTH), which comdb2 does not expose.
select 'abcdefg' regexp '((((((((((((((((((abcdefg))))))))))))))))))' as match;
