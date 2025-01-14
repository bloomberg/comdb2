# Snapisol

TODOS:
- [ ] Fix bug[^1] exposed by `t12_01.req`


[^1]: A snapshot reader should fail
	with an error if it tries to read a table that 
	was schema changed after the snapshot reader
	started. When a certain race condition is triggered,
	a snapshot reader can wrongly succeed even
	when the table that it reads was schema changed.
	`t12_01.req` often exposes this bug.
