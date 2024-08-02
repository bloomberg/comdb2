source util.sh

function fixture_src_tbl_and_dst_tbl_are_the_same() {
	local -n _src_tbl="$1" _dst_tbl="$2"
	_src_tbl=foo _dst_tbl=foo
	query_src_db "create table $_src_tbl(i int unique)"
	query_src_db "insert into $_src_tbl values(1)"
	query_dst_db "create table $_dst_tbl(i int unique)"
}

function fixture_src_tbl_and_dst_tbl_have_different_schemas() {
	local -n _src_tbl="$1" _dst_tbl="$2"
	_src_tbl=foo _dst_tbl=foo
	query_src_db "create table $_src_tbl(i int unique)"
	query_src_db "insert into $_src_tbl values(1)"
	query_dst_db "create table $_dst_tbl(i int unique, j int unique)"
}

function fixture_src_tbl_and_dst_tbl_have_different_names() {
	local -n _src_tbl="$1" _dst_tbl="$2"
	_src_tbl=foo _dst_tbl=bar
	query_src_db "create table $_src_tbl(i int unique)"
	query_src_db "insert into $_src_tbl values(1)"
	query_dst_db "create table $_dst_tbl(b blob)"
}

function fixture_src_tbl_has_noblob_dst_tbl_has_blob() {
	local -n _src_tbl="$1" _dst_tbl="$2"
	_src_tbl=foo _dst_tbl=foo
	query_src_db "create table $_src_tbl(i int unique)"
	query_src_db "insert into $_src_tbl values(1)"
	query_dst_db "create table $_dst_tbl(b blob)"
}

function fixture_src_tbl_has_blob_dst_tbl_has_no_blob() {
	local -n _src_tbl="$1" _dst_tbl="$2"
	_src_tbl=foo _dst_tbl=foo
	query_src_db "create table $_src_tbl(b blob)"
	query_src_db "insert into $_src_tbl values(x'f00f')"
	query_dst_db "create table $_dst_tbl(i int unique)"
}

function fixture_src_tbl_was_schema_changed() {
	local -n _src_tbl="$1" _dst_tbl="$2"
	_src_tbl=foo _dst_tbl=foo
	query_src_db "create table $_src_tbl(i int unique)"
	query_src_db "insert into $_src_tbl values(1)"
	query_src_db "alter table $_src_tbl add column j int"
	query_src_db "insert into $_src_tbl values(2, 3)"
	query_src_db "alter table $_src_tbl add column b blob"
	query_src_db "insert into $_src_tbl values(4, 5, x'f00f')"
	query_dst_db "create table $_dst_tbl(i int unique)"
}

FIXTURES=$(compgen -A function | grep -oh "fixture_\w*")
