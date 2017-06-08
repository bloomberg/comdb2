local function main()
	local t
	local row

	-- fetch loop should work
	t = db:exec("select count(*) from t")
	row = t:fetch()
	while row do
		db:emit(row)
		row = t:fetch()
	end

	-- just calling close should work
	t = db:exec("select count(*) from t")
	row = t:fetch()
	db:emit(row)
	t:close()

	-- emitting the entire stmt should work
	t = db:exec("select count(*) from t")
	t:emit()

	-- another way to do the prev
	t = db:exec("select count(*) from t")
	db:emit(t)

	-- this shouldn't fail anymore -- allow multiple stmts
	t = db:exec("select count(*) from t")
	row = t:fetch()
	db:emit(row)

	local t1 = db:exec("select count(*) from t")
	row = t1:fetch()
	db:emit(row)
end
