local function main()
	db:setmaxinstructions(100000)
	local dbtab = db:prepare("insert into prepare values(@i, @j, @s)")
	local max = 2000
	local i = 1

	db:begin()
	for i = 1, max do
		local j = max - i
		dbtab:bind("i", i)
		dbtab:bind("j", j)
		dbtab:bind("s", i .. j)
		dbtab:exec()
	end
	db:commit()

	dbtab = db:exec("select count(*) from prepare")
	dbtab:emit()
end
