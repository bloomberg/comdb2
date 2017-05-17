local function gen(tbl, a, b)
	local stmt = db:prepare("insert into "..tbl.."(i) values(@a),(@b),(@c),(@d),(@e),(@f),(@g),(@h),(@i),(@j)")
	local params = 10
	local num = b - a + 1
	local per_tran
	if num > 1000 then
		per_tran = 1000
	else
		per_tran = num
	end
	local loop = (b - a + 1) / per_tran
	local per_loop = per_tran / params
	local i = a
	for j = 1, loop do
		db:begin()
		for k = 1, per_loop do
			stmt:bind('a', i + 0)
			stmt:bind('b', i + 1)
			stmt:bind('c', i + 2)
			stmt:bind('d', i + 3)
			stmt:bind('e', i + 4)
			stmt:bind('f', i + 5)
			stmt:bind('g', i + 6)
			stmt:bind('h', i + 7)
			stmt:bind('i', i + 8)
			stmt:bind('j', i + 9)
			stmt:exec()
			i = i + params
		end
		db:commit()
	end
end

local function main(tbl, to, num_thds)
	local from = 1
	if tbl == nil then tbl = "nums" end
	if to == nil then to = 100000 end
	if num_thds == nil then num_thds = 10 end
	local thds = {}
	local a = from
	local b = to / num_thds
	for i = 1, num_thds do
		table.insert(thds, db:create_thread(gen, tbl, a, a + b - 1))
		a = a + b
	end
	for _,t in pairs(thds) do
		t:join()
	end
end
