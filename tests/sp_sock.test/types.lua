local ret = 0
local function printwhere(from, info)
	ret = -201
	local line = info.currentline
	local func = info.name
	db:emit(string.format('failed test %s -> %s @ line: %d',
	  from, tostring(func), line))
end

local function printsp(...)
	print("print from sp - ", unpack(arg))
end

local function testeq(from, v1, v2)
	--printsp('testeq '..type(v1)..' & '..type(v2))
	if not (v1 == v2) then
		printwhere(from, debug.getinfo(1))
	end
	if not (v2 == v1) then
		printwhere(from, debug.getinfo(1))
	end
	if v1 ~= v2 then 
		printwhere(from, debug.getinfo(1))
	end
	if v2 ~= v1 then 
		printwhere(from, debug.getinfo(1))
	end
	if v1 < v2 then
		printwhere(from, debug.getinfo(1))
	end
	if v1 > v2 then
		printwhere(from, debug.getinfo(1))
	end
	if v2 > v1 then
		printwhere(from, debug.getinfo(1))
	end
	if v2 < v1 then
		printwhere(from, debug.getinfo(1))
	end
	if not (v1 >= v2) then
		printwhere(from, debug.getinfo(1))
	end
	if not (v1 <= v2) then
		printwhere(from, debug.getinfo(1))
	end
	if not (v2 >= v1) then
		printwhere(from, debug.getinfo(1))
	end
	if not (v2 <= v1) then
		printwhere(from, debug.getinfo(1))
	end
end

local function strcmp()
	declare cstr 'cstring'
	local lstr = 'hello'
	cstr := lstr
	testeq('strcmp', cstr, lstr)
end

local function intnumcmp()
	declare i 'int'
	local n = 10
	i := n
	testeq('intcmp', i, n)
end

local function intrealcmp()
	local n = 10
	declare i 'int'
	declare r 'real'
	i := n
	r := n
	testeq('intrealcmp', i, r)
end

local function realnumcmp()
	declare r 'real'
	local n = 10
	r := n
	testeq('realcmp', r, n)
end

local function tabcmp_ll(from, k1, k2)
	--printsp('tabcmp '..type(k1)..' & '..type(k2))
	local value = 99
	local tab = {}
	tab[k1] = value

	if tab[k2] == nil then
		printwhere(from, debug.getinfo(1))
	end

	if not (tab[k2] == value) then
		printwhere(from, debug.getinfo(1))
	end

	if tab[k2] ~= value then
		printwhere(from, debug.getinfo(1))
	end

	-- Replace key
	value = value - 1
	--printsp("replacing "..type(k1).." with "..type(k2))
	tab[k2] = value

	if tab[k2] == nil then
		printwhere(from, debug.getinfo(1))
	end

	if not (tab[k2] == value) then
		printwhere(from, debug.getinfo(1))
	end

	if tab[k2] ~= value then
		printwhere(from, debug.getinfo(1))
	end

	local checked = false
	for k, v in pairs(tab) do
		if v == value then
			if type(k) == type(k2) then
				checked = true
			end
		end
	end
	if not checked then
		printwhere(from, debug.getinfo(1))
	end
end

local function tabcmp(from, k1, k2)
	tabcmp_ll(from, k1, k2)
	tabcmp_ll(from, k2, k1)
end

local function strtable()
	local s = 'hello'
	declare c 'cstring'
	c := s
	tabcmp('strtable', c, s)
end

local function blobtable()
	local blob = x'deadbeef'
	declare i 'blob'
	i := blob
	tabcmp('blobtable', blob, i)
end


local function intnumtable()
	local n = 10
	declare i 'int'
	i := n
	tabcmp('intnumtable', i, n)
end

local function intrealtable()
	local n = 10
	declare i 'int'
	declare r 'real'
	i := n
	r := n
	tabcmp('intrealtable', i, r)
end

local function realnumtable()
	local n = 10
	declare r 'real'
	r := n
	tabcmp('realnumtable', r, n)
end

local function arraytest(from, n)
	local t
	local a = {1,2,3,4,5,6,7,8,9,10}
	for k, v in pairs(n) do
		a[v] = v
		t = type(v)
	end
	local checked = false
	for k, v in ipairs(a) do
		if type(v) ~= t then
			printwhere(from, debug.getinfo(1))
		end
		checked = true
	end
	if not checked then
		printwhere(from, debug.getinfo(1))
	end
end

local function intarray()
	--generate some comdb2 ints
	local stmt = db:exec("select 1,2,3,4,5,6,7,8,9,10")
	local row = stmt:fetch()
	stmt:close()
	arraytest('intarray', row)
end

local function realarray()
	--generate some comdb2 reals 
	local stmt = db:exec("select 1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0")
	local row = stmt:fetch()
	stmt:close()
	arraytest('intarray', row)
end

local function main()
	db:num_columns(1)
	db:column_type('text', 1)
	db:column_name('err', 1)

	strcmp()
	intnumcmp()
	intrealcmp()
	realnumcmp()

	strtable()
	blobtable()
	intnumtable()
	intrealtable()
	realnumtable()

	intarray()
	realarray()

	return ret
end
