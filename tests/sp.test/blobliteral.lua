local function foo(x)
	db:emit(x)
end

local function bar(x blob)
	db:emit(x)
end

local function main()
	db:num_columns(1)
	db:column_type('blob', 1)
	db:column_name('blob', 1)

	declare x "blob"
	x := x'dbdbdbdb'
	local y = x'f0f0f0f0'

	foo(x)
	foo(y)
	foo(x'deadbeef')

	bar(x)
	bar(y)
	bar(x'deadbeef')

	if x'facefeed' ~= x'deadd00d' then
		db:emit(x'ffffffff')
	end

	if x == x'dbdbdbdb' then
		db:emit(x'00000000')
	end
end
