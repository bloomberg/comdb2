local function main(...)
	for k, v in ipairs(arg) do
		db:emit(v)
	end
end
