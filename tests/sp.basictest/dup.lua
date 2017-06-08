local function main()
	local dup = db:table("dup")
	if dup == nil then return -201 end

	local rc

	rc = dup:insert({i = 0})
	if rc ~= 0 then return -202 end

	rc = db:begin()
	if rc ~= 0 then return -203 end

	rc = dup:insert({i = 1})
	if rc ~= 0 then return -204 end

	rc = dup:insert({i = 1})
	if rc ~= 0 then return -205 end

	rc = db:commit()

	dup:emit()

	if rc ~= 0 then return -206, db:error() end

	return -207

end
