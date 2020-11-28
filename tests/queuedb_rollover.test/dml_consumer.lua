local function main(test_no)
	db:begin()
	local statement1 = db:prepare("INSERT INTO t1 VALUES(?)")
	if statement1 == nil then return db:error() end
	statement1:bind(1, 'pre ' .. tostring(test_no))
	local rc1 = statement1:exec()
	if rc1 ~= 0 then return db:error() end
	local consumer = db:consumer()
	local event = consumer:get()
	local oldi = "<nil>"
	local newi = "<nil>"
	if event.old ~= nil then
		oldi = tostring(event.old.i)
	end
	if event.new ~= nil then
		newi = tostring(event.new.i)
	end
	local statement2 = db:prepare("INSERT INTO t1 VALUES(?)")
	if statement2 == nil then return db:error() end
	statement2:bind(1, oldi)
	local rc2 = statement2:exec()
	if rc2 ~= 0 then return db:error() end
	local statement3 = db:prepare("INSERT INTO t2 VALUES(?)")
	if statement3 == nil then return db:error() end
	statement3:bind(1, newi)
	local rc3 = statement3:exec()
	if rc3 ~= 0 then return db:error() end
	local statement4 = db:exec("SELECT s FROM t1 ORDER BY CAST(s AS INTEGER), s")
	if statement4 == nil then return db:error() end
	local row4 = statement4:fetch()
	while row4 do
		consumer:emit(row4)
		row4 = statement4:fetch()
	end
	local statement5 = db:exec("SELECT s FROM t2 ORDER BY CAST(s AS INTEGER), s")
	if statement5 == nil then return db:error() end
	local row5 = statement5:fetch()
	while row5 do
		consumer:emit(row5)
		row5 = statement5:fetch()
	end
	consumer:consume()
	db:commit()
end
