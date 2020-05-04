local function main()
	local statement1 = db:prepare("INSERT INTO t1 VALUES(?)")
	statement1:bind(1, 'pre')
	statement1:exec()
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
	statement2:bind(1, oldi)
	statement2:exec()
	local statement3 = db:prepare("INSERT INTO t2 VALUES(?)")
	statement3:bind(1, newi)
	statement3:exec()
	local statement4 = db:exec("SELECT s FROM t1")
	local row4 = statement4:fetch()
	while row4 do
		row4 = statement4:fetch()
	end
	local statement5 = db:exec("SELECT s FROM t2")
	local row5 = statement5:fetch()
	while row5 do
		row5 = statement5:fetch()
	end
	consumer:consume()
end
