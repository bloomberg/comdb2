local function main()
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
	print(event.type .. ", " .. oldi .. ", " .. newi)
	consumer:consume()
end
