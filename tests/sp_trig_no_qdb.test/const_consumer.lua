local function main(i_must_have_value)
	local consumer = db:consumer()
	local event = consumer:get()
	local newi = "<nil>"
	if event.new ~= nil then
		newi = tostring(event.new.i)
	end
	if (newi == tostring(i_must_have_value)) then
		consumer:consume()
	else
		local msg = "FAILED: wanted " .. tostring(i_must_have_value) ..
		            ", got " .. newi
		db:emit(msg)
	end
end
