local function main()
	local consumer = db:consumer()
	consumer:get()
	consumer:consume()
end
