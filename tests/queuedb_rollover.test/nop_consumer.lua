local function main()
	local consumer = db:consumer()
	consumer:consume()
end
