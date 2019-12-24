local function main()
	local consumer = db:consumer()
	db:begin()
	consumer:consume()
	db:commit()
end
