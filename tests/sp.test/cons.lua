local function main()
	local consumer = db:consumer()
	local audit = db:table("audit")
	while true do
		local event = consumer:get()
		local tp = event.type
		local inew, iold
		if tp == 'add' then
			inew = event.new.i
		elseif tp == 'del' then
			iold = event.old.i
		end

		db:begin()
			consumer:consume()
			audit:insert({added_by='consumer',type=tp, inew=inew, iold=iold})
		db:commit()
	end
end
