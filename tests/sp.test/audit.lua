local function main(event)
	local audit = db:table("audit")	
	local tp = event.type
	local inew, iold
	if tp == 'add' then
		inew = event.new.i
	elseif tp == 'del' then
		iold = event.old.i
	end
	return audit:insert({added_by='trigger',iold=iold,inew=inew,type=tp})
end
