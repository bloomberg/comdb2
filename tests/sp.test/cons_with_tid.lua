local function define_emit_columns()
    local schema = { {"text", "jsonEmitObj"} }
	db:num_columns(#schema)
	for k,v in ipairs(schema) do
		db:column_type(v[1], k)
		db:column_name(v[2], k)
	end
end
local function main()
	define_emit_columns()
	-- get handle to consumer associated with stored procedure
	local consumer = db:consumer({with_tid=true})
    -- local consumer = db:consumer()
	while true do
		local change = consumer:poll(5) -- blocking call
		if change.new ~= nil then
			change.new = db:table_to_json(change.new)
		end
		if change.old ~= nil then
			change.old = db:table_to_json(change.old)
		end
        if change.id then
            change.id = "642" -- some constant value
        end    
        if change.tid then
            change.tid = 246  -- some constant number
        end    
        local emitObj = db:table_to_json(change)
		consumer:emit(emitObj) -- blocking call
		consumer:consume()
	end
end
