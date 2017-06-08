local function main(work)
	for _, sql in ipairs(db:json_to_table(work)) do
		local rc = 0
		if sql == "begin" then
			rc = db:begin()
		elseif sql == "commit" then
			rc = db:commit()
		elseif sql == "rollback" then
			db:rollback()
		else
			db:exec(sql)
		end
		if rc ~= 0 then
			db:rollback()
			return rc, db:sqlerror()
		end
	end
end
