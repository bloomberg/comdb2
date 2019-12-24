local function main()
	local consumer = db:consumer()
	local event = consumer:get()
	db:begin()
	db:exec("SELECT comdb2_test_log("
		.. event.type .. ", "
		.. tostring(event.old.i) .. ", "
		.. tostring(event.new.i) ..
	")")
	consumer:consume()
	db:commit()
end
