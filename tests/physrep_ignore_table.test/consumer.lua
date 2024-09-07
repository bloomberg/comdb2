local function main(event)
    local consumer = db:consumer()
    while true do
        local event = consumer:get()
        local tp = event.type
        print("Event type: " .. tp)
        consumer:consume()
    end
end
    
