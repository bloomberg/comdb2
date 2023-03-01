local function comdb2_trigger_main()
    local sp = db:spname()
    db:ctrace('trigger:'..sp..' send register')
    local c = db:trigger({register_timeout = 1000})
    if c == nil then
        db:ctrace('trigger:'..sp..' register failed')
        return
    end
    db:ctrace('trigger:'..sp..' assigned; now running')
    local e = c:get()
    while e do
        db:trigger_version_check()
        db:trigger_begin()
        local rc = main(e)
        if rc ~= 0 then
            db:ctrace('trigger:'..sp..' main rc:'..rc..' err:'..db:error())
            db:trigger_rollback()
            break
        end
        rc = c:consume()
        if rc ~= 0 then
            db:ctrace('trigger:'..sp..' consume rc:'..rc..' err:'..db:error())
            db:trigger_rollback()
            break
        end
        rc = db:trigger_commit()
        if rc ~= 0 then
            db:ctrace('trigger:'..sp..' commit rc:'..rc..' err:'..db:error())
            break
        end
        e = c:get()
    end
    if e == nil then
        db:ctrace('trigger'..sp..' nil event')
    end
end
