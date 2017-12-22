local agg = nil
local function step(a)
	if agg == nil then
		agg = {}
		agg.as = a
		agg.n = 1
	else
		agg.as = agg.as + a
		agg.n = agg.n + 1
	end
end

local function final()
	return agg.as / agg.n	
end
