
lua_1 = """
    local k = 1/math.sqrt(0.05)
    local val = tonumber(ARGV[1])
    local old_vals = redis.call('get',KEYS[1])
    local new_vals = {}
    if (old_vals) then
        old_vals = cjson.decode(old_vals)

        new_vals["count_1"] = old_vals['count_1'] + 1
        local delta = val - old_vals["mean_1"]
        new_vals["mean_1"] = old_vals["mean_1"] + delta / new_vals["count_1"]
        new_vals["M2_1"] = old_vals["M2_1"] + delta * (val - new_vals["mean_1"])
        new_vals["variance_1"] = new_vals["M2_1"] / new_vals["count_1"]
        local std = math.sqrt(new_vals["variance_1"])
        new_vals["ODV1L"] = new_vals["mean_1"] - k * std
        new_vals["ODV1U"] = new_vals["mean_1"] + k * std

        if (val <=  new_vals["ODV1U"] and val >=  new_vals["ODV1L"]) then
            new_vals["count_2"] = old_vals['count_2'] + 1
            delta = val - old_vals["mean_2"]
            new_vals["mean_2"] = old_vals["mean_2"] + delta / new_vals["count_2"]
            new_vals["M2_2"] = old_vals["M2_2"] + delta * (val - new_vals["mean_2"])
            new_vals["variance_2"] = new_vals["M2_2"] / new_vals["count_2"]
            std = math.sqrt(new_vals["variance_2"])
            new_vals["ODV2L"] = new_vals["mean_2"] - k * std
            new_vals["ODV2U"] = new_vals["mean_2"] + k * std
        else
            new_vals["count_2"] = old_vals['count_2']
            new_vals["mean_2"] = old_vals["mean_2"]
            new_vals["M2_2"] = old_vals["M2_2"]
            new_vals["variance_2"] = old_vals["variance_2"]
            new_vals["ODV2L"] = old_vals["ODV2L"]
            new_vals["ODV2U"] = old_vals["ODV2U"]
        end
    else
        new_vals["count_1"] = 1
        new_vals["mean_1"] = val
        new_vals["M2_1"] = 0
        new_vals["variance_1"] = 0
        new_vals["ODV1L"] = val
        new_vals["ODV1U"] = val

        new_vals["count_2"] = 1
         new_vals["mean_2"] = val
        new_vals["M2_2"] = 0
        new_vals["variance_2"] = 0
        new_vals["ODV2L"] = val
        new_vals["ODV2U"] = val
    end
    redis.call('set', KEYS[1], cjson.encode(new_vals))
"""

lua_2 = """
    local val = tonumber(ARGV[1])
    local dt = tostring(ARGV[2])
    local month = tostring(ARGV[3])
    local hour = tostring(ARGV[4])
    local old_vals = redis.call('get',KEYS[1])
    local new_vals = {}
    if (old_vals) then
        old_vals = cjson.decode(old_vals)
        new_vals = old_vals
        if(old_vals["count_" .. month]) then
            new_vals["count_" .. month] = old_vals["count_" .. month] + 1
            new_vals["sum_" .. month] = old_vals["sum_" .. month] + val
        else
            new_vals["count_" .. month] = 1
            new_vals["sum_" .. month] = val
        end
        if(old_vals["count_" .. dt]) then
            new_vals["count_" .. dt] = old_vals["count_" .. dt] + 1
            new_vals["sum_" .. dt] = old_vals["sum_" .. dt] + val
        else
            new_vals["count_" .. dt] = 1
            new_vals["sum_" .. dt] = val
        end
        if (old_vals["count_" .. dt .. '_' .. hour]) then
            new_vals["count_" .. dt .. '_' .. hour] = old_vals["count_" .. dt .. '_' .. hour] + 1
            new_vals["sum_" .. dt .. '_' .. hour] = old_vals["sum_" .. dt .. '_' .. hour] + val
        else
            new_vals["count_" .. dt .. '_' .. hour] = 1
            new_vals["sum_" .. dt .. '_' .. hour] = val
        end
    else
        new_vals["count_" .. dt .. '_' .. hour] = 1
        new_vals["sum_" .. dt .. '_' .. hour] = val
        new_vals["count_" .. dt] = 1
        new_vals["sum_" .. dt] = val
        new_vals["count_" .. month] = 1
        new_vals["sum_" .. month] = val
    end
    redis.call('set', KEYS[1], cjson.encode(new_vals))
"""
