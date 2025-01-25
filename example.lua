--- Luajit
local av = require("alpha_vantage")
local client = av.new(av.load_api_key())

local function len(tbl)
    local count = 0
    for _ in pairs(tbl) do count = count + 1 end
    return count
end

local function parse_date(date_str)
    local year, month = date_str:match("^(%d%d%d%d)-(%d%d)")
    if not year or not month then
        return nil, "Invalid date format"
    end
    return tonumber(year), tonumber(month)
end
---Helper takes actual data not ticker strings
local function calculate_beta(asset_data, benchmark_data)
    local function get_returns(data)
        local dates = {}
        local price_map = {}

        for date, value in pairs(data["Monthly Adjusted Time Series"]) do
            table.insert(dates, date)
            price_map[date] = tonumber(value["5. adjusted close"])
        end

        table.sort(dates, function(a, b) return a > b end)

        local returns = {}
        for i = 2, #dates do
            local curr_price = price_map[dates[i]]
            local prev_price = price_map[dates[i - 1]]
            local return_val = (curr_price - prev_price) / prev_price
            table.insert(returns, return_val)
        end

        return returns
    end

    local asset_returns = get_returns(asset_data)
    local benchmark_returns = get_returns(benchmark_data)

    local min_length = math.min(#asset_returns, #benchmark_returns)
    while #asset_returns > min_length do table.remove(asset_returns) end
    while #benchmark_returns > min_length do table.remove(benchmark_returns) end

    local mean_asset = 0
    local mean_benchmark = 0
    for i = 1, #asset_returns do
        mean_asset = mean_asset + asset_returns[i]
        mean_benchmark = mean_benchmark + benchmark_returns[i]
    end
    mean_asset = mean_asset / #asset_returns
    mean_benchmark = mean_benchmark / #benchmark_returns

    local covariance = 0
    local variance_benchmark = 0
    for i = 1, #asset_returns do
        covariance = covariance + (asset_returns[i] - mean_asset) * (benchmark_returns[i] - mean_benchmark)
        variance_benchmark = variance_benchmark + (benchmark_returns[i] - mean_benchmark) ^ 2
    end

    covariance = covariance / (#asset_returns - 1)
    variance_benchmark = variance_benchmark / (#asset_returns - 1)

    return covariance / variance_benchmark
end

local function compute_stats(start_year, data)
    local list = data["Monthly Adjusted Time Series"]
    if not list then
        return nil, "Missing time series data"
    end

    local dividend_sum = 0
    local prices = {}
    local dividend_months = {}

    for date, value_dict in pairs(list) do
        local year, month = parse_date(date)
        if not year then goto continue end
        if year < start_year then goto continue end

        local div = tonumber(value_dict["7. dividend amount"])
        local price = tonumber(value_dict["4. close"])

        if div and price and div > 0 and price > 0 then
            dividend_sum = dividend_sum + div
            table.insert(prices, price)
            dividend_months[year .. "-" .. month] = true
        end
        ::continue::
    end

    if #prices == 0 then
        return nil, "No valid data found"
    end

    table.sort(prices)
    local price_sum = 0
    for _, i in ipairs(prices) do
        price_sum = price_sum + i
    end
    local average_price = price_sum / #prices
    local median_price = prices[math.ceil(#prices / 2)]
    local annual_dividend = dividend_sum * (12 / len(dividend_months))
    local annual_yield = (annual_dividend / median_price) * 100

    return average_price, median_price, annual_dividend, annual_yield
end

-- Takes data of benchmark so we dont need to requery it and the ticker so that we can do the initial query for that asset
local function compute_stats_against(benchmarch_data, start_year, ticker)
    local data, error = client:query(av.Func.Stock.TS.Monthly_Adj, { symbol = ticker, outputsize = "compact" })
    if data == nil then
        print(string.format("Error fetching %s: %s", ticker, error))
        return nil
    end
    local average_price, median_price, annual_dividend, annual_yield = compute_stats(start_year, data)
    local beta = calculate_beta(data, benchmarch_data)
    return average_price, median_price, annual_dividend, annual_yield, beta
end

local function create_frame(start_year, benchmark_ticker, tickers)
    local data = {
        headers = { "Ticker", "Avg Price", "Med Price", "Ann Div", "Yield", "Beta" },
        rows = {}
    }

    local benchmarch = client:query(av.Func.Stock.TS.Monthly_Adj, { symbol = benchmark_ticker, outputsize = "compact" })
    if nil == benchmarch then return nil, "Error fetching benchmark" end

    for _, ticker in ipairs(tickers) do
        local avg_price, med_price, ann_div, yield, beta = compute_stats_against(benchmarch, start_year, ticker)
        if nil ~= avg_price then
            table.insert(data.rows, {
                ticker    = ticker,
                avg_price = avg_price,
                med_price = med_price,
                ann_div   = ann_div,
                yield     = yield,
                beta      = beta
            })
        end
    end

    return data
end

local function print_frame(data)
    -- Then use unpack_func instead of either unpack or table.unpack
    print(string.format("%-6s\t%-12s\t%-12s\t%-12s\t%-12s\t%-6s",
        ---@diagnostic disable-next-line: deprecated
        unpack(data.headers)))
    print(string.rep("-", 80))

    -- Print rows
    for _, row in ipairs(data.rows) do
        print(string.format("%-6s\t$%-11.2f\t$%-11.2f\t$%-11.2f\t%-11.2f%%\t%.2f",
            row.ticker, row.avg_price, row.med_price, row.ann_div, row.yield, row.beta))
    end
end

local function write_csv(data, filename)
    local file = io.open(filename, "w")
    if not file then return nil, "Cannot open file" end

    -- Write headers
    file:write(table.concat(data.headers, ",") .. "\n")

    -- Write rows
    for _, row in ipairs(data.rows) do
        file:write(string.format("%s,%.2f,%.2f,%.2f,%.2f,%.2f\n",
            row.ticker, row.avg_price, row.med_price, row.ann_div, row.yield, row.beta))
    end

    file:close()
    return true
end

-- local table_data = create_frame(2024, "SPY",
--     { "SPY", "PFRL", "JEPI", "JEPQ", "USFR", "TFLO", "DIVO", "JQUA", "SCHD", "PFF" })

-- print_frame(table_data)

-- write_csv(table_data, "etf_analysis.csv")

print(av.table_to_json(client:query(av.Func.News.Top)))