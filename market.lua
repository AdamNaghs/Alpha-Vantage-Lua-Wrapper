local M = { frame = {}, file = {} }
--- Luajit
M.av = require("alpha_vantage")
-- could be smart to decrease the rate from your actual hard cap to ensure all calls are made
M.client = M.av.new(M.av.load_api_key(), 75)


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

local function calculate_returns(data)
    local dates = {}
    local prices = {}
    for date, value in pairs(data["Monthly Adjusted Time Series"]) do
        table.insert(dates, date)
        prices[date] = tonumber(value["5. adjusted close"])
    end
    table.sort(dates)

    -- Calculate monthly returns
    local returns = {}
    for i = 2, #dates do
        local monthly_return = (prices[dates[i]] - prices[dates[i - 1]]) / prices[dates[i - 1]]
        table.insert(returns, monthly_return)
    end

    -- Average monthly return
    local sum = 0
    for _, r in ipairs(returns) do
        sum = sum + r
    end
    return sum / #returns
end

local function CAPM(market_return, risk_free_rate, beta)
    -- Convert annual risk-free to monthly
    local monthly_rf = (1 + risk_free_rate) ^ (1 / 12) - 1
    return 12 * (monthly_rf + beta * (market_return - monthly_rf))
end

-- does not compute CAPM because it requred a beta which required a benchmark
local function compute_stats(start_year, data)
    local function normalize_date(date)
        local year, month, day = date:match("^(%d%d%d%d)-(%d%d)-(%d%d)")
        return year and month and day and string.format("%s-%s-%s", year, month, day)
    end

    local list = data["Monthly Adjusted Time Series"]
    if not list then return nil, "Missing time series data" end

    local dates = {}
    local valid_prices = {}
    local latest_price = nil
    local dividends = {}

    -- First pass: collect all dates and validate data
    for date in pairs(list) do
        local normalized_date = normalize_date(date)
        if normalized_date then
            table.insert(dates, normalized_date)
        end
    end
    table.sort(dates, function(a, b) return a > b end) -- Sort descending

    -- Second pass: collect prices and dividends
    for _, date in ipairs(dates) do
        local value_dict = list[date]
        local price = tonumber(value_dict["5. adjusted close"])
        local div = tonumber(value_dict["7. dividend amount"])

        if price and price > 0 then
            latest_price = latest_price or price -- Set latest price if not set
            table.insert(valid_prices, price)
        end

        if div and div > 0 then
            dividends[date] = div
        end
    end

    if #valid_prices < 3 then return nil, "Insufficient valid price data" end
    table.sort(valid_prices)

    local median_price = valid_prices[math.floor(#valid_prices / 2)]
    local trim_count = math.floor(#valid_prices * 0.1)
    local sum = 0
    for i = trim_count + 1, #valid_prices - trim_count do
        sum = sum + valid_prices[i]
    end
    local average_price = sum / (math.max(1, #valid_prices - 2 * trim_count))

    -- Calculate annual dividend
    local annual_dividend = 0
    if next(dividends) then
        local latest_year, latest_month = parse_date(dates[1])
        for date, div in pairs(dividends) do
            local year, month = parse_date(date)
            if year and month then
                local months_diff = (latest_year - year) * 12 + (latest_month - month)
                if months_diff >= 0 and months_diff < 12 then
                    annual_dividend = annual_dividend + div
                end
            end
        end
    end
    local annual_yield = latest_price > 0 and (annual_dividend / latest_price) * 100 or 0
    return latest_price, average_price, valid_prices[#valid_prices], valid_prices[1],
        median_price, annual_dividend, annual_yield
end

-- Does not compute CAPM because the benchmark_data is not easy to iterate over
-- Takes data of benchmark so we dont need to requery it and the ticker so that we can do the initial query for that asset
local function compute_stats_against(benchmarch_data, start_year, ticker)
    local data, error = M.client:query(M.av.Func.Stock.TS.Monthly_Adj, { symbol = ticker, outputsize = "compact" })
    if data == nil then
        print(string.format("Error fetching %s: %s", ticker, error))
        return nil
    end
    local latest_price, average_price, high_price, low_price, median_price, annual_dividend, annual_yield = compute_stats(
        start_year,
        data)
    local beta = calculate_beta(data, benchmarch_data)
    return latest_price, average_price, high_price, low_price, median_price, annual_dividend, annual_yield, beta
end

local function format_time(seconds)
    local hours = math.floor(seconds / 3600)
    local mins = math.floor((seconds % 3600) / 60)
    local secs = math.floor(seconds % 60)
    return string.format("%02d:%02d:%02d", hours, mins, secs)
end

---comment
---@param start_year number
---@param benchmark_ticker string
---@param tickers table
---@return table|nil, nil|string
function M.frame.create(start_year, benchmark_ticker, tickers)
    local start = M.av.gettime()
    local data = {
        headers = { "Ticker", "Latest Price ($)", "Avg Price ($)", "Med Price ($)", "High Price ($)", "Low Price ($)", "Ann Div ($)", "Yield (%)", "Beta", "CAPM (%)" },
        rows = {}
    }

    local benchmarch, error = M.client:query(M.av.Func.Stock.TS.Monthly_Adj,
        { symbol = benchmark_ticker, outputsize = "compact" })
    if nil == benchmarch then return nil, error end

    local market_return = calculate_returns(benchmarch)

    for idx, ticker in ipairs(tickers) do
        local time = M.av.gettime()
        local elapsed = time - start
        local avg_time_per_ticker = elapsed / idx -- Average time per ticker processed
        local remaining_tickers = #tickers - idx  -- Number of tickers left
        local remaining_time = avg_time_per_ticker * remaining_tickers

        io.write(string.format("Time Elapsed %s, Remaining Time: %s, ticker: %-6s, %d/%d\r",
            format_time(elapsed), format_time(remaining_time), ticker, idx, #tickers))
        io.flush()
        local stats = { compute_stats_against(benchmarch, start_year, ticker) }
        if stats[1] then -- if first value exists
            local latest_price, average_price, price_high, price_low, median_price, annual_dividend, annual_yield, beta =
            ---@diagnostic disable-next-line: deprecated
                unpack(stats)
            local capm = CAPM(market_return / 100, 0.0567, beta) * 100
            table.insert(data.rows, {
                ticker       = ticker,
                latest_price = latest_price,
                avg_price    = average_price,
                med_price    = median_price,
                high_price   = price_high,
                low_price    = price_low,
                ann_div      = annual_dividend,
                yield        = annual_yield,
                beta         = beta,
                capm         = capm
            })
        else
            if nil ~= stats[2] then --
                error = string.format("Error computing stats for %s: %s", ticker, stats[2])
                M.av:log(error)
                print(string.format("%s%s\r", error, string.rep(" ", 20)))
            end
        end
    end
    io.write("\n")
    function data:get_row(ticker)
        for _, row in ipairs(self.rows) do
            if row.ticker == ticker then
                return row
            end
        end
        return nil
    end

    return data
end

function M.frame.print(data)
    print(string.format("%-6s\t%-14s\t%-14s\t%-14s\t%-14s\t%-14s\t%-14s\t%-8s\t%-8s\t%-8s",
        ---@diagnostic disable-next-line: deprecated
        unpack(data.headers)))
    print(string.rep("-", 152))

    for _, row in ipairs(data.rows) do
        print(string.format(
            "%-6s\t$%-13.2f\t\t$%-13.2f\t$%-13.2f\t$%-13.2f\t$%-13.2f\t$%-13.2f\t%.2f%%\t\t%.2f\t\t%.2f%%",
            row.ticker, row.latest_price, row.avg_price, row.med_price, row.high_price, row.low_price, row.ann_div,
            row.yield, row.beta, row.capm))
    end
end

function M.file.write_csv(data, filename)
    local file = io.open(filename, "w")
    if not file then return nil, "Cannot open file" end

    -- Write headers
    file:write(table.concat(data.headers, ",") .. "\n")

    -- Write rows
    for _, row in ipairs(data.rows) do
        file:write(string.format("%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n",
            row.ticker, row.latest_price, row.avg_price, row.med_price, row.high_price, row.low_price, row.ann_div,
            row.yield, row.beta,
            row.capm))
    end

    file:close()
    return true
end

function M.file.lines_from(filepath)
    local lines = {}
    for line in io.lines(filepath) do
        lines[#lines + 1] = line
    end
    return lines
end

function M.file.clear(filepath)
    local file = io.open(filepath,"w+")
    if nil == file then
        print(string.format("File %s not found.",filepath))
        return
    end
    file:close()
end

return M
