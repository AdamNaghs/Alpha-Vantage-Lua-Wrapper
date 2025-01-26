local M = {log_file_path = "av_queries.log"}

local socket = require("socket")
local http = require("socket.http")
local ltn12 = require("ltn12")
local json = require("cjson")

--- Alpha Vantage docs for parameters
M.Func = {
    Stock =
    {
        -- Time Series
        TS = {
            Intraday = "TIME_SERIES_INTRADAY",
            Daily = "TIME_SERIES_DAILY",
            Daily_Adj = "TIME_SERIES_DAILY_ADJUSTED",
            Monthly = "TIME_SERIES_MONTHLY",
            Monthly_Adj = "TIME_SERIES_MONTHLY_ADJUSTED",
        },
        Quote = "GLOBAL_QUOTE",
        Realtime_Bulk_Quotes = "REALTIME_BULK_QUOTES",
    },
    Options = {
        Realtime = "REALTIME_OPTIONS",
        Historical = "HISTORICAL_OPTIONS",
    },
    News = {
        Sentiment = "NEWS_SENTIMENT",
        Top_Movers = "TOP_GAINERS_LOSERS",
        Insider = "INSDER_TRANSACTIONS",
        Analytics =
        {
            Fixed = "ANALYTICS_FIXED_WINDOW",
            Sliding = "ANALYTICS_SLIDING_WINDOW",
        }
    },
    Data =
    {
        Overview = "OVERVIEW",
        ETF_Profile = "ETF_PROFILE",
        -- Corperate Action
        CA = {
            Dividends = "DIVIDENDS",
            Splits = "SPLITS",
        },
        Income = "INCOME_STATEMENT",
        Balence_Sheet = "BALANCE_SHEET",
        Cash_Flow = "CASH_FLOW",
        Earnings = "EARNINGS",
        Listing_Status = "LISTING_STATUS",
        Earnings_Calendar = "EARNINGS_CALENDAR",
        IPO_Calendar = "IPO_CALENDAR"
    },
    Forex =
    {
        Exchange_Rate = "CURRENCY_EXCHANGE_RATE",
        Intraday = "FX_INTRADAY",
        Daily = "FX_DAILY",
        Weekly = "FX_WEEKLY",
        Monthly = "FX_MONTHLY",
    },
    Crypto =
    {
        Exchange_Rate = "CURRENCY_EXCHANGE_RATE",
        Intraday = "CRYPTO_INTRADAY",
        Daily = "CRYPTO_DAILY",
        Weekly = "CRYPTO_WEEKLY",
        Monthly = "CRYPTO_MONTHLY",
    },
    Commodities =
    {
        Crude_WTI = "WTI",
        Crude_Brent = "BRENT",
        Natural_Gas = "NATURAL_GAS",
        Copper = "COPPER",
        Aluminum = "ALUMINUM",
        Wheat = "WHEAT",
        Corn = "CORN",
        Cotton = "COTTON",
        Sugar = "SUGAR",
        Coffee = "COFFEE",
        All = "ALL_COMMODITIES",
    },
    Indicators =
    {
        Economic =
        {
            GDP = "REAL_GDP",
            GDP_Per_Capita = "REAL_GDP_PER_CAPITA",
            Treasury_Yield = "TREASURY_YIELD",
            Federal_Funds_Rate = "FEDERAL_FUNDS_RATE",
            CPI = "CPI",
            Inflation = "INFLATION",
            Retail_Sales = "RETAIL_SALES",
            Durables = "DURABLES",
            Unemployment = "UNEMPLOYMENT",
            Nonfarm_Payroll = "NONFARM_PAYROLL",
        },
        Technical = {
            -- Moving Averages
            SMA = "SMA",       -- Simple Moving Average
            EMA = "EMA",       -- Exponential Moving Average
            WMA = "WMA",       -- Weighted Moving Average
            DEMA = "DEMA",     -- Double Exponential Moving Average
            TEMA = "TEMA",     -- Triple Exponential Moving Average
            TRIMA = "TRIMA",   -- Triangular Moving Average
            KAMA = "KAMA",     -- Kaufman Adaptive Moving Average
            MAMA = "MAMA",     -- MESA Adaptive Moving Average
            T3 = "T3",         -- Triple Exponential Moving Average
            VWAP = "VWAP",     -- Volume Weighted Average Price
            -- Oscillators
            MACD = "MACD",     -- Moving Average Convergence/Divergence
            MACDEXT = "MACDEXT",
            STOCH = "STOCH",   -- Stochastic Oscillator
            STOCHF = "STOCHF", -- Stochastic Fast
            RSI = "RSI",       -- Relative Strength Index
            STOCHRSI = "STOCHRSI",
            WILLR = "WILLR",   -- Williams' %R
            ADX = "ADX",       -- Average Directional Movement Index
            ADXR = "ADXR",     -- ADX Rating
            APO = "APO",       -- Absolute Price Oscillator
            PPO = "PPO",       -- Percentage Price Oscillator
            MOM = "MOM",       -- Momentum
            BOP = "BOP",       -- Balance of Power
            CCI = "CCI",       -- Commodity Channel Index
            CMO = "CMO",       -- Chande Momentum Oscillator
            ROC = "ROC",       -- Rate of Change
            ROCR = "ROCR",     -- Rate of Change Ratio
            AROON = "AROON",
            AROONOSC = "AROONOSC",
            MFI = "MFI",       -- Money Flow Index
            TRIX = "TRIX",     -- Triple Exponential Average
            ULTOSC = "ULTOSC", -- Ultimate Oscillator
            DX = "DX",         -- Directional Movement Index
            -- Directional Indicators
            MINUS_DI = "MINUS_DI",
            PLUS_DI = "PLUS_DI",
            MINUS_DM = "MINUS_DM",
            PLUS_DM = "PLUS_DM",
            -- Other Indicators
            BBANDS = "BBANDS", -- Bollinger Bands
            MIDPOINT = "MIDPOINT",
            MIDPRICE = "MIDPRICE",
            SAR = "SAR",       -- Parabolic SAR
            TRANGE = "TRANGE", -- True Range
            ATR = "ATR",       -- Average True Range
            NATR = "NATR",     -- Normalized ATR
            AD = "AD",         -- Chaikin A/D Line
            ADOSC = "ADOSC",   -- Chaikin A/D Oscillator
            OBV = "OBV",       -- On Balance Volume
            -- Hilbert Transform
            HT_TRENDLINE = "HT_TRENDLINE",
            HT_SINE = "HT_SINE",
            HT_TRENDMODE = "HT_TRENDMODE",
            HT_DCPERIOD = "HT_DCPERIOD",
            HT_DCPHASE = "HT_DCPHASE",
            HT_PHASOR = "HT_PHASOR"
        }
    }
}

function M:log(str)
    local log_file = io.open(self.log_file_path, "a")
    if nil == log_file then
        print("Could not log: ", str)
        return
    end
    log_file:write(os.date("%x %X ", os.time()) .. str .. "\n")
    log_file:close()
end

---comment
---@param self any
---@param func any
---@param args any
---@return nil|table, string|nil
local function query(self, func, args)
    if nil == self.api_key then
        self.api_key = M.load_api_key()
        if nil == self.api_key then
            return nil, "Alpha Vantage Client missing api_key. Could not load from \"alpha_vantage_key.txt\"."
        end
    end
    local base_url = "https://www.alphavantage.co/query?function=" .. func
    for key, value in pairs(args or {}) do
        base_url = base_url .. "&" .. key .. "=" .. value
    end
    M:log(base_url) -- not logging apikey
    base_url = base_url .. "&apikey=" .. self.api_key
    local response_body = {}
    local response, code = http.request({
        url = base_url,
        method = "GET",
        sink = ltn12.sink.table(response_body),
        headers = { ["User-Agent"] = "Lua Alpha Vantage Client" }
    })
    if code ~= 200 then
        local error = string.format("HTTP request failed with code %d", code)
        M:log(error)
        return nil, error
    end
    local decoded = json.decode(table.concat(response_body))
    if decoded["Error Message"] then
        M:log(decoded["Error Message"])
        return nil, decoded["Error Message"]
    end
    if decoded["Note"] then -- Rate limit message
        M:log(decoded["Note"])
        return nil, decoded["Note"]
    end
    if decoded["Information"] then
        M:log(decoded["Information"])
        return nil, decoded["Information"]
    end
    if not decoded or next(decoded) == nil then
        M:log("Empty response")
        return nil, "Empty response"
    end
    return decoded, nil
end

---comment Expects file alpha_vantage_key.txt to exist and contain key.
---@return string
function M.load_api_key()
    local file = assert(io.open("alpha_vantage_key.txt", "r"))
    local key = file:read("*all"):gsub("^%s*(.-)%s*$", "%1")
    file:close()
    return key
end

function M.table_to_json(tbl)
    return json.encode(tbl)
end

M.uncapped_rate = 1000000

M.gettime = socket.gettime

M.sleep = socket.sleep

-- Set the rate_per_minute to 3600 to uncap
function M.new(api_key, rate_per_minute)
    local client = {
        api_key = api_key,
        rate_per_minute = rate_per_minute or 5,
        last_query_time = M.gettime(),
    }

    -- Wrap query function with rate limiting
    client.query = function(self, func, args)
        local time_since_last = M.gettime() - self.last_query_time
        local delay_needed = (60 / self.rate_per_minute) - time_since_last

        if self.rate_per_minute ~= M.uncapped_rate and delay_needed > 0 then
            M:log("Sleeping " .. tostring(delay_needed))
            M.sleep(delay_needed)
        end

        self.last_query_time = M.gettime()
        return query(self, func, args)
    end

    return client
end

return M
