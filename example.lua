local market = require("market")

local data = market.frame.create(2024, "SPY", { "JEPI", "JEPQ", "USFR", "TFLO" })

market.file.clear(market.av.log_file_path)

market.frame.print(data)

market.file.write_csv(data, "tmp.csv")

market.file.write_csv(market.frame.create(2024, "SPY", market.file.lines_from("all_tickers.txt")), "market.csv")
