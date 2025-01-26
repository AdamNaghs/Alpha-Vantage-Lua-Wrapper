local market = require("market")

local data = market.frame.create(2024,"SPY",{"JEPI","JEPQ","USFR","TFLO"})

market.frame.print(data)

market.file.write_csv(data,"tmp.csv")
