It's a GoLang framework for managing financial market 
data - ticks/candles...

Package is still under hard development. More documentation and functionality will be later.

You can create your own connector to datasource or exchange using
provider interface. Now it supports only ActiveTick as datasource.

By default all data stored in .json files. If you want to store it in
SQL/NoSQL database you should make your own implementation of storage interface

