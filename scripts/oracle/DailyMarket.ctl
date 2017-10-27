OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/DailyMarket.txt'
APPEND
INTO TABLE daily_market
FIELDS TERMINATED BY '|'
(
  dm_date DATE "YYYY-MM-DD",
  dm_s_symb,
  dm_close,
  dm_high,
  dm_low,
  dm_vol
)
