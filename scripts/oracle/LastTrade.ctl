OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/LastTrade.txt'
APPEND
INTO TABLE last_trade
FIELDS TERMINATED BY '|'
(
  lt_s_symb,
  lt_dts TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF3",
  lt_price,
  lt_open_price,
  lt_vol
)
