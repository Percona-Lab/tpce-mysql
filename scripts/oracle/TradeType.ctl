OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/TradeType.txt'
APPEND
INTO TABLE trade_type
FIELDS TERMINATED BY '|'
(
  tt_id,
  tt_name,
  tt_is_sell,
  tt_is_mrkt
)
