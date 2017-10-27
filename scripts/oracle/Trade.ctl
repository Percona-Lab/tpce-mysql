OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Trade.txt'
APPEND
INTO TABLE trade
FIELDS TERMINATED BY '|'
(
  t_id,
  t_dts TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF3",
  t_st_id,
  t_tt_id,
  t_is_cash,
  t_s_symb,
  t_qty,
  t_bid_price,
  t_ca_id,
  t_exec_name,
  t_trade_price,
  t_chrg,
  t_comm,
  t_tax,
  t_lifo
)
