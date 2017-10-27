OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/TradeHistory.txt'
APPEND
INTO TABLE trade_history
FIELDS TERMINATED BY '|'
(
  th_t_id,
  th_dts TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF3",
  th_st_id
)
