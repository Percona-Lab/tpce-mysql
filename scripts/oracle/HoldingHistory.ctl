OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/HoldingHistory.txt'
APPEND
INTO TABLE holding_history
FIELDS TERMINATED BY '|'
(
  hh_h_t_id,
  hh_t_id,
  hh_before_qty,
  hh_after_qty
)
