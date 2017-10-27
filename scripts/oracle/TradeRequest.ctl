OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/TradeRequest.txt'
APPEND
INTO TABLE trade_request
FIELDS TERMINATED BY '|'
(
  tr_t_id,
  tr_tt_id,
  tr_s_symb,
  tr_qty,
  tr_bid_price,
  tr_b_id
)
