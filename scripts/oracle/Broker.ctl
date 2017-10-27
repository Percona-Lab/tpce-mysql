OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Broker.txt'
APPEND
INTO TABLE broker
FIELDS TERMINATED BY '|'
(
  b_id,
  b_st_id,
  b_name,
  b_num_trades,
  b_comm_total
)
