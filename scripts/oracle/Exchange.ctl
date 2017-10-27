OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Exchange.txt'
APPEND
INTO TABLE exchange
FIELDS TERMINATED BY '|'
(
  ex_id,
  ex_name,
  ex_num_symb,
  ex_open,
  ex_close,
  ex_desc,
  ex_ad_id
)
