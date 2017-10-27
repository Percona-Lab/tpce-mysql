OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Holding.txt'
APPEND
INTO TABLE holding
FIELDS TERMINATED BY '|'
(
  h_t_id,
  h_ca_id,
  h_s_symb,
  h_dts TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF3",
  h_price,
  h_qty
)
