OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Taxrate.txt'
APPEND
INTO TABLE taxrate
FIELDS TERMINATED BY '|'
(
  tx_id,
  tx_name,
  tx_rate
)
