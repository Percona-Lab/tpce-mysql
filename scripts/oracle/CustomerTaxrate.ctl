OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/CustomerTaxrate.txt'
APPEND
INTO TABLE customer_taxrate
FIELDS TERMINATED BY '|'
(
  cx_tx_id,
  cx_c_id
)
