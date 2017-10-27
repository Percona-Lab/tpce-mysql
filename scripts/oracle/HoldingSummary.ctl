OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/HoldingSummary.txt'
APPEND
INTO TABLE holding_summary
FIELDS TERMINATED BY '|'
(
  hs_ca_id,
  hs_s_symb,
  hs_qty
)
