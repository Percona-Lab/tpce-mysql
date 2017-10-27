OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Charge.txt'
APPEND
INTO TABLE charge
FIELDS TERMINATED BY '|'
(
  ch_tt_id,
  ch_c_tier,
  ch_chrg
)
