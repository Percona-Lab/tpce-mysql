OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/CommissionRate.txt'
APPEND
INTO TABLE commission_rate
FIELDS TERMINATED BY '|'
(
  cr_c_tier,
  cr_tt_id,
  cr_ex_id,
  cr_from_qty,
  cr_to_qty,
  cr_rate
)
