OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Settlement.txt'
APPEND
INTO TABLE settlement
FIELDS TERMINATED BY '|'
(
  se_t_id,
  se_cash_type,
  se_cash_due_date DATE "YYYY-MM-DD",
  se_amt
)
