OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/CashTransaction.txt'
APPEND
INTO TABLE cash_transaction
FIELDS TERMINATED BY '|'
(
  ct_t_id,
  ct_dts TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF3",
  ct_amt,
  ct_name
)
