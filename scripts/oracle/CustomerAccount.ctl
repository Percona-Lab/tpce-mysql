OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/CustomerAccount.txt'
APPEND
INTO TABLE customer_account
FIELDS TERMINATED BY '|'
(
  ca_id,
  ca_b_id,
  ca_c_id,
  ca_name,
  ca_tax_st,
  ca_bal
)
