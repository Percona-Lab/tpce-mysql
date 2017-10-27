OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/AccountPermission.txt'
APPEND
INTO TABLE account_permission
FIELDS TERMINATED BY '|'
(
  ap_ca_id,
  ap_acl,
  ap_tax_id,
  ap_l_name,
  ap_f_name
)
