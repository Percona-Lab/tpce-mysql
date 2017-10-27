OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Address.txt'
APPEND
INTO TABLE address
FIELDS TERMINATED BY '|'
(
  ad_id,
  ad_line1,
  ad_line2,
  ad_zc_code,
  ad_ctry
)
