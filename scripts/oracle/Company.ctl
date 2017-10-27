OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Company.txt'
APPEND
INTO TABLE company
FIELDS TERMINATED BY '|'
(
  co_id,
  co_st_id,
  co_name,
  co_in_id,
  co_sp_rate,
  co_ceo,
  co_ad_id,
  co_desc,
  co_open_date DATE "YYYY-MM-DD"
)
