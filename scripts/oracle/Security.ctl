OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Security.txt'
APPEND
INTO TABLE security
FIELDS TERMINATED BY '|'
(
  s_symb,
  s_issue,
  s_st_id,
  s_name,
  s_ex_id,
  s_co_id,
  s_num_out,
  s_start_date DATE "YYYY-MM-DD",
  s_exch_date DATE "YYYY-MM-DD",
  s_pe,
  s_52wk_high,
  s_52wk_high_date DATE "YYYY-MM-DD",
  s_52wk_low,
  s_52wk_low_date DATE "YYYY-MM-DD",
  s_dividend,
  s_yield
)
