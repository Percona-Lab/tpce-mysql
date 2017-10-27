OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Customer.txt'
APPEND
INTO TABLE customer
FIELDS TERMINATED BY '|'
(
  c_id,
  c_tax_id,
  c_st_id,
  c_l_name,
  c_f_name,
  c_m_name,
  c_gndr,
  c_tier,
  c_dob DATE "YYYY-MM-DD",
  c_ad_id,
  c_ctry_1,
  c_area_1,
  c_local_1,
  c_ext_1,
  c_ctry_2,
  c_area_2,
  c_local_2,
  c_ext_2,
  c_ctry_3,
  c_area_3,
  c_local_3,
  c_ext_3,
  c_email_1,
  c_email_2
)
