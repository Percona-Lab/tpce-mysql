OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/ZipCode.txt'
APPEND
INTO TABLE zip_code
FIELDS TERMINATED BY '|'
(
  zc_code,
  zc_town,
  zc_div
)
