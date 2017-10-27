OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Sector.txt'
APPEND
INTO TABLE sector
FIELDS TERMINATED BY '|'
(
  sc_id,
  sc_name
)
