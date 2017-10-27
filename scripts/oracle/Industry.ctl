OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Industry.txt'
APPEND
INTO TABLE industry
FIELDS TERMINATED BY '|'
(
  in_id,
  in_name,
  in_sc_id
)
