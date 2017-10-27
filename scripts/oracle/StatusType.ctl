OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/StatusType.txt'
APPEND
INTO TABLE status_type
FIELDS TERMINATED BY '|'
(
  st_id,
  st_name
)
