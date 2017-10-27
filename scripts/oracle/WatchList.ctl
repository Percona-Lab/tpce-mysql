OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/WatchList.txt'
APPEND
INTO TABLE watch_list
FIELDS TERMINATED BY '|'
(
  wl_id,
  wl_c_id
)
