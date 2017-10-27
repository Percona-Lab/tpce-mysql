OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/WatchItem.txt'
APPEND
INTO TABLE watch_item
FIELDS TERMINATED BY '|'
(
  wi_wl_id,
  wi_s_symb
)
