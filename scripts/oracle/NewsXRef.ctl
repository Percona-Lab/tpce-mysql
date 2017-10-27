OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/NewsXRef.txt'
APPEND
INTO TABLE news_xref
FIELDS TERMINATED BY '|'
(
  nx_ni_id,
  nx_co_id
)

