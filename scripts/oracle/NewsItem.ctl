OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/NewsItem.txt'
APPEND
INTO TABLE news_item
FIELDS TERMINATED BY '|'
(
  ni_id,
  ni_headline,
  ni_summary,
  ni_item CHAR(100000),
  ni_dts TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF3",
  ni_source,
  ni_author
)
