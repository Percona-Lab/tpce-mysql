OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/CompanyCompetitor.txt'
APPEND
INTO TABLE company_competitor
FIELDS TERMINATED BY '|'
(
  cp_co_id,
  cp_comp_co_id,
  cp_in_id
)
