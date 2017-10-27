exec dbms_stats.gather_schema_stats(ownname=> '"TPCE"' , cascade=> TRUE, estimate_percent=> null, degree=> DBMS_STATS.AUTO_DEGREE, no_invalidate=> DBMS_STATS.AUTO_INVALIDATE, granularity=> 'AUTO', method_opt=> 'FOR ALL COLUMNS SIZE AUTO', options=> 'GATHER');

