DROP SEQUENCE IF EXISTS seq_trade_id;
CREATE SEQUENCE seq_trade_id INCREMENT 1;
SELECT SETVAL('seq_trade_id',(SELECT MAX(t_id) FROM trade));

