#/bin/sh
USERID=tpce/tpce
NEXT_T_ID=`( sqlplus $USERID <<_EOF_
SET NUMWIDTH 50
SELECT MAX(t_id)+1 FROM trade;
_EOF_
) | grep [0-9] | grep -v [A-Z]`
sqlplus $USERID <<_EOF_
DROP SEQUENCE seq_trade_id;
CREATE SEQUENCE seq_trade_id START WITH $NEXT_T_ID INCREMENT BY 1 NOMAXVALUE NOCYCLE;
_EOF_

