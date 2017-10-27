// TradeUpdateDB.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

CTradeUpdateDB::CTradeUpdateDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CTradeUpdateDB::~CTradeUpdateDB()
{
}

void CTradeUpdateDB::DoTradeUpdateFrame1(const TTradeUpdateFrame1Input *pIn,
					 TTradeUpdateFrame1Output *pOut)
{
#ifdef DEBUG
    cout<<"CTradeUpdateDB::DoTradeUpdateFrame1"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;

    char old_ex_name[cEXEC_NAME_len+1];
    char new_ex_name[cEXEC_NAME_len+1];

    unsigned char is_cash; //SQL_C_BIT
    unsigned char is_market; //SQL_C_BIT

    char date_buf[15]; //pOut->trade_info[].settlement_cash_due_date
    char datetime_buf[30]; //pOut->trade_info[].cash_transaction_dts pOut->trade_info[].trade_history_dts[]
    char trade_history_status_id[cTH_ST_ID_len+1];

#ifdef ORACLE_ODBC
#ifdef USE_PREPARE
    char param_num_str[30];
#endif
#endif

    // Isolation level required by Clause 7.4.1.3
#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_ISO_L2];
    rc = SQLExecute(stmt);
#else
    stmt = m_Stmt;
#if (defined(ORACLE_ODBC)||defined(PGSQL_ODBC))
    //Oracle and PostgreSQL don't have "REPEATABLE READ" level
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", SQL_NTS);
#else
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", SQL_NTS);
#endif
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();


    pOut->num_found = 0;
    pOut->num_updated = 0;

    for (int i = 0; i < pIn->max_trades; i++)
    {
	if (pOut->num_updated < pIn->max_updates)
	{
	    /* SELECT t_exec_name
	       FROM trade
	       WHERE t_id = %ld */

#ifdef USE_PREPARE
	    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF1_1];
#ifdef ORACLE_ODBC
	    snprintf(param_num_str, 30, "%lld", pIn->trade_id[i]);
	    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->trade_id[i]), NULL);
#endif
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLExecute(stmt);
#else // !USE_PREPARE
	    stmt = m_Stmt;
	    ostringstream osTUF1_1;
	    osTUF1_1 << "SELECT t_exec_name FROM trade WHERE t_id = " <<
		pIn->trade_id[i];
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_1.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, old_ex_name, cEXEC_NAME_len+1, NULL);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLFetch(stmt);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);

	    if (rc != SQL_NO_DATA_FOUND) {
		pOut->num_found++;
	    } else {
		continue;
	    }

	    char *p_index;
	    if ((p_index = strstr(old_ex_name, " X ")) != 0)
	    {
		strncpy(new_ex_name, old_ex_name, p_index - old_ex_name);
		*(new_ex_name + (p_index - old_ex_name)) = ' ';
		strncpy(new_ex_name + (p_index - old_ex_name) + 1,
			p_index + 3, cEXEC_NAME_len+1 - ((p_index - old_ex_name) + 1));
	    }
	    else if ((p_index = index(old_ex_name,(int)' ')) != 0)
	    {
		strncpy(new_ex_name, old_ex_name, p_index - old_ex_name);
		strcpy(new_ex_name + (p_index - old_ex_name), " X ");
		strncpy(new_ex_name + (p_index - old_ex_name) + 3,
			p_index + 1, cEXEC_NAME_len+1 - ((p_index - old_ex_name) + 3));
	    }
	    else
	    {
		strncpy(new_ex_name, old_ex_name, cEXEC_NAME_len+1);
	    }


	    /* UPDATE trade
	       SET t_exec_name = '%s'
	       WHERE t_id = %ld */

#ifdef USE_PREPARE
	    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF1_2];
	    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(new_ex_name, cEXEC_NAME_len+1), 0,
			      new_ex_name, NULL);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
	    snprintf(param_num_str, 30, "%lld", pIn->trade_id[i]);
	    rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	    rc = SQLBindParam(stmt, 2, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->trade_id[i]), NULL);
#endif
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLExecute(stmt);
#else // !USE_PREPARE
	    stmt = m_Stmt;
	    ostringstream osTUF1_2;
	    osTUF1_2 << "UPDATE trade SET t_exec_name = '" <<
		new_ex_name << "' WHERE t_id = " <<
		pIn->trade_id[i];
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_2.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    
	    SQLLEN row_count;
	    SQLRowCount(stmt, &row_count);

	    SQLCloseCursor(stmt);

	    pOut->num_updated += row_count;
	}


	/* SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt, t_trade_price
	   FROM trade, trade_type
	   WHERE t_id = %ld
	     AND t_tt_id = tt_id */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF1_3];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pIn->trade_id[i]);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->trade_id[i]), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTUF1_3;
	osTUF1_3 << "SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt, t_trade_price FROM trade, trade_type WHERE t_id = " <<
            pIn->trade_id[i] << " AND t_tt_id = tt_id";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_3.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].bid_price), 0, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 2, SQL_C_CHAR, pOut->trade_info[i].exec_name, cEXEC_NAME_len+1, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 3, SQL_C_BIT, &is_cash, 0, NULL); //pOut->trade_info[i].is_cash
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 4, SQL_C_BIT, &is_market, 0, NULL); //pOut->trade_info[i].is_market
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 5, SQL_C_DOUBLE, &(pOut->trade_info[i].trade_price), 0, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        SQLCloseCursor(stmt);

        pOut->trade_info[i].is_cash = (is_cash != 0);
        pOut->trade_info[i].is_market = (is_market != 0);


	/* SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type
	   FROM settlement
	   WHERE se_t_id = %ld */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF1_4];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pIn->trade_id[i]);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->trade_id[i]), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTUF1_4;
#ifdef MYSQL_ODBC
	osTUF1_4 << "SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = " <<
#elif PGSQL_ODBC
	osTUF1_4 << "SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = " <<
#elif ORACLE_ODBC
	osTUF1_4 << "SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = " <<
#endif
            pIn->trade_id[i];
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_4.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].settlement_amount), 0, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 2, SQL_C_CHAR, date_buf, 15, NULL); //pOut->trade_info[].settlement_cash_due_date
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].settlement_cash_type, cSE_CASH_TYPE_len+1, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        SQLCloseCursor(stmt);

        //pOut->trade_info[i].settlement_cash_due_date = date_buf
        sscanf(date_buf,"%4hd-%2hu-%2hu",
               &(pOut->trade_info[i].settlement_cash_due_date.year),
               &(pOut->trade_info[i].settlement_cash_due_date.month),
               &(pOut->trade_info[i].settlement_cash_due_date.day));
        pOut->trade_info[i].settlement_cash_due_date.hour
            = pOut->trade_info[i].settlement_cash_due_date.minute
            = pOut->trade_info[i].settlement_cash_due_date.second
            = 0;
        pOut->trade_info[i].settlement_cash_due_date.fraction = 0;


	if(pOut->trade_info[i].is_cash)
        {
	    /* SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name
	       FROM cash_transaction
	       WHERE ct_t_id = %ld */

#ifdef USE_PREPARE
	    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF1_5];
#ifdef ORACLE_ODBC
	    snprintf(param_num_str, 30, "%lld", pIn->trade_id[i]);
	    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->trade_id[i]), NULL);
#endif
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLExecute(stmt);
#else // !USE_PREPARE
	    stmt = m_Stmt;
	    ostringstream osTUF1_5;
#ifdef MYSQL_ODBC
	    osTUF1_5 << "SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
#elif PGSQL_ODBC
	    osTUF1_5 << "SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
#elif ORACLE_ODBC
	    osTUF1_5 << "SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
#endif
                pIn->trade_id[i];
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_5.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].cash_transaction_amount), 0, NULL);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLBindCol(stmt, 2, SQL_C_CHAR, datetime_buf, 30, NULL); //pOut->trade_info[].cash_transaction_dts
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].cash_transaction_name, cCT_NAME_len+1, NULL);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLFetch(stmt);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
                ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            SQLCloseCursor(stmt);

            //pOut->trade_info[i].cash_transaction_dts = datetime_buf
            sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
                   &(pOut->trade_info[i].cash_transaction_dts.year),
                   &(pOut->trade_info[i].cash_transaction_dts.month),
                   &(pOut->trade_info[i].cash_transaction_dts.day),
                   &(pOut->trade_info[i].cash_transaction_dts.hour),
                   &(pOut->trade_info[i].cash_transaction_dts.minute),
                   &(pOut->trade_info[i].cash_transaction_dts.second),
                   &(pOut->trade_info[i].cash_transaction_dts.fraction));
	    pOut->trade_info[i].cash_transaction_dts.fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
	}


	/* SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id
	   FROM trade_history
	   WHERE th_t_id = %ld
	   ORDER BY th_dts
	   LIMIT 3 */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF1_6];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pIn->trade_id[i]);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->trade_id[i]), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTUF1_6;
#ifdef MYSQL_ODBC
	osTUF1_6 << "SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pIn->trade_id[i] << " ORDER BY th_dts LIMIT 3";
#elif PGSQL_ODBC
	osTUF1_6 << "SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.US'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pIn->trade_id[i] << " ORDER BY th_dts LIMIT 3";
#elif ORACLE_ODBC
	osTUF1_6 << "SELECT * FROM (SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pIn->trade_id[i] << " ORDER BY th_dts) WHERE ROWNUM <= 3";
#endif
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF1_6.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 1, SQL_C_CHAR, datetime_buf, 30, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 2, SQL_C_CHAR, trade_history_status_id, cTH_ST_ID_len+1, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

        int j = 0;
        while(rc != SQL_NO_DATA_FOUND && j < 3)
        {
            //pOut->trade_info[i].trade_history_dts[j] = datetime_buf
            sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
                   &(pOut->trade_info[i].trade_history_dts[j].year),
                   &(pOut->trade_info[i].trade_history_dts[j].month),
                   &(pOut->trade_info[i].trade_history_dts[j].day),
                   &(pOut->trade_info[i].trade_history_dts[j].hour),
                   &(pOut->trade_info[i].trade_history_dts[j].minute),
                   &(pOut->trade_info[i].trade_history_dts[j].second),
                   &(pOut->trade_info[i].trade_history_dts[j].fraction));
	    pOut->trade_info[i].trade_history_dts[j].fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
            strncpy(pOut->trade_info[i].trade_history_status_id[j],
                    trade_history_status_id, cTH_ST_ID_len+1);
            j++;

            rc = SQLFetch(stmt);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
                ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        }
        SQLCloseCursor(stmt);
    }

    CommitTxn();

    pOut->status = CBaseTxnErr::SUCCESS;

    //These are dummy to avoid optimize out local values...
#ifdef USE_PREPARE
    new_ex_name[cEXEC_NAME_len]=0;
#ifdef ORACLE_ODBC
    param_num_str[29]=0;
#endif
#endif
}

void CTradeUpdateDB::DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn,
					 TTradeUpdateFrame2Output *pOut)
{
#ifdef DEBUG
    cout<<"CTradeUpdateDB::DoTradeUpdateFrame2"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;

    char cash_type[cSE_CASH_TYPE_len+1];

    char start_trade_dts[30]; //pIn->start_trade_dts
    char end_trade_dts[30]; //pIn->end_trade_dts

    TTradeUpdateFrame2TradeInfo trade_info;
    unsigned char is_cash; //SQL_C_BIT

    char date_buf[15]; //pOut->trade_info[].settlement_cash_due_date
    char datetime_buf[30]; //pOut->trade_info[].cash_transaction_dts pOut->trade_info[].trade_history_dts[]

    char trade_history_status_id[cTH_ST_ID_len+1];

    int i;

#ifdef ORACLE_ODBC
#ifdef USE_PREPARE
    char param_num_str[30];
#endif
    char num_str[30];
#endif

    // Isolation level required by Clause 7.4.1.3
#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_ISO_L2];
    rc = SQLExecute(stmt);
#else
    stmt = m_Stmt;
#if (defined(ORACLE_ODBC)||defined(PGSQL_ODBC))
    //Oracle and PostgreSQL don't have "REPEATABLE READ" level
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", SQL_NTS);
#else
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", SQL_NTS);
#endif
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();

    /* SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price
       FROM trade
       WHERE t_ca_id = %ld
         AND t_dts >= '%s'
         AND t_dts <= '%s'
       ORDER BY t_dts ASC
       LIMIT pIn->max_trades */

    snprintf(start_trade_dts, 30, "%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%06u",
             pIn->start_trade_dts.year,
             pIn->start_trade_dts.month,
             pIn->start_trade_dts.day,
             pIn->start_trade_dts.hour,
             pIn->start_trade_dts.minute,
             pIn->start_trade_dts.second,
             pIn->start_trade_dts.fraction / 1000); //nano -> micro
    snprintf(end_trade_dts, 30, "%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%06u",
             pIn->end_trade_dts.year,
             pIn->end_trade_dts.month,
             pIn->end_trade_dts.day,
             pIn->end_trade_dts.hour,
             pIn->end_trade_dts.minute,
             pIn->end_trade_dts.second,
             pIn->end_trade_dts.fraction / 1000); //nano -> micro

#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF2_1];
#ifdef ORACLE_ODBC
    snprintf(param_num_str, 30, "%lld", pIn->acct_id);
    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->acct_id), NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(start_trade_dts, 30), 0, start_trade_dts, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_CHAR, strnlen(end_trade_dts, 30), 0, end_trade_dts, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 4, SQL_C_LONG, SQL_INTEGER, 0, 0, (void*) &(pIn->max_trades), NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLExecute(stmt);
#else // !USE_PREPARE
    stmt = m_Stmt;
    ostringstream osTUF2_1;
#ifdef ORACLE_ODBC
    osTUF2_1 << "SELECT * FROM (SELECT t_bid_price, t_exec_name, t_is_cash, TO_CHAR(t_id), t_trade_price FROM trade WHERE t_ca_id = " <<
        pIn->acct_id << " AND t_dts >= '" <<
        start_trade_dts << "' AND t_dts <= '" <<
        end_trade_dts << "' ORDER BY t_dts ASC) WHERE ROWNUM <= " <<
        pIn->max_trades;
#else
    osTUF2_1 << "SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price FROM trade WHERE t_ca_id = " <<
        pIn->acct_id << " AND t_dts >= '" <<
        start_trade_dts << "' AND t_dts <= '" <<
        end_trade_dts << "' ORDER BY t_dts ASC LIMIT " <<
        pIn->max_trades;
#endif
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_1.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(trade_info.bid_price), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, trade_info.exec_name, cEXEC_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_BIT, &is_cash, 0, NULL); //pOut->trade_info[i].is_cash
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt, 4, SQL_C_CHAR, num_str, 30, NULL);
#else
    rc = SQLBindCol(stmt, 4, SQL_C_SBIGINT, &(trade_info.trade_id), 0, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 5, SQL_C_DOUBLE, &(trade_info.trade_price), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < pIn->max_trades)
    {
#ifdef ORACLE_ODBC
	trade_info.trade_id = atoll(num_str);
#endif

        pOut->trade_info[i].bid_price = trade_info.bid_price;
        strncpy(pOut->trade_info[i].exec_name, trade_info.exec_name, cEXEC_NAME_len+1);
        pOut->trade_info[i].is_cash = (is_cash != 0);
        pOut->trade_info[i].trade_id = trade_info.trade_id;
        pOut->trade_info[i].trade_price = trade_info.trade_price;
        i++;

        rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

    pOut->num_found = i;
    pOut->num_updated = 0;


    for (i = 0; i < pOut->num_found; i++)
    {
	if (pOut->num_updated < pIn->max_updates)
	{
	    /* SELECT se_cash_type
	       FROM settlement
	       WHERE se_t_id = %s */

#ifdef USE_PREPARE
	    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF2_2];
#ifdef ORACLE_ODBC
	    snprintf(param_num_str, 30, "%lld", pOut->trade_info[i].trade_id);
	    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &(pOut->trade_info[i].trade_id), NULL);
#endif
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLExecute(stmt);
#else // !USE_PREPARE
	    stmt = m_Stmt;
	    ostringstream osTUF2_2;
	    osTUF2_2 << "SELECT se_cash_type FROM settlement WHERE se_t_id = " <<
		pOut->trade_info[i].trade_id;
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_2.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, cash_type, cSE_CASH_TYPE_len+1, NULL);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLFetch(stmt);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    SQLCloseCursor(stmt);


	    if(pOut->trade_info[i].is_cash)
	    {
		if (strcmp(cash_type, "Cash Account") == 0)
		    strcpy(cash_type, "Cash");
		else
		    strcpy(cash_type, "Cash Account");
	    }
	    else
	    {
		if (strcmp(cash_type, "Margin Account") == 0)
		    strcpy(cash_type, "Margin");
		else
		    strcpy(cash_type, "Margin Account");
	    }

	    /* UPDATE settlement
	       SET se_cash_type = '%s'
	       WHERE se_t_id = %s */

#ifdef USE_PREPARE
	    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF2_3];
	    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(cash_type, cSE_CASH_TYPE_len+1), 0,
			      &cash_type, NULL);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
	    snprintf(param_num_str, 30, "%lld", pOut->trade_info[i].trade_id);
	    rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	    rc = SQLBindParam(stmt, 2, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &(pOut->trade_info[i].trade_id), NULL);
#endif
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLExecute(stmt);
#else // !USE_PREPARE
	    stmt = m_Stmt;
	    ostringstream osTUF2_3;
	    osTUF2_3 << "UPDATE settlement SET se_cash_type = '" <<
		cash_type << "' WHERE se_t_id = " <<
		pOut->trade_info[i].trade_id;
	    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_3.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	    SQLLEN row_count;
	    SQLRowCount(stmt, &row_count);

	    SQLCloseCursor(stmt);

	    pOut->num_updated += row_count;
	}

	/* SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type
	   FROM settlement
	   WHERE se_t_id = %s */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF2_4];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pOut->trade_info[i].trade_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &(pOut->trade_info[i].trade_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTUF2_4;
#ifdef MYSQL_ODBC
	osTUF2_4 << "SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = " <<
#elif PGSQL_ODBC
	osTUF2_4 << "SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = " <<
#elif ORACLE_ODBC
	osTUF2_4 << "SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = " <<
#endif
            pOut->trade_info[i].trade_id;
        rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_4.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].settlement_amount), 0, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 2, SQL_C_CHAR, date_buf, 15, NULL); //pOut->trade_info[].settlement_cash_due_date
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].settlement_cash_type, cSE_CASH_TYPE_len+1, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        SQLCloseCursor(stmt);

        //pOut->trade_info[i].settlement_cash_due_date = date_buf
        sscanf(date_buf,"%4hd-%2hu-%2hu",
               &(pOut->trade_info[i].settlement_cash_due_date.year),
               &(pOut->trade_info[i].settlement_cash_due_date.month),
               &(pOut->trade_info[i].settlement_cash_due_date.day));
        pOut->trade_info[i].settlement_cash_due_date.hour
            = pOut->trade_info[i].settlement_cash_due_date.minute
            = pOut->trade_info[i].settlement_cash_due_date.second
            = 0;
        pOut->trade_info[i].settlement_cash_due_date.fraction = 0;


	if(pOut->trade_info[i].is_cash)
	{
	    /* SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name
	       FROM cash_transaction
	       WHERE ct_t_id = %s */

#ifdef USE_PREPARE
	    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF2_5];
#ifdef ORACLE_ODBC
	    snprintf(param_num_str, 30, "%lld", pOut->trade_info[i].trade_id);
	    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &(pOut->trade_info[i].trade_id), NULL);
#endif
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLExecute(stmt);
#else // !USE_PREPARE
	    stmt = m_Stmt;
	    ostringstream osTUF2_5;
#ifdef MYSQL_ODBC
	    osTUF2_5 << "SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
#elif PGSQL_ODBC
	    osTUF2_5 << "SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
#elif ORACLE_ODBC
	    osTUF2_5 << "SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
#endif
                pOut->trade_info[i].trade_id;
            rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_5.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].cash_transaction_amount), 0, NULL);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLBindCol(stmt, 2, SQL_C_CHAR, datetime_buf, 30, NULL); //pOut->trade_info[].cash_transaction_dts
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].cash_transaction_name, cCT_NAME_len+1, NULL);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLFetch(stmt);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
                ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            SQLCloseCursor(stmt);

	    //pOut->trade_info[i].cash_transaction_dts = datetime_buf
            sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
                   &(pOut->trade_info[i].cash_transaction_dts.year),
                   &(pOut->trade_info[i].cash_transaction_dts.month),
                   &(pOut->trade_info[i].cash_transaction_dts.day),
                   &(pOut->trade_info[i].cash_transaction_dts.hour),
                   &(pOut->trade_info[i].cash_transaction_dts.minute),
                   &(pOut->trade_info[i].cash_transaction_dts.second),
                   &(pOut->trade_info[i].cash_transaction_dts.fraction));
	    pOut->trade_info[i].cash_transaction_dts.fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
	}

	/* SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id
	   FROM trade_history
	   WHERE th_t_id = %s
	   ORDER BY th_dts
	   LIMIT 3 */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF2_6];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pOut->trade_info[i].trade_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &(pOut->trade_info[i].trade_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTUF2_6;
#ifdef MYSQL_ODBC
	osTUF2_6 << "SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pOut->trade_info[i].trade_id << " ORDER BY th_dts LIMIT 3";
#elif PGSQL_ODBC
	osTUF2_6 << "SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.US'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pOut->trade_info[i].trade_id << " ORDER BY th_dts LIMIT 3";
#elif ORACLE_ODBC
	osTUF2_6 << "SELECT * FROM (SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pOut->trade_info[i].trade_id << " ORDER BY th_dts) WHERE ROWNUM <= 3";
#endif
        rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF2_6.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 1, SQL_C_CHAR, datetime_buf, 30, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 2, SQL_C_CHAR, trade_history_status_id, cTH_ST_ID_len+1, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

        int j = 0;
        while(rc != SQL_NO_DATA_FOUND && j < 3)
        {
            //pOut->trade_info[i].trade_history_dts[j] = datetime_buf
            sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
                   &(pOut->trade_info[i].trade_history_dts[j].year),
                   &(pOut->trade_info[i].trade_history_dts[j].month),
                   &(pOut->trade_info[i].trade_history_dts[j].day),
                   &(pOut->trade_info[i].trade_history_dts[j].hour),
                   &(pOut->trade_info[i].trade_history_dts[j].minute),
                   &(pOut->trade_info[i].trade_history_dts[j].second),
                   &(pOut->trade_info[i].trade_history_dts[j].fraction));
	    pOut->trade_info[i].trade_history_dts[j].fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
            strncpy(pOut->trade_info[i].trade_history_status_id[j],
                    trade_history_status_id, cTH_ST_ID_len+1);
            j++;

            rc = SQLFetch(stmt);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
                ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        }
        SQLCloseCursor(stmt);
    }

    CommitTxn();

    pOut->status = CBaseTxnErr::SUCCESS;

    //These are dummy to avoid optimize out local values...
#ifdef USE_PREPARE
    start_trade_dts[29]=0;
    end_trade_dts[29]=0;
    cash_type[cSE_CASH_TYPE_len]=0;
#ifdef ORACLE_ODBC
    param_num_str[29]=0;
#endif
#endif
}

void CTradeUpdateDB::DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn,
					 TTradeUpdateFrame3Output *pOut)
{
#ifdef DEBUG
    cout<<"CTradeUpdateDB::DoTradeUpdateFrame3"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;

    char ct_name[cCT_NAME_len+1];

    char start_trade_dts[30]; //pIn->start_trade_dts
    char end_trade_dts[30]; //pIn->end_trade_dts

    TTradeUpdateFrame3TradeInfo trade_info;
    unsigned char is_cash; //SQL_C_BIT

    char date_buf[15]; //pOut->trade_info[].settlement_cash_due_date
    char datetime_buf[30]; //pOut->trade_info[].trade_dts pOut->trade_info[].cash_transaction_dts pOut->trade_info[].trade_history_dts[]

    char trade_history_status_id[cTH_ST_ID_len+1];

    int i;

#ifdef ORACLE_ODBC
#ifdef USE_PREPARE
    char param_num_str[30];
#endif
    char num_str[30];
    char num_str2[30];
#endif

    // Isolation level required by Clause 7.4.1.3
#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_ISO_L2];
    rc = SQLExecute(stmt);
#else
    stmt = m_Stmt;
#if (defined(ORACLE_ODBC)||defined(PGSQL_ODBC))
    //Oracle and PostgreSQL don't have "REPEATABLE READ" level
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", SQL_NTS);
#else
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", SQL_NTS);
#endif
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();

    /* SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty,
              s_name, DATE_FORMAT(t_dts, '%Y-%m-%d %H:%i:%s.%f'), t_id, t_tt_id, tt_name
       FROM trade, trade_type, security
       WHERE t_s_symb = '%s'
         AND t_dts >= '%s'
         AND t_dts <= '%s'
         AND tt_id = t_tt_id
         AND s_symb = t_s_symb
       ORDER BY t_dts ASC
       LIMIT pIn->max_trades */

    snprintf(start_trade_dts, 30, "%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%06u",
             pIn->start_trade_dts.year,
             pIn->start_trade_dts.month,
             pIn->start_trade_dts.day,
             pIn->start_trade_dts.hour,
             pIn->start_trade_dts.minute,
             pIn->start_trade_dts.second,
             pIn->start_trade_dts.fraction / 1000); //nano -> micro
    snprintf(end_trade_dts, 30, "%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%06u",
             pIn->end_trade_dts.year,
             pIn->end_trade_dts.month,
             pIn->end_trade_dts.day,
             pIn->end_trade_dts.hour,
             pIn->end_trade_dts.minute,
             pIn->end_trade_dts.second,
             pIn->end_trade_dts.fraction / 1000); //nano -> micro

#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF3_1];
    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->symbol, cSYMBOL_len+1), 0,
                      (void*) (pIn->symbol), NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(start_trade_dts, 30), 0, start_trade_dts, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_CHAR, strnlen(end_trade_dts, 30), 0, end_trade_dts, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 4, SQL_C_LONG, SQL_INTEGER, 0, 0, (void*) &(pIn->max_trades), NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLExecute(stmt);
#else // !USE_PREPARE
    stmt = m_Stmt;
    ostringstream osTUF3_1;
#ifdef MYSQL_ODBC
    osTUF3_1 << "SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty, s_name, DATE_FORMAT(t_dts, '%Y-%m-%d %H:%i:%s.%f'), t_id, t_tt_id, tt_name FROM trade, trade_type FORCE INDEX(PRIMARY), security WHERE t_s_symb = '" <<
	pIn->symbol << "' AND t_dts >= '" <<
	start_trade_dts << "' AND t_dts <= '" <<
	end_trade_dts << "' AND tt_id = t_tt_id AND s_symb = t_s_symb ORDER BY t_dts ASC LIMIT " <<
	pIn->max_trades;
#elif PGSQL_ODBC
    osTUF3_1 << "SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty, s_name, TO_CHAR(t_dts, 'YYYY-MM-DD HH24:MI:SS.US'), t_id, t_tt_id, tt_name FROM trade, trade_type, security WHERE t_s_symb = '" <<
	pIn->symbol << "' AND t_dts >= '" <<
	start_trade_dts << "' AND t_dts <= '" <<
	end_trade_dts << "' AND tt_id = t_tt_id AND s_symb = t_s_symb ORDER BY t_dts ASC LIMIT " <<
	pIn->max_trades;
#elif ORACLE_ODBC
    osTUF3_1 << "SELECT * FROM (SELECT TO_CHAR(t_ca_id), t_exec_name, t_is_cash, t_trade_price, t_qty, s_name, TO_CHAR(t_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), TO_CHAR(t_id), t_tt_id, tt_name FROM trade, trade_type, security WHERE t_s_symb = '" <<
	pIn->symbol << "' AND t_dts >= '" <<
	start_trade_dts << "' AND t_dts <= '" <<
	end_trade_dts << "' AND tt_id = t_tt_id AND s_symb = t_s_symb ORDER BY t_dts ASC) WHERE ROWNUM <= " <<
	pIn->max_trades;
#endif
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_1.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, num_str, 30, NULL);
#else
    rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &(trade_info.acct_id), 0, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, trade_info.exec_name, cEXEC_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_BIT, &is_cash, 0, NULL); //pOut->trade_info[i].is_cash
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_DOUBLE, &(trade_info.price), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 5, SQL_C_LONG, &(trade_info.quantity), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 6, SQL_C_CHAR, trade_info.s_name, cS_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 7, SQL_C_CHAR, datetime_buf, 30, NULL); //pOut->trade_info[].trade_dts
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt, 8, SQL_C_CHAR, num_str2, 30, NULL);
#else
    rc = SQLBindCol(stmt, 8, SQL_C_SBIGINT, &(trade_info.trade_id), 0, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 9, SQL_C_CHAR, trade_info.trade_type, cTT_ID_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 10, SQL_C_CHAR, trade_info.type_name, cTT_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < pIn->max_trades)
    {
#ifdef ORACLE_ODBC
	trade_info.acct_id = atoll(num_str);
	trade_info.trade_id = atoll(num_str2);
#endif

        pOut->trade_info[i].acct_id = trade_info.acct_id;
        strncpy(pOut->trade_info[i].exec_name, trade_info.exec_name, cEXEC_NAME_len+1);
        pOut->trade_info[i].is_cash = (is_cash != 0);
        pOut->trade_info[i].price = trade_info.price;
        pOut->trade_info[i].quantity = trade_info.quantity;
	strncpy(pOut->trade_info[i].s_name, trade_info.s_name, cS_NAME_len+1);

	//pOut->trade_info[i].trade_dts = datetime_buf
        sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
               &(pOut->trade_info[i].trade_dts.year),
               &(pOut->trade_info[i].trade_dts.month),
               &(pOut->trade_info[i].trade_dts.day),
               &(pOut->trade_info[i].trade_dts.hour),
               &(pOut->trade_info[i].trade_dts.minute),
               &(pOut->trade_info[i].trade_dts.second),
               &(pOut->trade_info[i].trade_dts.fraction));
	pOut->trade_info[i].trade_dts.fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.

        pOut->trade_info[i].trade_id = trade_info.trade_id;
        strncpy(pOut->trade_info[i].trade_type, trade_info.trade_type, cTT_ID_len+1);
	strncpy(pOut->trade_info[i].type_name, trade_info.type_name, cTT_NAME_len+1);
	i++;

        rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

    pOut->num_found = i;
    pOut->num_updated = 0;


    for (i = 0; i < pOut->num_found; i++)
    {
	/* SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type
	   FROM settlement
	   WHERE se_t_id = %s" */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF3_2];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pOut->trade_info[i].trade_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &(pOut->trade_info[i].trade_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTUF3_2;
#ifdef MYSQL_ODBC
	osTUF3_2 << "SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = " <<
#elif PGSQL_ODBC
	osTUF3_2 << "SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = " <<
#elif ORACLE_ODBC
	osTUF3_2 << "SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = " <<
#endif
            pOut->trade_info[i].trade_id;
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_2.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].settlement_amount), 0, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 2, SQL_C_CHAR, date_buf, 15, NULL); //pOut->trade_info[].settlement_cash_due_date
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].settlement_cash_type, cSE_CASH_TYPE_len+1, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        SQLCloseCursor(stmt);

        //pOut->trade_info[i].settlement_cash_due_date = date_buf
        sscanf(date_buf,"%4hd-%2hu-%2hu",
               &(pOut->trade_info[i].settlement_cash_due_date.year),
               &(pOut->trade_info[i].settlement_cash_due_date.month),
               &(pOut->trade_info[i].settlement_cash_due_date.day));
        pOut->trade_info[i].settlement_cash_due_date.hour
            = pOut->trade_info[i].settlement_cash_due_date.minute
            = pOut->trade_info[i].settlement_cash_due_date.second
            = 0;
        pOut->trade_info[i].settlement_cash_due_date.fraction = 0;


	if(pOut->trade_info[i].is_cash)
	{
	    if (pOut->num_updated < pIn->max_updates)
	    {
		/* SELECT ct_name
		   FROM cash_transaction
		   WHERE ct_t_id = %s */

#ifdef USE_PREPARE
		stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF3_3];
#ifdef ORACLE_ODBC
		snprintf(param_num_str, 30, "%lld", pOut->trade_info[i].trade_id);
		rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
		rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &(pOut->trade_info[i].trade_id), NULL);
#endif
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLExecute(stmt);
#else // !USE_PREPARE
		stmt = m_Stmt;
		ostringstream osTUF3_3;
		osTUF3_3 << "SELECT ct_name FROM cash_transaction WHERE ct_t_id = " <<
		    pOut->trade_info[i].trade_id;
		rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_3.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindCol(stmt, 1, SQL_C_CHAR, ct_name, cCT_NAME_len+1, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLFetch(stmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
		    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		SQLCloseCursor(stmt);


		/* UPDATE cash_transaction
		   SET ct_name = '%s'
		   WHERE ct_t_id = %s */

#ifdef USE_PREPARE
		if(strstr(ct_name, " shares of "))
		    snprintf(ct_name, cCT_NAME_len+1, "%s %d Shares of %s",
			     pOut->trade_info[i].type_name, pOut->trade_info[i].quantity, pOut->trade_info[i].s_name);
		else
		    snprintf(ct_name, cCT_NAME_len+1, "%s %d shares of %s",
			     pOut->trade_info[i].type_name, pOut->trade_info[i].quantity, pOut->trade_info[i].s_name);

		stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF3_4];
		rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(ct_name, cCT_NAME_len+1), 0,
				  ct_name, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
		snprintf(param_num_str, 30, "%lld", pOut->trade_info[i].trade_id);
		rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
		rc = SQLBindParam(stmt, 2, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &(pOut->trade_info[i].trade_id), NULL);
#endif
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLExecute(stmt);
#else // !USE_PREPARE
		char s_name[cS_NAME_len+1];
		expand_quote(s_name, pOut->trade_info[i].s_name, cS_NAME_len+1);

		stmt = m_Stmt;
		ostringstream osTUF3_4;
		osTUF3_4 << "UPDATE cash_transaction SET ct_name = '" <<
		    pOut->trade_info[i].type_name << " " << pOut->trade_info[i].quantity;
		if(strstr(ct_name, " shares of "))
		    osTUF3_4 << " Shares of ";
		else
		    osTUF3_4 << " shares of ";
		osTUF3_4 << s_name << "' WHERE ct_t_id = " <<
		    pOut->trade_info[i].trade_id;

		rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_4.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

		SQLLEN row_count;
		SQLRowCount(stmt, &row_count);

		SQLCloseCursor(stmt);

		pOut->num_updated += row_count;
	    }

	    /* SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name
	       FROM cash_transaction
	       WHERE ct_t_id = %s */

#ifdef USE_PREPARE
	    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF3_5];
#ifdef ORACLE_ODBC
	    snprintf(param_num_str, 30, "%lld", pOut->trade_info[i].trade_id);
	    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &(pOut->trade_info[i].trade_id), NULL);
#endif
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	    rc = SQLExecute(stmt);
#else // !USE_PREPARE
	    stmt = m_Stmt;
	    ostringstream osTUF3_5;
#ifdef MYSQL_ODBC
	    osTUF3_5 << "SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
#elif PGSQL_ODBC
	    osTUF3_5 << "SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
#elif ORACLE_ODBC
	    osTUF3_5 << "SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ct_name FROM cash_transaction WHERE ct_t_id = " <<
#endif
                pOut->trade_info[i].trade_id;
            rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_5.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->trade_info[i].cash_transaction_amount), 0, NULL);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLBindCol(stmt, 2, SQL_C_CHAR, datetime_buf, 30, NULL); //pOut->trade_info[].cash_transaction_dts
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->trade_info[i].cash_transaction_name, cCT_NAME_len+1, NULL);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            rc = SQLFetch(stmt);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
                ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
            SQLCloseCursor(stmt);

	    //pOut->trade_info[i].cash_transaction_dts = datetime_buf
            sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
                   &(pOut->trade_info[i].cash_transaction_dts.year),
                   &(pOut->trade_info[i].cash_transaction_dts.month),
                   &(pOut->trade_info[i].cash_transaction_dts.day),
                   &(pOut->trade_info[i].cash_transaction_dts.hour),
                   &(pOut->trade_info[i].cash_transaction_dts.minute),
                   &(pOut->trade_info[i].cash_transaction_dts.second),
                   &(pOut->trade_info[i].cash_transaction_dts.fraction));
	    pOut->trade_info[i].cash_transaction_dts.fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
	}

	/* SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id
	   FROM trade_history
	   WHERE th_t_id = %s
	   ORDER BY th_dts ASC
	   LIMIT 3 */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_TUF3_6];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pOut->trade_info[i].trade_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &(pOut->trade_info[i].trade_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTUF3_6;
#ifdef MYSQL_ODBC
	osTUF3_6 << "SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pOut->trade_info[i].trade_id << " ORDER BY th_dts ASC LIMIT 3";
#elif PGSQL_ODBC
	osTUF3_6 << "SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.US'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pOut->trade_info[i].trade_id << " ORDER BY th_dts ASC LIMIT 3";
#elif ORACLE_ODBC
	osTUF3_6 << "SELECT * FROM (SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), th_st_id FROM trade_history WHERE th_t_id = " <<
            pOut->trade_info[i].trade_id << " ORDER BY th_dts ASC) WHERE ROWNUM <= 3";
#endif
        rc = SQLExecDirect(stmt, (SQLCHAR*)(osTUF3_6.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 1, SQL_C_CHAR, datetime_buf, 30, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLBindCol(stmt, 2, SQL_C_CHAR, trade_history_status_id, cTH_ST_ID_len+1, NULL);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

        int j = 0;
        while(rc != SQL_NO_DATA_FOUND && j < 3)
        {
            //pOut->trade_info[i].trade_history_dts[j] = datetime_buf
            sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
                   &(pOut->trade_info[i].trade_history_dts[j].year),
                   &(pOut->trade_info[i].trade_history_dts[j].month),
                   &(pOut->trade_info[i].trade_history_dts[j].day),
                   &(pOut->trade_info[i].trade_history_dts[j].hour),
                   &(pOut->trade_info[i].trade_history_dts[j].minute),
                   &(pOut->trade_info[i].trade_history_dts[j].second),
                   &(pOut->trade_info[i].trade_history_dts[j].fraction));
	    pOut->trade_info[i].trade_history_dts[j].fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
            strncpy(pOut->trade_info[i].trade_history_status_id[j],
                    trade_history_status_id, cTH_ST_ID_len+1);
            j++;

            rc = SQLFetch(stmt);
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
                ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
        }
        SQLCloseCursor(stmt);
    }

    CommitTxn();

    pOut->status = CBaseTxnErr::SUCCESS;

    //These are dummy to avoid optimize out local values...
#ifdef USE_PREPARE
    start_trade_dts[29]=0;
    end_trade_dts[29]=0;
    ct_name[cCT_NAME_len]=0;
#ifdef ORACLE_ODBC
    param_num_str[29]=0;
#endif
#endif
}
