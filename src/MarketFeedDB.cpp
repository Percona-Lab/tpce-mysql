// MarketFeedDB.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

CMarketFeedDB::CMarketFeedDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CMarketFeedDB::~CMarketFeedDB()
{
}

void CMarketFeedDB::DoMarketFeedFrame1(
    const TMarketFeedFrame1Input *pIn, TMarketFeedFrame1Output *pOut,
    CSendToMarketInterface *pSendToMarket)
{
#ifdef DEBUG
    cout<<"CMarketFeedDB::DoMarketFeedFrame1"<<endl;
#endif
    SQLHSTMT stmt;
    SQLHSTMT stmt2;

    SQLRETURN rc;

    int	   rows_updated, rows_sent;
    char   now_dts[TIMESTAMP_LEN+1];
    TTradeRequest TradeRequest;
    vector<TTradeRequest> TradeRequestBuffer;
    double req_price_quote;
    TTrade req_trade_id; /*INT64*/
    INT32  req_trade_qty;
    char   req_trade_type[cTT_ID_len+1];

#ifdef ORACLE_ODBC
#ifdef USE_PREPARE
    char param_num_str[30];
#endif
    char num_str[30];
#endif

    gettimestamp(now_dts, STRFTIME_FORMAT, TIMESTAMP_LEN);

    rows_updated = 0;

    for (int i = 0; i < max_feed_len; i++)
    {
	for (int j = 0; j < MAX_RETRY; j++)
	{
#ifdef PRINT_DEADLOCK
	    if (j)
		cout << "  Retry (MF):" << j << endl;
#endif

	    try
	    {
		// Isolation level required by Clause 7.4.1.3
#ifdef USE_PREPARE
		stmt = m_pDBConnection->m_pPrepared[MEESUT_STMT_ISO_L2];
		rc = SQLExecute(stmt);
#else
		stmt = m_Stmt;
#if (defined(ORACLE_ODBC)||defined(PGSQL_ODBC))
		//Oracle and PostgreSQL don't have "REPEATABLE READ" level
		rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", SQL_NTS);
#else
		rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", SQL_NTS);
#endif //ORACLE_ODBC PGSQL_ODBC
#endif
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		SQLCloseCursor(stmt);

		BeginTxn();

		rows_sent = 0;

		/*	UPDATE  LAST_TRADE
			SET     LT_PRICE = price_quote[i],
			        LT_VOL = LT_VOL + trade_qty[i],
			        LT_DTS = now_dts
			WHERE   LT_S_SYMB = symbol[i]; */

#ifdef USE_PREPARE
		stmt = m_pDBConnection->m_pPrepared[MEESUT_STMT_MFF1_1];
		rc = SQLBindParam(stmt, 1, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, (void*) &(pIn->Entries[i].price_quote), NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindParam(stmt, 2, SQL_C_LONG, SQL_INTEGER, 0, 0, (void*) &(pIn->Entries[i].trade_qty), NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_CHAR, strnlen(now_dts, TIMESTAMP_LEN+1), 0, now_dts, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLBindParam(stmt, 4, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->Entries[i].symbol, cSYMBOL_len+1), 0,
				  (void*)(pIn->Entries[i].symbol), NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		rc = SQLExecute(stmt);
#else // !USE_PREPARE
		stmt = m_Stmt;
		ostringstream osMFF1_1;
		osMFF1_1 << "UPDATE last_trade SET lt_price = " << pIn->Entries[i].price_quote <<
		    ", lt_vol = lt_vol + " << pIn->Entries[i].trade_qty <<
		    ", lt_dts = '" << now_dts <<
		    "' WHERE lt_s_symb = '" << pIn->Entries[i].symbol << "'";
		rc = SQLExecDirect(stmt, (SQLCHAR*)(osMFF1_1.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

		SQLLEN row_count;
		SQLRowCount(stmt, &row_count); /* row_count should be 1 */

		SQLCloseCursor(stmt);

		//rows_updated += row_count; /* It should be after commit transaction? */

		/*	OPEN request_list FOR
			SELECT  TR_T_ID,
			        TR_BID_PRICE,
			        TR_TT_ID,
			        TR_QTY
			FROM    TRADE_REQUEST
			WHERE   TR_S_SYMB = symbol[i] and
			        ((TR_TT_ID = type_stop_loss and TR_BID_PRICE >= price_quote[i]) or
			        (TR_TT_ID = type_limit_sell and TR_BID_PRICE <= price_quote[i]) or
			        (TR_TT_ID = type_limit_buy and TR_BID_PRICE >= price_quote[i])); */

#ifdef USE_PREPARE
		stmt2 = m_pDBConnection->m_pPrepared[MEESUT_STMT_MFF1_2];
		rc = SQLBindParam(stmt2, 1, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->Entries[i].symbol, cSYMBOL_len+1), 0,
				  (void*)(pIn->Entries[i].symbol), NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
		rc = SQLBindParam(stmt2, 2, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->StatusAndTradeType.type_stop_loss, cTT_ID_len+1), 0,
				  (void*)(pIn->StatusAndTradeType.type_stop_loss), NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
		rc = SQLBindParam(stmt2, 3, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, (void*) &(pIn->Entries[i].price_quote), NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
		rc = SQLBindParam(stmt2, 4, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->StatusAndTradeType.type_limit_sell, cTT_ID_len+1), 0,
				  (void*)(pIn->StatusAndTradeType.type_limit_sell), NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
		rc = SQLBindParam(stmt2, 5, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, (void*) &(pIn->Entries[i].price_quote), NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
		rc = SQLBindParam(stmt2, 6, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->StatusAndTradeType.type_limit_buy, cTT_ID_len+1), 0,
				  (void*)(pIn->StatusAndTradeType.type_limit_buy), NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
		rc = SQLBindParam(stmt2, 7, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, (void*) &(pIn->Entries[i].price_quote), NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
		rc = SQLExecute(stmt2);
#else // !USE_PREPARE
		stmt2 = m_Stmt2;
		ostringstream osMFF1_2;
#ifdef ORACLE_ODBC
		osMFF1_2 << "SELECT TO_CHAR(tr_t_id), tr_bid_price, tr_tt_id, tr_qty " <<
#else
		osMFF1_2 << "SELECT tr_t_id, tr_bid_price, tr_tt_id, tr_qty " <<
#endif
		    "FROM trade_request WHERE tr_s_symb = '" << pIn->Entries[i].symbol <<
		    "' AND ((tr_tt_id = '" << pIn->StatusAndTradeType.type_stop_loss <<
		    "' AND tr_bid_price >= " << pIn->Entries[i].price_quote <<
		    ") OR (tr_tt_id = '" << pIn->StatusAndTradeType.type_limit_sell <<
		    "' AND tr_bid_price <= " << pIn->Entries[i].price_quote <<
		    ") OR (tr_tt_id = '" << pIn->StatusAndTradeType.type_limit_buy <<
		    "' AND tr_bid_price >= " << pIn->Entries[i].price_quote <<
		    "))";
		rc = SQLExecDirect(stmt2, (SQLCHAR*)(osMFF1_2.str().c_str()), SQL_NTS); /* using m_Stmt2 for fetch loop */
#endif // USE_PREPARE

		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);


		/*	FETCH   request_list
			INTO    req_trade_id,
			        req_price_quote,
			        req_trade_type,
			        req_trade_qty; */

#ifdef ORACLE_ODBC
		rc = SQLBindCol(stmt2, 1, SQL_C_CHAR, num_str, 30, NULL);
#else
		rc = SQLBindCol(stmt2, 1, SQL_C_SBIGINT, &req_trade_id, 0, NULL);
#endif
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
		rc = SQLBindCol(stmt2, 2, SQL_C_DOUBLE, &req_price_quote, 0, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
		rc = SQLBindCol(stmt2, 3, SQL_C_CHAR, req_trade_type, cTT_ID_len+1, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
		rc = SQLBindCol(stmt2, 4, SQL_C_LONG, &req_trade_qty, 0, NULL);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);

		rc = SQLFetch(stmt2);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);


		while (rc != SQL_NO_DATA_FOUND)
		{
#ifdef ORACLE_ODBC
		    req_trade_id = atoll(num_str);
#endif

		    try
		    {

		    /*	UPDATE  TRADE
			SET     T_DTS = now_dts,
			        T_ST_ID = status_submitted
			WHERE   T_ID = trade_id; */

#ifdef USE_PREPARE
		    stmt = m_pDBConnection->m_pPrepared[MEESUT_STMT_MFF1_3];
		    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(now_dts, TIMESTAMP_LEN+1), 0, now_dts, NULL);
		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->StatusAndTradeType.status_submitted, cST_ID_len+1), 0,
				      (void*)(pIn->StatusAndTradeType.status_submitted), NULL);
		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
		    snprintf(param_num_str, 30, "%lld", req_trade_id);
		    rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
		    rc = SQLBindParam(stmt, 3, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &req_trade_id, NULL);
#endif
		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    rc = SQLExecute(stmt);
#else // !USE_PREPARE
		    stmt = m_Stmt;
		    ostringstream osMFF1_3;
		    osMFF1_3 << "UPDATE trade SET t_dts = '" << now_dts <<
			"', t_st_id = '" << pIn->StatusAndTradeType.status_submitted <<
			"' WHERE t_id = " << req_trade_id;
		    rc = SQLExecDirect(stmt, (SQLCHAR*)(osMFF1_3.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    SQLCloseCursor(stmt);


		    /*	DELETE  FROM TRADE_REQUEST
			WHERE   TR_T_ID = trade_id; */

#ifdef USE_PREPARE
		    stmt = m_pDBConnection->m_pPrepared[MEESUT_STMT_MFF1_4];
#ifdef ORACLE_ODBC
		    snprintf(param_num_str, 30, "%lld", req_trade_id);
		    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
		    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &req_trade_id, NULL);
#endif
		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    rc = SQLExecute(stmt);
#else // !USE_PREPARE
		    stmt = m_Stmt;
		    ostringstream osMFF1_4;
		    osMFF1_4 << "DELETE FROM trade_request WHERE tr_t_id = " << req_trade_id;

		    rc = SQLExecDirect(stmt, (SQLCHAR*)(osMFF1_4.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    SQLCloseCursor(stmt);


		    /*	INSERT INTO TRADE_HISTORY
			VALUES (req_trade_id, now_dts, status_submitted); */

#ifdef USE_PREPARE
		    stmt = m_pDBConnection->m_pPrepared[MEESUT_STMT_MFF1_5];
#ifdef ORACLE_ODBC
		    snprintf(param_num_str, 30, "%lld", req_trade_id);
		    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
		    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &req_trade_id, NULL);
#endif
		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(now_dts, TIMESTAMP_LEN+1), 0, now_dts, NULL);
		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->StatusAndTradeType.status_submitted, cST_ID_len+1), 0,
				      (void*)(pIn->StatusAndTradeType.status_submitted), NULL);
		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    rc = SQLExecute(stmt);
#else // !USE_PREPARE
		    stmt = m_Stmt;
		    ostringstream osMFF1_5;
		    osMFF1_5 << "INSERT INTO trade_history (th_t_id, th_dts, th_st_id) VALUES (" <<
			req_trade_id << ", '" << now_dts << "', '" <<
			pIn->StatusAndTradeType.status_submitted <<
			"')";
		    rc = SQLExecDirect(stmt, (SQLCHAR*)(osMFF1_5.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
		    SQLCloseCursor(stmt);


		    strcpy(TradeRequest.symbol, pIn->Entries[i].symbol);
		    TradeRequest.trade_id = req_trade_id;
		    TradeRequest.price_quote = req_price_quote;
		    TradeRequest.trade_qty = req_trade_qty;
		    strcpy(TradeRequest.trade_type_id, req_trade_type);

		    TradeRequestBuffer.push_back(TradeRequest);

		    rows_sent += 1;

		    }
		    catch (const CODBCERR* e)
		    {
			SQLCloseCursor(stmt2);
			throw;
		    }

		    /*	FETCH   request_list
			INTO    req_trade_id,
			        req_price_quote,
			        req_trade_type,
			        req_trade_qty;*/

		    rc = SQLFetch(stmt2);
		    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
			ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
		}
		SQLCloseCursor(stmt2);

		CommitTxn();

		rows_updated += row_count;
		break;
	    }
	    catch (const CODBCERR* e)
	    {
		RollbackTxn();

		//rollback also TradeRequestBuffer
		if(rows_sent > 0)
		{
		    for(int k = 0; k < rows_sent; k++)
			TradeRequestBuffer.pop_back();

		    rows_sent = 0;
		}

		if (e->m_bDeadLock)
		{
#ifdef PRINT_DEADLOCK
		    cout << "  " << e->what() << endl;
#endif
		    delete e;
		    continue; //retry this transaction
		}
		else
		{
		    cout << "ODBCERR at CMarketFeedDB::DoMarketFeedFrame1()" << endl << "  " << e->what() << endl;
		    delete e;
		    break; //end this transaction
		}
	    }
	}
    }


    if (!TradeRequestBuffer.empty())
    {
	int i = 0;
	bool bSent;
	for (vector<TTradeRequest>::iterator c = TradeRequestBuffer.begin();
	     c != TradeRequestBuffer.end(); ++c )
	{
	    bSent = pSendToMarket->SendToMarketFromFrame(*c);
	    if (bSent)
	    {
		++i;
	    }
	}
	pOut->send_len = i;
    }
    else
    {
	pOut->send_len = 0;
    }
    pOut->status = CBaseTxnErr::SUCCESS;

    if (rows_updated != max_feed_len)
    {
	pOut->status = -311;
    }

    //These are dummy to avoid optimize out local values...
#ifdef USE_PREPARE
    now_dts[TIMESTAMP_LEN]=0;
    req_trade_id++;
#ifdef ORACLE_ODBC
    param_num_str[29]=0;
#endif
#endif
}
