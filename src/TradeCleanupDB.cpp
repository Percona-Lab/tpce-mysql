// TradeCleanupDB.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

CTradeCleanupDB::CTradeCleanupDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CTradeCleanupDB::~CTradeCleanupDB()
{
}

void CTradeCleanupDB::DoTradeCleanupFrame1(
    const TTradeCleanupFrame1Input *pIn,
    TTradeCleanupFrame1Output *pOut)
{
#ifdef DEBUG
    cout<<"CTradeCleanupDB::DoTradeCleanupFrame1"<<endl;
#endif
    SQLHSTMT stmt;
    SQLHSTMT stmt2;

    SQLRETURN rc;

    TTrade t_id; /*INT64*/
    TTrade tr_t_id; /*INT64*/
    char   now_dts[TIMESTAMP_LEN+1];

#ifdef ORACLE_ODBC
#ifdef USE_PREPARE
    char param_num_str[30];
#endif
    char num_str[30];
#endif

    // * Isolation level is "not" required. So set L1.
#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[DMSUT_STMT_ISO_L1];
    rc = SQLExecute(stmt);
#else
    stmt = m_Stmt;
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();


    /*	OPEN pending_list FOR
	SELECT  TR_T_ID
	FROM    TRADE_REQUEST
	ORDER BY TR_T_ID; */

    stmt2 = m_Stmt2;
    ostringstream osTCF1_1;
#ifdef ORACLE_ODBC
    osTCF1_1 << "SELECT TO_CHAR(tr_t_id) FROM trade_request ORDER BY tr_t_id";
#else
    osTCF1_1 << "SELECT tr_t_id FROM trade_request ORDER BY tr_t_id";
#endif
    rc = SQLExecDirect(stmt2, (SQLCHAR*)(osTCF1_1.str().c_str()), SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);


    /*	FETCH   pending_list
	INTO    tr_t_id; */

#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt2, 1, SQL_C_CHAR, num_str, 30, NULL);
#else
    rc = SQLBindCol(stmt2, 1, SQL_C_SBIGINT, &tr_t_id, 0, NULL);
#endif

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);

    rc = SQLFetch(stmt2);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);


    while (rc != SQL_NO_DATA_FOUND)
    {
#ifdef ORACLE_ODBC
	tr_t_id = atoll(num_str);
#endif

	gettimestamp(now_dts, STRFTIME_FORMAT, TIMESTAMP_LEN);

	try
	{

	/*	INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID)
		VALUES (tr_t_id, now_dts, st_submitted_id); */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[DMSUT_STMT_TCF1_2];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", tr_t_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &tr_t_id, NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(now_dts, TIMESTAMP_LEN+1), 0, now_dts, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->st_submitted_id, cST_ID_len+1), 0,
			  (void*) (pIn->st_submitted_id), NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTCF1_2;
	osTCF1_2 << "INSERT INTO trade_history (th_t_id, th_dts, th_st_id) VALUES (" << tr_t_id <<
	    ", '" << now_dts << "', '" << pIn->st_submitted_id <<
	    "')";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_2.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);


	/*	UPDATE  TRADE
		SET     T_ST_ID = st_canceled_id,
		T_DTS = now_dts
		WHERE   T_ID = tr_t_id; */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[DMSUT_STMT_TCF1_5];
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->st_canceled_id, cST_ID_len+1), 0,
			  (void*) (pIn->st_canceled_id), NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(now_dts, TIMESTAMP_LEN+1), 0, now_dts, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", tr_t_id);
	rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 3, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &tr_t_id, NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTCF1_5;
	osTCF1_5 << "UPDATE trade SET t_st_id = '" << pIn->st_canceled_id <<
	    "', t_dts = '" << now_dts <<
	    "' WHERE t_id = " << tr_t_id;
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_5.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);


	/*	INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID)
		VALUES (tr_t_id, now_dts, st_canceled_id); */
#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[DMSUT_STMT_TCF1_2];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", tr_t_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &tr_t_id, NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(now_dts, TIMESTAMP_LEN+1), 0, now_dts, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->st_canceled_id, cST_ID_len+1), 0,
			  (void*) (pIn->st_canceled_id), NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTCF1_6;
	osTCF1_6 << "INSERT INTO trade_history(th_t_id, th_dts, th_st_id) VALUES (" << tr_t_id <<
	    ", '" << now_dts << "', '" << pIn->st_canceled_id <<
	    "')";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_6.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	}
	catch (const CODBCERR* e)
	{
	    SQLCloseCursor(stmt2);
	    throw;
	}

	/*	FETCH   pending_list
		INTO    tr_trade_id; */

	rc = SQLFetch(stmt2);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt2);


    /*	DELETE FROM TRADE_REQUEST; */

    stmt = m_Stmt;
    ostringstream osTCF1_3;
    osTCF1_3 << "DELETE FROM trade_request";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_3.str().c_str()), SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);


    /*	OPEN submit_list FOR
	SELECT  T_ID
	FROM    TRADE
	WHERE   T_ID >= start_trade_id AND
	        T_ST_ID = st_submitted_id; */

    stmt2 = m_Stmt2;
    ostringstream osTCF1_4;
#ifdef ORACLE_ODBC
    osTCF1_4 << "SELECT TO_CHAR(t_id) FROM trade WHERE t_id >= " << pIn->start_trade_id <<
#else
    osTCF1_4 << "SELECT t_id FROM trade WHERE t_id >= " << pIn->start_trade_id <<
#endif
	" AND t_st_id = '" << pIn->st_submitted_id <<
	"'";
    rc = SQLExecDirect(stmt2, (SQLCHAR*)(osTCF1_4.str().c_str()), SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);


    /*	FETCH   submit_list
	INTO    t_id; */

#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt2, 1, SQL_C_CHAR, num_str, 30, NULL);
#else
    rc = SQLBindCol(stmt2, 1, SQL_C_SBIGINT, &t_id, 0, NULL);
#endif

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);

    rc = SQLFetch(stmt2);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);


    while (rc != SQL_NO_DATA_FOUND)
    {
#ifdef ORACLE_ODBC
	t_id = atoll(num_str);
#endif

	gettimestamp(now_dts, STRFTIME_FORMAT, TIMESTAMP_LEN);

	try
	{

	/*	UPDATE  TRADE
		SET     T_ST_ID = st_canceled_id,
		        T_DTS = now_dts
		WHERE   T_ID = t_id; */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[DMSUT_STMT_TCF1_5];
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->st_canceled_id, cST_ID_len+1), 0,
			  (void*) (pIn->st_canceled_id), NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(now_dts, TIMESTAMP_LEN+1), 0, now_dts, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", t_id);
	rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 3, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &t_id, NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTCF1_5;
	osTCF1_5 << "UPDATE trade SET t_st_id = '" << pIn->st_canceled_id <<
	    "', t_dts = '" << now_dts <<
	    "' WHERE t_id = " << t_id;
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_5.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);


	/*	INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID)
		VALUES (t_id, now_dts, st_canceled_id); */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[DMSUT_STMT_TCF1_2];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", t_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &t_id, NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(now_dts, TIMESTAMP_LEN+1), 0, now_dts, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->st_canceled_id, cST_ID_len+1), 0,
			  (void*) (pIn->st_canceled_id), NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osTCF1_6;
	osTCF1_6 << "INSERT INTO trade_history(th_t_id, th_dts, th_st_id) VALUES (" << t_id <<
	    ", '" << now_dts << "', '" << pIn->st_canceled_id <<
	    "')";
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osTCF1_6.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	}
	catch (const CODBCERR* e)
	{
	    SQLCloseCursor(stmt2);
	    throw;
	}

	/*	FETCH   submit_list
		INTO    t_id; */

	rc = SQLFetch(stmt2);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt2, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt2);

    CommitTxn();

    pOut->status = CBaseTxnErr::SUCCESS;

    //These are dummy to avoid optimize out local values...
#ifdef USE_PREPARE
    t_id++;
    tr_t_id++;
    now_dts[TIMESTAMP_LEN]=0;
#ifdef ORACLE_ODBC
    param_num_str[29]=0;
#endif
#endif
}
