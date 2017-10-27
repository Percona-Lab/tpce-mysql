// MarketWatchDB.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

CMarketWatchDB::CMarketWatchDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CMarketWatchDB::~CMarketWatchDB()
{
}

void CMarketWatchDB::DoMarketWatchFrame1(const TMarketWatchFrame1Input *pIn,
					 TMarketWatchFrame1Output *pOut)
{
#ifdef DEBUG
    cout<<"CMarketWatchDB::DoMarketWatchFrame1"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;

    char start_date[15]; //pIn->start_day

    double old_mkt_cap = 0.0;
    double new_mkt_cap = 0.0;

#ifdef ORACLE_ODBC
#ifdef USE_PREPARE
    char param_num_str[30];
    char param_num_str2[30];
#endif
#endif

    // Isolation level required by Clause 7.4.1.3
#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_ISO_L1];
    rc = SQLExecute(stmt);
#else
    stmt = m_Stmt;
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();

    pOut->status = CBaseTxnErr::SUCCESS;

    // start_date = pIn->start_day
    snprintf(start_date, 15, "%04hd-%02hu-%02hu",
	     pIn->start_day.year,
	     pIn->start_day.month,
	     pIn->start_day.day);

#ifndef USE_PREPARE
    ostringstream osMWF1_1;
#endif
    if(pIn->c_id != 0)
    {
	/* SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0)
	   FROM watch_item, watch_list,
	        last_trade, security, daily_market
	   WHERE wl_c_id = %lu
	         AND wi_wl_id = wl_id
		 AND dm_s_symb = wi_s_symb
		 AND dm_date = '%s'
		 AND lt_s_symb = dm_s_symb
		 AND s_symb = dm_s_symb */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_MWF1_1a];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pIn->c_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->c_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(start_date, 15), 0, start_date, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#else // !USE_PREPARE
	stmt = m_Stmt;
	osMWF1_1 << "SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM watch_item, watch_list, last_trade, security, daily_market WHERE wl_c_id = " <<
	    pIn->c_id << " AND wi_wl_id = wl_id AND dm_s_symb = wi_s_symb AND dm_date = '" <<
	    start_date << "' AND lt_s_symb = dm_s_symb AND s_symb = dm_s_symb";
#endif // USE_PREPARE
    }
    else if(pIn->industry_name[0] != 0) // ""
    {
	/* SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0)
	   FROM industry, company, security,
	        last_trade, daily_market
	   WHERE in_name = '%s'
	         AND co_in_id = in_id
		 AND co_id BETWEEN %lu AND %lu
		 AND s_co_id = co_id
		 AND dm_s_symb = s_symb
		 AND dm_date = '%s'
		 AND lt_s_symb = dm_s_symb */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_MWF1_1b];
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->industry_name, cIN_NAME_len+1), 0,
			  (void*)(pIn->industry_name), NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pIn->starting_co_id);
	rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 2, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->starting_co_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
	snprintf(param_num_str2, 30, "%lld", pIn->ending_co_id);
	rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str2, 30), 0, param_num_str2, NULL);
#else
	rc = SQLBindParam(stmt, 3, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->ending_co_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 4, SQL_C_CHAR, SQL_CHAR, strnlen(start_date, 15), 0, start_date, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#else // !USE_PREPARE
	stmt = m_Stmt;
	osMWF1_1 << "SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM industry, company, security, last_trade, daily_market WHERE in_name = '" <<
	    pIn->industry_name << "' AND co_in_id = in_id AND co_id BETWEEN " <<
	    pIn->starting_co_id << " AND " << pIn->ending_co_id << " AND s_co_id = co_id AND dm_s_symb = s_symb AND dm_date = '" <<
	    start_date << "' AND lt_s_symb = dm_s_symb";
#endif // USE_PREPARE
    }
    else if(pIn->acct_id != 0)
    {
	/* SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0)
	   FROM holding_summary,
	        last_trade, security, daily_market
	   WHERE hs_ca_id = %lu
	         AND dm_s_symb = hs_s_symb
		 AND dm_date = '%s'
		 AND lt_s_symb = dm_s_symb
		 AND s_symb = dm_s_symb */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_MWF1_1c];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pIn->acct_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->acct_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(start_date, 15), 0, start_date, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#else // !USE_PREPARE
	stmt = m_Stmt;
	osMWF1_1 << "SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM holding_summary, last_trade, security, daily_market WHERE hs_ca_id = " <<
	    pIn->acct_id << " AND dm_s_symb = hs_s_symb AND dm_date = '" <<
	    start_date << "' AND lt_s_symb = dm_s_symb AND s_symb = dm_s_symb";
#endif // USE_PREPARE
//    }
//    else
//    {
//	pOut->status = CBaseTxnErr::BAD_INPUT_DATA;
    }

//    if(pOut->status != CBaseTxnErr::BAD_INPUT_DATA)
    {
#ifdef USE_PREPARE
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osMWF1_1.str().c_str()), SQL_NTS);
#endif // USE_PREPARE
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &old_mkt_cap, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 2, SQL_C_DOUBLE, &new_mkt_cap, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	if (old_mkt_cap != 0.0)
	{
	    pOut->pct_change = 100.0 * (new_mkt_cap / old_mkt_cap - 1.0);
	}
	else
	{
	    pOut->pct_change = 0.0;
	}
    }


    CommitTxn();

    //These are dummy to avoid optimize out local values...
#ifdef USE_PREPARE
    start_date[14]=0;
#ifdef ORACLE_ODBC
    param_num_str[29]=0;
    param_num_str2[29]=0;
#endif
#endif
}
