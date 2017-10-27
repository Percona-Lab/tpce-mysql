// DBConnection.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

CDBConnection::CDBConnection(const char *szHost, const char *szDBName,
			     const char *szDBUser, const char *szDBPass, int szPrepareType)
{
    SQLRETURN rc;

#ifdef DEBUG
    cout<<"CDBConnection::CDBConnection"<<endl;
#endif

    m_Env = NULL;
    m_Conn = NULL;
    m_Stmt = m_Stmt2 = NULL;

    m_PrepareType = szPrepareType;
    m_pPrepared = NULL;

    if ( SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_Env) != SQL_SUCCESS )
	ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_ENV, m_Env);

    if ( SQLSetEnvAttr(m_Env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0 ) != SQL_SUCCESS )
	ThrowError(CODBCERR::eSetEnvAttr);

    if ( SQLAllocHandle(SQL_HANDLE_DBC, m_Env, &m_Conn) != SQL_SUCCESS )
	ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_ENV, m_Env);

    if ( SQLSetConnectAttr(m_Conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0) != SQL_SUCCESS )
	ThrowError(CODBCERR::eSetConnectAttr);
#ifdef ODBC_WRAPPER
    rc = SQLConnect_DIRECT(m_Conn, (SQLCHAR *)szHost, SQL_NTS, (SQLCHAR *)szDBName, SQL_NTS,
			   (SQLCHAR *)szDBUser, SQL_NTS, (SQLCHAR *)szDBPass, SQL_NTS);
#else
    rc = SQLConnect(m_Conn, (SQLCHAR *)szDBName, SQL_NTS,
		    (SQLCHAR *)szDBUser, SQL_NTS, (SQLCHAR *)szDBPass, SQL_NTS);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eConnect);

    if ( SQLAllocHandle(SQL_HANDLE_STMT, m_Conn, &m_Stmt) != SQL_SUCCESS)
	ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_DBC, m_Conn);

    if ( SQLAllocHandle(SQL_HANDLE_STMT, m_Conn, &m_Stmt2) != SQL_SUCCESS)
	ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_DBC, m_Conn);

#ifdef USE_PREPARE
    if (m_PrepareType == 1) //Prapered Statements for CESUT
    {
	//============================================================================

	m_pPrepared = new SQLHSTMT[CESUT_STMT_MAX];
	for (int i=0; i < CESUT_STMT_MAX; i++)
	{
	    if ( SQLAllocHandle(SQL_HANDLE_STMT, m_Conn, &m_pPrepared[i]) != SQL_SUCCESS)
		ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_DBC, m_Conn);
	}

	//ISO_L1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_ISO_L1],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_ISO_L1], __FILE__, __LINE__);

	//ISO_L2
#if (defined(ORACLE_ODBC)||defined(PGSQL_ODBC))
	//Oracle and PostgreSQL don't have "REPEATABLE READ" level
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_ISO_L2],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", SQL_NTS);
#else
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_ISO_L2],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", SQL_NTS);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_ISO_L2], __FILE__, __LINE__);

	//ISO_L3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_ISO_L3],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_ISO_L3], __FILE__, __LINE__);

	//===BrokerVolume=========================
	//BVF1_1

	//===CustomerPosition=========================
	//CPF1_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_CPF1_1],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(c_id) FROM customer WHERE c_tax_id = ?",
#else
			(SQLCHAR*)"SELECT c_id FROM customer WHERE c_tax_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_CPF1_1], __FILE__, __LINE__);

	//CPF1_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_CPF1_2],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT c_st_id, c_l_name, c_f_name, c_m_name, c_gndr, c_tier, DATE_FORMAT(c_dob,'%Y-%m-%d'), c_ad_id, c_ctry_1, c_area_1, c_local_1, c_ext_1, c_ctry_2, c_area_2, c_local_2, c_ext_2, c_ctry_3, c_area_3, c_local_3, c_ext_3, c_email_1, c_email_2 FROM customer WHERE c_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT c_st_id, c_l_name, c_f_name, c_m_name, c_gndr, c_tier, TO_CHAR(c_dob,'YYYY-MM-DD'), c_ad_id, c_ctry_1, c_area_1, c_local_1, c_ext_1, c_ctry_2, c_area_2, c_local_2, c_ext_2, c_ctry_3, c_area_3, c_local_3, c_ext_3, c_email_1, c_email_2 FROM customer WHERE c_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT c_st_id, c_l_name, c_f_name, c_m_name, c_gndr, c_tier, TO_CHAR(c_dob,'YYYY-MM-DD'), TO_CHAR(c_ad_id), c_ctry_1, c_area_1, c_local_1, c_ext_1, c_ctry_2, c_area_2, c_local_2, c_ext_2, c_ctry_3, c_area_3, c_local_3, c_ext_3, c_email_1, c_email_2 FROM customer WHERE c_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_CPF1_2], __FILE__, __LINE__);

	//CPF1_3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_CPF1_3],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT /*+ USE_NL(customer_account holding_summary last_trade) */ * FROM (SELECT /*+ USE_NL(customer_account holding_summary last_trade) */ TO_CHAR(ca_id), ca_bal, COALESCE(SUM(hs_qty * lt_price),0) price_sum FROM customer_account LEFT OUTER JOIN holding_summary ON hs_ca_id = ca_id, last_trade WHERE ca_c_id = ? AND lt_s_symb = hs_s_symb GROUP BY ca_id,ca_bal ORDER BY price_sum ASC ) WHERE ROWNUM <= ?",
#else
			(SQLCHAR*)"SELECT ca_id, ca_bal, COALESCE(SUM(hs_qty * lt_price),0) AS price_sum FROM customer_account LEFT OUTER JOIN holding_summary ON hs_ca_id = ca_id, last_trade WHERE ca_c_id = ? AND lt_s_symb = hs_s_symb GROUP BY ca_id,ca_bal ORDER BY price_sum ASC LIMIT ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_CPF1_3], __FILE__, __LINE__);

	//CPF2_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_CPF2_1],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT t_id, t_s_symb, t_qty, st_name, DATE_FORMAT(th_dts,'%Y-%m-%d %H:%i:%s.%f') FROM (SELECT t_id AS id FROM trade WHERE t_ca_id = ? ORDER BY t_dts DESC LIMIT 10) AS t, trade, trade_history, status_type FORCE INDEX(PRIMARY) WHERE t_id = id AND th_t_id = t_id AND st_id = th_st_id ORDER BY th_dts DESC LIMIT ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT t_id, t_s_symb, t_qty, st_name, TO_CHAR(th_dts,'YYYY-MM-DD HH24:MI:SS.US') FROM (SELECT t_id AS id FROM trade WHERE t_ca_id = ? ORDER BY t_dts DESC LIMIT 10) AS t, trade, trade_history, status_type WHERE t_id = id AND th_t_id = t_id AND st_id = th_st_id ORDER BY th_dts DESC LIMIT ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(t_id), t_s_symb, t_qty, st_name, TO_CHAR(th_dts,'YYYY-MM-DD HH24:MI:SS.FF6') FROM (SELECT * FROM (SELECT t_id AS id FROM trade WHERE t_ca_id = ? ORDER BY t_dts DESC) WHERE ROWNUM <= 10), trade, trade_history, status_type WHERE t_id = id AND th_t_id = t_id AND st_id = th_st_id ORDER BY th_dts DESC) WHERE ROWNUM <= ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_CPF2_1], __FILE__, __LINE__);

	//===MarketWatch==================
	//MWF1_1a
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_MWF1_1a],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT /*+ USE_NL(watch_item watch_list last_trade security daily_market) */ COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM watch_item, watch_list, last_trade, security, daily_market WHERE wl_c_id = ? AND wi_wl_id = wl_id AND dm_s_symb = wi_s_symb AND dm_date = ? AND lt_s_symb = dm_s_symb AND s_symb = dm_s_symb",
#else
			(SQLCHAR*)"SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM watch_item, watch_list, last_trade, security, daily_market WHERE wl_c_id = ? AND wi_wl_id = wl_id AND dm_s_symb = wi_s_symb AND dm_date = ? AND lt_s_symb = dm_s_symb AND s_symb = dm_s_symb",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_MWF1_1a], __FILE__, __LINE__);

	//MWF1_1b
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_MWF1_1b],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT /*+ USE_NL(industry company security last_trade daily_market) */ COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM industry, company, security, last_trade, daily_market WHERE in_name = ? AND co_in_id = in_id AND co_id BETWEEN ? AND ? AND s_co_id = co_id AND dm_s_symb = s_symb AND dm_date = ? AND lt_s_symb = dm_s_symb",
#else
			(SQLCHAR*)"SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM industry, company, security, last_trade, daily_market WHERE in_name = ? AND co_in_id = in_id AND co_id BETWEEN ? AND ? AND s_co_id = co_id AND dm_s_symb = s_symb AND dm_date = ? AND lt_s_symb = dm_s_symb",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_MWF1_1b], __FILE__, __LINE__);

	//MWF1_1c
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_MWF1_1c],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT /*+ USE_NL(holding_summary last_trade security daily_market) */ COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM holding_summary, last_trade, security, daily_market WHERE hs_ca_id = ? AND dm_s_symb = hs_s_symb AND dm_date = ? AND lt_s_symb = dm_s_symb AND s_symb = dm_s_symb",
#else
			(SQLCHAR*)"SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM holding_summary, last_trade, security, daily_market WHERE hs_ca_id = ? AND dm_s_symb = hs_s_symb AND dm_date = ? AND lt_s_symb = dm_s_symb AND s_symb = dm_s_symb",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_MWF1_1c], __FILE__, __LINE__);

	//===SecurityDetail================
	//SDF1_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_SDF1_1],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT s_name, co_id, co_name, co_sp_rate, co_ceo, co_desc, DATE_FORMAT(co_open_date,'%Y-%m-%d'), co_st_id, ca.ad_line1, ca.ad_line2, zca.zc_town, zca.zc_div, ca.ad_zc_code, ca.ad_ctry, s_num_out, DATE_FORMAT(s_start_date,'%Y-%m-%d'), DATE_FORMAT(s_exch_date,'%Y-%m-%d'), s_pe, s_52wk_high, DATE_FORMAT(s_52wk_high_date,'%Y-%m-%d'), s_52wk_low, DATE_FORMAT(s_52wk_low_date,'%Y-%m-%d'), s_dividend, s_yield, zea.zc_div, ea.ad_ctry, ea.ad_line1, ea.ad_line2, zea.zc_town, ea.ad_zc_code, ex_close, ex_desc, ex_name, ex_num_symb, ex_open FROM security, company, address ca, address ea, zip_code zca, zip_code zea, exchange WHERE s_symb = ? AND co_id = s_co_id AND ca.ad_id = co_ad_id AND ea.ad_id = ex_ad_id AND ex_id = s_ex_id AND ca.ad_zc_code = zca.zc_code AND ea.ad_zc_code = zea.zc_code",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT s_name, co_id, co_name, co_sp_rate, co_ceo, co_desc, TO_CHAR(co_open_date,'YYYY-MM-DD'), co_st_id, ca.ad_line1, ca.ad_line2, zca.zc_town, zca.zc_div, ca.ad_zc_code, ca.ad_ctry, s_num_out, TO_CHAR(s_start_date,'YYYY-MM-DD'), TO_CHAR(s_exch_date,'YYYY-MM-DD'), s_pe, s_52wk_high, TO_CHAR(s_52wk_high_date,'YYYY-MM-DD'), s_52wk_low, TO_CHAR(s_52wk_low_date,'YYYY-MM-DD'), s_dividend, s_yield, zea.zc_div, ea.ad_ctry, ea.ad_line1, ea.ad_line2, zea.zc_town, ea.ad_zc_code, ex_close, ex_desc, ex_name, ex_num_symb, ex_open FROM security, company, address ca, address ea, zip_code zca, zip_code zea, exchange WHERE s_symb = ? AND co_id = s_co_id AND ca.ad_id = co_ad_id AND ea.ad_id = ex_ad_id AND ex_id = s_ex_id AND ca.ad_zc_code = zca.zc_code AND ea.ad_zc_code = zea.zc_code",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT s_name, TO_CHAR(co_id), co_name, co_sp_rate, co_ceo, co_desc, TO_CHAR(co_open_date,'YYYY-MM-DD'), co_st_id, ca.ad_line1, ca.ad_line2, zca.zc_town, zca.zc_div, ca.ad_zc_code, ca.ad_ctry, TO_CHAR(s_num_out), TO_CHAR(s_start_date,'YYYY-MM-DD'), TO_CHAR(s_exch_date,'YYYY-MM-DD'), s_pe, s_52wk_high, TO_CHAR(s_52wk_high_date,'YYYY-MM-DD'), s_52wk_low, TO_CHAR(s_52wk_low_date,'YYYY-MM-DD'), s_dividend, s_yield, zea.zc_div, ea.ad_ctry, ea.ad_line1, ea.ad_line2, zea.zc_town, ea.ad_zc_code, ex_close, ex_desc, ex_name, ex_num_symb, ex_open FROM security, company, address ca, address ea, zip_code zca, zip_code zea, exchange WHERE s_symb = ? AND co_id = s_co_id AND ca.ad_id = co_ad_id AND ea.ad_id = ex_ad_id AND ex_id = s_ex_id AND ca.ad_zc_code = zca.zc_code AND ea.ad_zc_code = zea.zc_code",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_SDF1_1], __FILE__, __LINE__);

	//SDF1_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_SDF1_2],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT co_name, in_name FROM company_competitor, company, industry WHERE cp_co_id = ? AND co_id = cp_comp_co_id AND in_id = cp_in_id) WHERE ROWNUM <= ?",
#else
			(SQLCHAR*)"SELECT co_name, in_name FROM company_competitor, company, industry WHERE cp_co_id = ? AND co_id = cp_comp_co_id AND in_id = cp_in_id LIMIT ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_SDF1_2], __FILE__, __LINE__);

	//SDF1_3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_SDF1_3],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT fi_year, fi_qtr, DATE_FORMAT(fi_qtr_start_date,'%Y-%m-%d'), fi_revenue, fi_net_earn, fi_basic_eps, fi_dilut_eps, fi_margin, fi_inventory, fi_assets, fi_liability, fi_out_basic, fi_out_dilut FROM financial WHERE fi_co_id = ? ORDER BY fi_year ASC, fi_qtr LIMIT ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT fi_year, fi_qtr, TO_CHAR(fi_qtr_start_date,'YYYY-MM-DD'), fi_revenue, fi_net_earn, fi_basic_eps, fi_dilut_eps, fi_margin, fi_inventory, fi_assets, fi_liability, fi_out_basic, fi_out_dilut FROM financial WHERE fi_co_id = ? ORDER BY fi_year ASC, fi_qtr LIMIT ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT fi_year, fi_qtr, TO_CHAR(fi_qtr_start_date,'YYYY-MM-DD'), fi_revenue, fi_net_earn, fi_basic_eps, fi_dilut_eps, fi_margin, fi_inventory, fi_assets, fi_liability, TO_CHAR(fi_out_basic), TO_CHAR(fi_out_dilut) FROM financial WHERE fi_co_id = ? ORDER BY fi_year ASC, fi_qtr) WHERE ROWNUM <= ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_SDF1_3], __FILE__, __LINE__);

	//SDF1_4
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_SDF1_4],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT DATE_FORMAT(dm_date,'%Y-%m-%d'), dm_close, dm_high, dm_low, dm_vol FROM daily_market WHERE dm_s_symb = ? AND dm_date >= ? ORDER BY dm_date ASC LIMIT ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(dm_date,'YYYY-MM-DD'), dm_close, dm_high, dm_low, dm_vol FROM daily_market WHERE dm_s_symb = ? AND dm_date >= ? ORDER BY dm_date ASC LIMIT ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(dm_date,'YYYY-MM-DD'), dm_close, dm_high, dm_low, TO_CHAR(dm_vol) FROM daily_market WHERE dm_s_symb = ? AND dm_date >= ? ORDER BY dm_date ASC) WHERE ROWNUM <= ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_SDF1_4], __FILE__, __LINE__);

	//SDF1_5
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_SDF1_5],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT lt_price, lt_open_price, TO_CHAR(lt_vol) FROM last_trade WHERE lt_s_symb = ?",
#else
			(SQLCHAR*)"SELECT lt_price, lt_open_price, lt_vol FROM last_trade WHERE lt_s_symb = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_SDF1_5], __FILE__, __LINE__);

	//SDF1_6
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_SDF1_6],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT ni_item, DATE_FORMAT(ni_dts, '%Y-%m-%d %H:%i:%s.%f'), ni_source, ni_author FROM news_xref, news_item WHERE  ni_id = nx_ni_id AND nx_co_id = ? LIMIT ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT ni_item, TO_CHAR(ni_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ni_source, ni_author FROM news_xref, news_item WHERE  ni_id = nx_ni_id AND nx_co_id = ? LIMIT ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT ni_item, TO_CHAR(ni_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ni_source, ni_author FROM news_xref, news_item WHERE  ni_id = nx_ni_id AND nx_co_id = ?) WHERE ROWNUM <= ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_SDF1_6], __FILE__, __LINE__);

	//SDF1_7
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_SDF1_7],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT DATE_FORMAT(ni_dts, '%Y-%m-%d %H:%i:%s.%f'), ni_source, ni_author, ni_headline, ni_summary FROM news_xref, news_item WHERE ni_id = nx_ni_id AND nx_co_id = ? LIMIT ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(ni_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ni_source, ni_author, ni_headline, ni_summary FROM news_xref, news_item WHERE ni_id = nx_ni_id AND nx_co_id = ? LIMIT ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(ni_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ni_source, ni_author, ni_headline, ni_summary FROM news_xref, news_item WHERE ni_id = nx_ni_id AND nx_co_id = ?) WHERE ROWNUM <= ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_SDF1_7], __FILE__, __LINE__);

	//===TradeLookup=========================
	//TLF1_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF1_1],
			(SQLCHAR*)"SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt, t_trade_price FROM trade, trade_type WHERE t_id = ? AND t_tt_id = tt_id",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF1_1], __FILE__, __LINE__);

	//TLF1_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF1_2],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF1_2], __FILE__, __LINE__);

	//TLF1_3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF1_3],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF1_3], __FILE__, __LINE__);

	//TLF1_4
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF1_4],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts LIMIT 3",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.US'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts LIMIT 3",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts) WHERE ROWNUM <= 3",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF1_4], __FILE__, __LINE__);

	//TLF2_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF2_1],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT t_bid_price, t_exec_name, t_is_cash, TO_CHAR(t_id), t_trade_price FROM trade WHERE t_ca_id = ? AND t_dts >= ? AND t_dts <= ? ORDER BY t_dts) WHERE ROWNUM <= ?",
#else
			(SQLCHAR*)"SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price FROM trade WHERE t_ca_id = ? AND t_dts >= ? AND t_dts <= ? ORDER BY t_dts LIMIT ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF2_1], __FILE__, __LINE__);

	//TLF2_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF2_2],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF2_2], __FILE__, __LINE__);

	//TLF2_3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF2_3],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF2_3], __FILE__, __LINE__);

	//TLF2_4
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF2_4],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts LIMIT 3",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.US'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts LIMIT 3",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts) WHERE ROWNUM <= 3",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF2_4], __FILE__, __LINE__);

	//TLF3_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF3_1],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty, DATE_FORMAT(t_dts, '%Y-%m-%d %H:%i:%s.%f'), t_id, t_tt_id FROM trade WHERE t_s_symb = ? AND t_dts >= ? AND t_dts <= ? ORDER BY t_dts LIMIT ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty, TO_CHAR(t_dts, 'YYYY-MM-DD HH24:MI:SS.US'), t_id, t_tt_id FROM trade WHERE t_s_symb = ? AND t_dts >= ? AND t_dts <= ? ORDER BY t_dts LIMIT ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(t_ca_id), t_exec_name, t_is_cash, t_trade_price, t_qty, TO_CHAR(t_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), TO_CHAR(t_id), t_tt_id FROM trade WHERE t_s_symb = ? AND t_dts >= ? AND t_dts <= ? ORDER BY t_dts) WHERE ROWNUM <= ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF3_1], __FILE__, __LINE__);

	//TLF3_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF3_2],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF3_2], __FILE__, __LINE__);

	//TLF3_3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF3_3],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF3_3], __FILE__, __LINE__);

	//TLF3_4
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF3_4],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts ASC LIMIT 3",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.US'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts ASC LIMIT 3",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts ASC) WHERE ROWNUM <= 3",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF3_4], __FILE__, __LINE__);

	//TLF4_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF4_1],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(t_id) FROM trade WHERE t_ca_id = ? AND t_dts >= ? ORDER BY t_dts ASC) WHERE ROWNUM <= 1",
#else
			(SQLCHAR*)"SELECT t_id FROM trade WHERE t_ca_id = ? AND t_dts >= ? ORDER BY t_dts ASC LIMIT 1",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF4_1], __FILE__, __LINE__);

	//TLF4_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TLF4_2],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(h1.hh_h_t_id), TO_CHAR(h1.hh_t_id), h1.hh_before_qty, h1.hh_after_qty FROM holding_history h1, holding_history h2 WHERE h1.hh_h_t_id = h2.hh_h_t_id AND h2.hh_t_id = ?) WHERE ROWNUM <= 20",
#else
			(SQLCHAR*)"SELECT h1.hh_h_t_id, h1.hh_t_id, h1.hh_before_qty, h1.hh_after_qty FROM holding_history h1, holding_history h2 WHERE h1.hh_h_t_id = h2.hh_h_t_id AND h2.hh_t_id = ? LIMIT 20",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TLF4_2], __FILE__, __LINE__);

	//===TradeOrder==========================
	//TOF1_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF1_1],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT ca_name, TO_CHAR(ca_b_id), TO_CHAR(ca_c_id), ca_tax_st FROM customer_account WHERE ca_id = ?",
#else
			(SQLCHAR*)"SELECT ca_name, ca_b_id, ca_c_id, ca_tax_st FROM customer_account WHERE ca_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF1_1], __FILE__, __LINE__);

	//TOF1_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF1_2],
			(SQLCHAR*)"SELECT c_f_name, c_l_name, c_tier, c_tax_id FROM customer WHERE c_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF1_2], __FILE__, __LINE__);

	//TOF1_3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF1_3],
			(SQLCHAR*)"SELECT b_name FROM broker WHERE b_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF1_3], __FILE__, __LINE__);

	//TOF2_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF2_1],
			(SQLCHAR*)"SELECT ap_acl FROM account_permission WHERE ap_ca_id = ? AND ap_f_name = ? AND ap_l_name = ? AND ap_tax_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF2_1], __FILE__, __LINE__);

	//TOF3_1a
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_1a],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(co_id) FROM company WHERE co_name = ?",
#else
			(SQLCHAR*)"SELECT co_id FROM company WHERE co_name = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_1a], __FILE__, __LINE__);

	//TOF3_2a
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_2a],
			(SQLCHAR*)"SELECT s_ex_id, s_name, s_symb FROM security WHERE s_co_id = ? AND s_issue = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_2a], __FILE__, __LINE__);

	//TOF3_1b
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_1b],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(s_co_id), s_ex_id, s_name FROM security WHERE s_symb = ?",
#else
			(SQLCHAR*)"SELECT s_co_id, s_ex_id, s_name FROM security WHERE s_symb = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_1b], __FILE__, __LINE__);

	//TOF3_2b
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_2b],
			(SQLCHAR*)"SELECT co_name FROM company WHERE co_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_2b], __FILE__, __LINE__);

	//TOF3_3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_3],
			(SQLCHAR*)"SELECT lt_price FROM last_trade WHERE lt_s_symb = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_3], __FILE__, __LINE__);

	//TOF3_4
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_4],
			(SQLCHAR*)"SELECT tt_is_mrkt, tt_is_sell FROM trade_type WHERE tt_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_4], __FILE__, __LINE__);

	//TOF3_5
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_5],
			(SQLCHAR*)"SELECT hs_qty FROM holding_summary WHERE hs_ca_id = ? AND hs_s_symb = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_5], __FILE__, __LINE__);

	//TOF3_6a
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_6a],
			(SQLCHAR*)"SELECT h_qty, h_price FROM holding WHERE h_ca_id = ? AND h_s_symb = ? ORDER BY h_dts DESC",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_6a], __FILE__, __LINE__);

	//TOF3_6b
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_6b],
			(SQLCHAR*)"SELECT h_qty, h_price FROM holding WHERE h_ca_id = ? AND h_s_symb = ? ORDER BY h_dts ASC",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_6b], __FILE__, __LINE__);

	//TOF3_7
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_7],
			(SQLCHAR*)"SELECT sum(tx_rate) FROM taxrate, customer_taxrate WHERE tx_id = cx_tx_id AND cx_c_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_7], __FILE__, __LINE__);

	//TOF3_8
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_8],
			(SQLCHAR*)"SELECT cr_rate FROM commission_rate WHERE cr_c_tier = ? AND cr_tt_id = ? AND cr_ex_id = ? AND cr_from_qty <= ? AND cr_to_qty >= ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_8], __FILE__, __LINE__);

	//TOF3_9
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_9],
			(SQLCHAR*)"SELECT ch_chrg FROM charge WHERE ch_c_tier = ? AND ch_tt_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_9], __FILE__, __LINE__);

	//TOF3_10
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_10],
			(SQLCHAR*)"SELECT ca_bal FROM customer_account WHERE ca_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_10], __FILE__, __LINE__);

	//TOF3_11
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF3_11],
			(SQLCHAR*)"SELECT sum(hs_qty * lt_price) FROM holding_summary, last_trade WHERE hs_ca_id = ? AND lt_s_symb = hs_s_symb",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF3_11], __FILE__, __LINE__);

	//TOF4_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF4_1],
			(SQLCHAR*)"INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name, t_trade_price, t_chrg, t_comm, t_tax, t_lifo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, 0, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF4_1], __FILE__, __LINE__);

	//TOF4_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF4_2],
			(SQLCHAR*)"INSERT INTO trade_request(tr_t_id, tr_tt_id, tr_s_symb, tr_qty, tr_bid_price, tr_b_id) VALUES (?, ?, ?, ?, ?, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF4_2], __FILE__, __LINE__);

	//TOF4_3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TOF4_3],
			(SQLCHAR*)"INSERT INTO trade_history(th_t_id, th_dts, th_st_id) VALUES (?, ?, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TOF4_3], __FILE__, __LINE__);


	//===TradeStatus=========================
	//TSF1_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TSF1_1],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT t_id, DATE_FORMAT(t_dts,'%Y-%m-%d %H:%i:%s.%f'), st_name, tt_name, t_s_symb, t_qty, t_exec_name, t_chrg, s_name, ex_name FROM trade, status_type FORCE INDEX(PRIMARY), trade_type FORCE INDEX(PRIMARY), security, exchange WHERE t_ca_id = ? AND st_id = t_st_id AND tt_id = t_tt_id AND s_symb = t_s_symb AND ex_id = s_ex_id ORDER BY t_dts DESC LIMIT ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT t_id, TO_CHAR(t_dts,'YYYY-MM-DD HH24:MI:SS.US'), st_name, tt_name, t_s_symb, t_qty, t_exec_name, t_chrg, s_name, ex_name FROM trade, status_type, trade_type, security, exchange WHERE t_ca_id = ? AND st_id = t_st_id AND tt_id = t_tt_id AND s_symb = t_s_symb AND ex_id = s_ex_id ORDER BY t_dts DESC LIMIT ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT /*+ USE_NL(trade status_type trade_type security exchange) */ * FROM (SELECT /*+ USE_NL(trade status_type trade_type security exchange) */ TO_CHAR(t_id), TO_CHAR(t_dts,'YYYY-MM-DD HH24:MI:SS.FF6'), st_name, tt_name, t_s_symb, t_qty, t_exec_name, t_chrg, s_name, ex_name FROM trade, status_type, trade_type, security, exchange WHERE t_ca_id = ? AND st_id = t_st_id AND tt_id = t_tt_id AND s_symb = t_s_symb AND ex_id = s_ex_id ORDER BY t_dts DESC) WHERE ROWNUM <= ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TSF1_1], __FILE__, __LINE__);

	//TSF1_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TSF1_2],
			(SQLCHAR*)"SELECT c_l_name, c_f_name, b_name FROM customer_account, customer, broker WHERE ca_id = ? AND c_id = ca_c_id AND b_id = ca_b_id",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TSF1_2], __FILE__, __LINE__);


	//===TradeUpdate=========================
	//TUF1_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF1_1],
			(SQLCHAR*)"SELECT t_exec_name FROM trade WHERE t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF1_1], __FILE__, __LINE__);

	//TUF1_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF1_2],
			(SQLCHAR*)"UPDATE trade SET t_exec_name = ? WHERE t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF1_2], __FILE__, __LINE__);

	//TUF1_3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF1_3],
			(SQLCHAR*)"SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt, t_trade_price FROM trade, trade_type WHERE t_id = ? AND t_tt_id = tt_id",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF1_3], __FILE__, __LINE__);

	//TUF1_4
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF1_4],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF1_4], __FILE__, __LINE__);

	//TUF1_5
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF1_5],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF1_5], __FILE__, __LINE__);

	//TUF1_6
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF1_6],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts LIMIT 3",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.US'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts LIMIT 3",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts) WHERE ROWNUM <= 3",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF1_6], __FILE__, __LINE__);

	//TUF2_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF2_1],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT t_bid_price, t_exec_name, t_is_cash, TO_CHAR(t_id), t_trade_price FROM trade WHERE t_ca_id = ? AND t_dts >= ? AND t_dts <= ? ORDER BY t_dts ASC) WHERE ROWNUM <= ?",
#else
			(SQLCHAR*)"SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price FROM trade WHERE t_ca_id = ? AND t_dts >= ? AND t_dts <= ? ORDER BY t_dts ASC LIMIT ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF2_1], __FILE__, __LINE__);

	//TUF2_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF2_2],
			(SQLCHAR*)"SELECT se_cash_type FROM settlement WHERE se_t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF2_2], __FILE__, __LINE__);

	//TUF2_3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF2_3],
			(SQLCHAR*)"UPDATE settlement SET se_cash_type = ? WHERE se_t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF2_3], __FILE__, __LINE__);

	//TUF2_4
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF2_4],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF2_4], __FILE__, __LINE__);

	//TUF2_5
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF2_5],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF2_5], __FILE__, __LINE__);

	//TUF2_6
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF2_6],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts LIMIT 3",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.US'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts LIMIT 3",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts) WHERE ROWNUM <= 3",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF2_6], __FILE__, __LINE__);

	//TUF3_1
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF3_1],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty, s_name, DATE_FORMAT(t_dts, '%Y-%m-%d %H:%i:%s.%f'), t_id, t_tt_id, tt_name FROM trade, trade_type FORCE INDEX(PRIMARY), security WHERE t_s_symb = ? AND t_dts >= ? AND t_dts <= ? AND tt_id = t_tt_id AND s_symb = t_s_symb ORDER BY t_dts ASC LIMIT ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty, s_name, TO_CHAR(t_dts, 'YYYY-MM-DD HH24:MI:SS.US'), t_id, t_tt_id, tt_name FROM trade, trade_type, security WHERE t_s_symb = ? AND t_dts >= ? AND t_dts <= ? AND tt_id = t_tt_id AND s_symb = t_s_symb ORDER BY t_dts ASC LIMIT ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(t_ca_id), t_exec_name, t_is_cash, t_trade_price, t_qty, s_name, TO_CHAR(t_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), TO_CHAR(t_id), t_tt_id, tt_name FROM trade, trade_type, security WHERE t_s_symb = ? AND t_dts >= ? AND t_dts <= ? AND tt_id = t_tt_id AND s_symb = t_s_symb ORDER BY t_dts ASC) WHERE ROWNUM <= ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF3_1], __FILE__, __LINE__);

	//TUF3_2
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF3_2],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, DATE_FORMAT(se_cash_due_date, '%Y-%m-%d'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT se_amt, TO_CHAR(se_cash_due_date, 'YYYY-MM-DD'), se_cash_type FROM settlement WHERE se_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF3_2], __FILE__, __LINE__);

	//TUF3_3
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF3_3],
			(SQLCHAR*)"SELECT ct_name FROM cash_transaction WHERE ct_t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF3_3], __FILE__, __LINE__);

	//TUF3_4
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF3_4],
			(SQLCHAR*)"UPDATE cash_transaction SET ct_name = ? WHERE ct_t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF3_4], __FILE__, __LINE__);

	//TUF3_5
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF3_5],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, DATE_FORMAT(ct_dts, '%Y-%m-%d %H:%i:%s.%f'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT ct_amt, TO_CHAR(ct_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ct_name FROM cash_transaction WHERE ct_t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF3_5], __FILE__, __LINE__);

	//TUF3_6
	rc = SQLPrepare(m_pPrepared[CESUT_STMT_TUF3_6],
#ifdef MYSQL_ODBC
			(SQLCHAR*)"SELECT DATE_FORMAT(th_dts, '%Y-%m-%d %H:%i:%s.%f'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts ASC LIMIT 3",
#elif PGSQL_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.US'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts ASC LIMIT 3",
#elif ORACLE_ODBC
			(SQLCHAR*)"SELECT * FROM (SELECT TO_CHAR(th_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), th_st_id FROM trade_history WHERE th_t_id = ? ORDER BY th_dts ASC) WHERE ROWNUM <= 3",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[CESUT_STMT_TUF3_6], __FILE__, __LINE__);


    }
    else if (m_PrepareType == 2) //Prapered Statements for MEESUT
    {
	//============================================================================

	m_pPrepared = new SQLHSTMT[MEESUT_STMT_MAX];
	for (int i=0; i < MEESUT_STMT_MAX; i++)
	{
	    if ( SQLAllocHandle(SQL_HANDLE_STMT, m_Conn, &m_pPrepared[i]) != SQL_SUCCESS)
		ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_DBC, m_Conn);
	}

	//ISO_L1
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_ISO_L1],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_ISO_L1], __FILE__, __LINE__);

	//ISO_L2
#if (defined(ORACLE_ODBC)||defined(PGSQL_ODBC))
	//Oracle and PostgreSQL don't have "REPEATABLE READ" level
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_ISO_L2],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", SQL_NTS);
#else
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_ISO_L2],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", SQL_NTS);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_ISO_L2], __FILE__, __LINE__);

	//ISO_L3
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_ISO_L3],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_ISO_L3], __FILE__, __LINE__);

	//===TradeResult=========================
	//TRF1_1
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF1_1],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(t_ca_id), t_tt_id, t_s_symb, t_qty, t_chrg, t_lifo, t_is_cash FROM trade WHERE t_id = ?",
#else
			(SQLCHAR*)"SELECT t_ca_id, t_tt_id, t_s_symb, t_qty, t_chrg, t_lifo, t_is_cash FROM trade WHERE t_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF1_1], __FILE__, __LINE__);

	//TRF1_2
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF1_2],
			(SQLCHAR*)"SELECT tt_name, tt_is_sell, tt_is_mrkt FROM trade_type WHERE tt_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF1_2], __FILE__, __LINE__);

	//TRF1_3
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF1_3],
			(SQLCHAR*)"SELECT hs_qty FROM holding_summary WHERE hs_ca_id = ? AND hs_s_symb = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF1_3], __FILE__, __LINE__);

	//TRF2_1
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF2_1],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(ca_b_id), TO_CHAR(ca_c_id), ca_tax_st FROM customer_account WHERE ca_id = ?",
#else
			(SQLCHAR*)"SELECT ca_b_id, ca_c_id, ca_tax_st FROM customer_account WHERE ca_id = ?",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF2_1], __FILE__, __LINE__);

	//TRF2_2a
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF2_2a],
			(SQLCHAR*)"INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty) VALUES(?, ?, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF2_2a], __FILE__, __LINE__);

	//TRF2_2b
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF2_2b],
			(SQLCHAR*)"UPDATE holding_summary SET hs_qty = ? WHERE hs_ca_id = ? AND hs_s_symb = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF2_2b], __FILE__, __LINE__);

	//TRF2_3a
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF2_3a],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(h_t_id), h_qty, h_price FROM holding WHERE h_ca_id = ? AND h_s_symb = ? ORDER BY h_dts DESC",
#else
			(SQLCHAR*)"SELECT h_t_id, h_qty, h_price FROM holding WHERE h_ca_id = ? AND h_s_symb = ? ORDER BY h_dts DESC",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF2_3a], __FILE__, __LINE__);

	//TRF2_3b
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF2_3b],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(h_t_id), h_qty, h_price FROM holding WHERE h_ca_id = ? AND h_s_symb = ? ORDER BY h_dts ASC",
#else
			(SQLCHAR*)"SELECT h_t_id, h_qty, h_price FROM holding WHERE h_ca_id = ? AND h_s_symb = ? ORDER BY h_dts ASC",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF2_3b], __FILE__, __LINE__);

	//TRF2_4
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF2_4],
			(SQLCHAR*)"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) VALUES(?, ?, ?, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF2_4], __FILE__, __LINE__);

	//TRF2_5a
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF2_5a],
			(SQLCHAR*)"UPDATE holding SET h_qty = ? WHERE h_t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF2_5a], __FILE__, __LINE__);

	//TRF2_5b
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF2_5b],
			(SQLCHAR*)"DELETE FROM holding WHERE h_t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF2_5b], __FILE__, __LINE__);

	//TRF2_6
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF2_6],
			(SQLCHAR*)"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) VALUES(?, ?, 0, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF2_6], __FILE__, __LINE__);

	//TRF2_7a
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF2_7a],
			(SQLCHAR*)"INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price, h_qty) VALUES(?, ?, ?, ?, ?, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF2_7a], __FILE__, __LINE__);

	//TRF2_7b
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF2_7b],
			(SQLCHAR*)"DELETE FROM holding_summary WHERE hs_ca_id = ? AND hs_s_symb = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF2_7b], __FILE__, __LINE__);

	//TRF3_1
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF3_1],
			(SQLCHAR*)"SELECT sum(tx_rate) FROM taxrate, customer_taxrate WHERE tx_id = cx_tx_id AND cx_c_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF3_1], __FILE__, __LINE__);

	//TRF3_2
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF3_2],
			(SQLCHAR*)"UPDATE trade SET t_tax = ? WHERE t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF3_2], __FILE__, __LINE__);

	//TRF4_1
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF4_1],
			(SQLCHAR*)"SELECT s_ex_id, s_name FROM security WHERE s_symb = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF4_1], __FILE__, __LINE__);

	//TRF4_2
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF4_2],
			(SQLCHAR*)"SELECT c_tier FROM customer WHERE c_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF4_2], __FILE__, __LINE__);

	//TRF4_3
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF4_3],
			(SQLCHAR*)"SELECT cr_rate FROM commission_rate WHERE cr_c_tier = ? AND cr_tt_id = ? AND cr_ex_id = ? AND cr_from_qty <= ? AND cr_to_qty >= ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF4_3], __FILE__, __LINE__);

	//TRF5_1
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF5_1],
			(SQLCHAR*)"UPDATE trade SET t_comm = ?, t_dts = ?, t_st_id = ?, t_trade_price = ? WHERE t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF5_1], __FILE__, __LINE__);

	//TRF5_2
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF5_2],
			(SQLCHAR*)"INSERT INTO trade_history(th_t_id, th_dts, th_st_id) VALUES(?, ?, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF5_2], __FILE__, __LINE__);

	//TRF5_3
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF5_3],
			(SQLCHAR*)"UPDATE broker SET b_comm_total = b_comm_total + ?, b_num_trades = b_num_trades + 1 WHERE b_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF5_3], __FILE__, __LINE__);

	//TRF6_1
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF6_1],
			(SQLCHAR*)"INSERT INTO settlement(se_t_id, se_cash_type, se_cash_due_date, se_amt) VALUES(?, ?, ?, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF6_1], __FILE__, __LINE__);

	//TRF6_2
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF6_2],
			(SQLCHAR*)"UPDATE customer_account SET ca_bal = ca_bal + ? WHERE ca_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF6_2], __FILE__, __LINE__);

	//TRF6_3
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF6_3],
			(SQLCHAR*)"INSERT INTO cash_transaction(ct_dts, ct_t_id, ct_amt, ct_name) VALUES(?, ?, ?, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF6_3], __FILE__, __LINE__);

	//TRF6_4
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_TRF6_4],
			(SQLCHAR*)"SELECT ca_bal FROM customer_account WHERE ca_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_TRF6_4], __FILE__, __LINE__);

	//===MarketFeed==========================

	//MFF1_1
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_MFF1_1],
			(SQLCHAR*)"UPDATE last_trade SET lt_price = ?, lt_vol = lt_vol + ?, lt_dts = ? WHERE lt_s_symb = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_MFF1_1], __FILE__, __LINE__);

	//MFF1_2
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_MFF1_2],
#ifdef ORACLE_ODBC
			(SQLCHAR*)"SELECT TO_CHAR(tr_t_id), tr_bid_price, tr_tt_id, tr_qty FROM trade_request WHERE tr_s_symb = ? AND ((tr_tt_id = ? AND tr_bid_price >= ?) OR (tr_tt_id = ? AND tr_bid_price <= ?) OR (tr_tt_id = ? AND tr_bid_price >= ?))",
#else
			(SQLCHAR*)"SELECT tr_t_id, tr_bid_price, tr_tt_id, tr_qty FROM trade_request WHERE tr_s_symb = ? AND ((tr_tt_id = ? AND tr_bid_price >= ?) OR (tr_tt_id = ? AND tr_bid_price <= ?) OR (tr_tt_id = ? AND tr_bid_price >= ?))",
#endif
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_MFF1_2], __FILE__, __LINE__);

	//MFF1_3
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_MFF1_3],
			(SQLCHAR*)"UPDATE trade SET t_dts = ?, t_st_id = ? WHERE t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_MFF1_3], __FILE__, __LINE__);

	//MFF1_4
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_MFF1_4],
			(SQLCHAR*)"DELETE FROM trade_request WHERE tr_t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_MFF1_4], __FILE__, __LINE__);

	//MFF1_5
	rc = SQLPrepare(m_pPrepared[MEESUT_STMT_MFF1_5],
			(SQLCHAR*)"INSERT INTO trade_history (th_t_id, th_dts, th_st_id) VALUES (?, ?, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_MFF1_5], __FILE__, __LINE__);

    }
    else if (m_PrepareType == 3) //Prapered Statements for DMSUT
    {
	//============================================================================

	m_pPrepared = new SQLHSTMT[DMSUT_STMT_MAX];
	for (int i=0; i < DMSUT_STMT_MAX; i++)
	{
	    if ( SQLAllocHandle(SQL_HANDLE_STMT, m_Conn, &m_pPrepared[i]) != SQL_SUCCESS)
		ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_DBC, m_Conn);
	}

	//ISO_L1
	rc = SQLPrepare(m_pPrepared[DMSUT_STMT_ISO_L1],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_ISO_L1], __FILE__, __LINE__);

	//ISO_L2
#if (defined(ORACLE_ODBC)||defined(PGSQL_ODBC))
	//Oracle and PostgreSQL don't have "REPEATABLE READ" level
	rc = SQLPrepare(m_pPrepared[DMSUT_STMT_ISO_L2],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", SQL_NTS);
#else
	rc = SQLPrepare(m_pPrepared[DMSUT_STMT_ISO_L2],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", SQL_NTS);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_ISO_L2], __FILE__, __LINE__);

	//ISO_L3
	rc = SQLPrepare(m_pPrepared[DMSUT_STMT_ISO_L3],
			(SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[MEESUT_STMT_ISO_L3], __FILE__, __LINE__);

	//===TradeCleanup==========================
	//TCF1_2
	rc = SQLPrepare(m_pPrepared[DMSUT_STMT_TCF1_2],
			(SQLCHAR*)"INSERT INTO trade_history (th_t_id, th_dts, th_st_id) VALUES (?, ?, ?)",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[DMSUT_STMT_TCF1_2], __FILE__, __LINE__);

	//TCF1_5
	rc = SQLPrepare(m_pPrepared[DMSUT_STMT_TCF1_5],
			(SQLCHAR*)"UPDATE trade SET t_st_id = ?, t_dts = ? WHERE t_id = ?",
			SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_pPrepared[DMSUT_STMT_TCF1_5], __FILE__, __LINE__);

	//TCF1_6 == TCF1_2

	//===DataMaintenance=======================
    }
#endif //USE_PREPARE
}

CDBConnection::~CDBConnection()
{
#ifdef DEBUG
    cout<<"CDBConnection::~CDBConnection"<<endl;
#endif

#ifdef USE_PREPARE
    if (m_PrepareType == 1) //Prapered Statements for CESUT
    {
	for (int i=0; i < CESUT_STMT_MAX; i++)
	{
	    SQLFreeHandle(SQL_HANDLE_STMT, m_pPrepared[i]);
	}
	delete m_pPrepared;
	m_pPrepared = NULL;
    }
    else if (m_PrepareType == 2) //Prapered Statements for MEESUT
    {
	for (int i=0; i < MEESUT_STMT_MAX; i++)
	{
	    SQLFreeHandle(SQL_HANDLE_STMT, m_pPrepared[i]);
	}
	delete m_pPrepared;
	m_pPrepared = NULL;
    }
#endif

    SQLFreeHandle(SQL_HANDLE_STMT, m_Stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, m_Stmt2);

    SQLEndTran(SQL_HANDLE_DBC, m_Conn, SQL_ROLLBACK);
    SQLDisconnect(m_Conn);
    SQLFreeHandle(SQL_HANDLE_DBC, m_Conn);
}

void CDBConnection::BeginTxn()
{
}

void CDBConnection::CommitTxn()
{
    SQLRETURN rc;

    rc = SQLEndTran(SQL_HANDLE_DBC, m_Conn, SQL_COMMIT);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eEndTran);
}

void CDBConnection::RollbackTxn()
{
    SQLEndTran(SQL_HANDLE_DBC, m_Conn, SQL_ROLLBACK);
}
void CDBConnection::ThrowError( CODBCERR::ACTION eAction, SQLSMALLINT HandleType, SQLHANDLE Handle,
				const char* FileName, unsigned int Line)
{
    RETCODE     rc;
    SQLINTEGER  lNativeError = 0;
    char        szState[6];
    char        szMsg[SQL_MAX_MESSAGE_LENGTH];
    char        szTmp[6*SQL_MAX_MESSAGE_LENGTH];
    char        szBuff[SQL_MAX_MESSAGE_LENGTH];
    CODBCERR   *pODBCErr;           // not allocated until needed (maybe never)

    pODBCErr = new CODBCERR();

    pODBCErr->m_NativeError = 0;
    pODBCErr->m_eAction = eAction;
    pODBCErr->m_bDeadLock = false;

    if (Handle == SQL_NULL_HANDLE)
    {
        switch (eAction)
        {
	    case CODBCERR::eSetEnvAttr:
		HandleType = SQL_HANDLE_ENV;
		Handle = m_Env;
		break;

	    case CODBCERR::eBcpBind:
	    case CODBCERR::eBcpControl:
	    case CODBCERR::eBcpBatch:
	    case CODBCERR::eBcpDone:
	    case CODBCERR::eBcpInit:
	    case CODBCERR::eBcpSendrow:
	    case CODBCERR::eConnect:
	    case CODBCERR::eSetConnectAttr:
	    case CODBCERR::eEndTran:
		HandleType = SQL_HANDLE_DBC;
		Handle = m_Conn;
		break;

	    case CODBCERR::eBindCol:
	    case CODBCERR::eCloseCursor:
	    case CODBCERR::ePrepare:
	    case CODBCERR::eExecute:
	    case CODBCERR::eExecDirect:
	    case CODBCERR::eFetch:
		HandleType = SQL_HANDLE_STMT;
		Handle = m_Stmt;
		break;
	    default:
		HandleType = SQL_HANDLE_DBC;
		Handle = m_Conn;
        }
    }
    szTmp[0] = 0;

    if(FileName)
    {
	sprintf(szTmp, "[%s:%d] ", FileName, Line);
    }

    int i = 0;
    while (1 && !(pODBCErr->m_bDeadLock))
    {

        rc = SQLGetDiagRec( HandleType, Handle, ++i, (BYTE *)&szState, &lNativeError,
			    (BYTE *)&szMsg, sizeof(szMsg), NULL);
        if (rc == SQL_NO_DATA)
            break;

        // check for deadlock
        if (
#ifdef MYSQL_ODBC
	    lNativeError == 1213 ||
#elif ORACLE_ODBC
#elif PGSQL_ODBC
	    strcmp(szState, "40P01") == 0 ||
#endif
	    strcmp(szState, "40001") == 0)
            pODBCErr->m_bDeadLock = true;

        // capture the (first) database error
        if (pODBCErr->m_NativeError == 0 && lNativeError != 0)
            pODBCErr->m_NativeError = lNativeError;

        // quit if there isn't enough room to concatenate error text
        if ( (strlen(szMsg) + 2) > (sizeof(szTmp) - strlen(szTmp)) )
            break;

        // include line break after first error msg
        if (i != 1)
            strcat( szTmp, "\n");

        sprintf(szBuff, "Native=%d, SQLState=%s : ", (int)lNativeError, szState);
	strcat(szTmp, szBuff);
	if(pODBCErr->m_bDeadLock)
	    strcat(szTmp, "[DEADLOCK]");
	else
	    strcat(szTmp, szMsg);
    }

    if (pODBCErr->m_odbcerrstr != NULL)
    {
        delete [] pODBCErr->m_odbcerrstr;
        pODBCErr->m_odbcerrstr = NULL;
    }

    if (szTmp[0] != 0)
    {
        pODBCErr->m_odbcerrstr = new char[ strlen(szTmp)+1 ];
        strcpy( pODBCErr->m_odbcerrstr, szTmp );
    }

    if(HandleType == SQL_HANDLE_STMT)
	SQLCloseCursor(Handle);

    throw pODBCErr;
}
