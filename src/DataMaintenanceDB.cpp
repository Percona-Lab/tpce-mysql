// DataMaintenanceDB.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

CDataMaintenanceDB::CDataMaintenanceDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CDataMaintenanceDB::~CDataMaintenanceDB()
{
}

void CDataMaintenanceDB::DoDataMaintenanceFrame1(
    const TDataMaintenanceFrame1Input *pIn,
    TDataMaintenanceFrame1Output *pOut)
{
#ifdef DEBUG
    cout<<"CDataMaintenanceDB::DoDataMaintenanceFrame1"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;
    char  now_dts[TIMESTAMP_LEN+1];

    int   rowcount;
    char  acl[4+1];

    char  line2[80+1];
    INT64 addr_id;

    char  sprate[4+1];

    char  email2[50+1+15]; /* 15 == strlen("@mindspring.com") */
    int   len;
    int   lenMindspring;

    char  old_tax_rate[4+1];
    char  new_tax_rate[4+1];

    char  tax_name[50+1];
    char  *pt;

    char  old_symbol[15+1];
    char  new_symbol[15+1];

    // Isolation level required by Clause 7.4.1.3
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

    if (strcmp(pIn->table_name, "ACCOUNT_PERMISSION") == 0)
    {
	acl[0] = 0; /* "" */

	/*	SELECT ap_acl
		FROM account_permission
		WHERE ap_ca_id = %ld
		ORDER BY ap_acl DESC LIMIT 1 */

	ostringstream osDMF1_1;
#ifdef ORACLE_ODBC
	osDMF1_1 << "SELECT * FROM (SELECT ap_acl FROM account_permission WHERE ap_ca_id = " <<
	    pIn->acct_id << " ORDER BY ap_acl DESC) WHERE ROWNUM <= 1";
#else
	osDMF1_1 << "SELECT ap_acl FROM account_permission WHERE ap_ca_id = " <<
	    pIn->acct_id << " ORDER BY ap_acl DESC LIMIT 1";
#endif
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_1.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLBindCol(m_Stmt, 1, SQL_C_CHAR, acl, 4+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);


	/*	UPDATE account_permission
		SET ap_acl = '%s'
		WHERE ap_ca_id = %ld AND ap_acl = '%s' */

	ostringstream osDMF1_2;
	osDMF1_2 << "UPDATE account_permission SET ap_acl = '";
	if (strcmp(acl, "1111") != 0)
	{
	    osDMF1_2 << "1111";
	}
	else
	{
	    osDMF1_2 << "0011";
	}
	osDMF1_2 << "' WHERE ap_ca_id = " <<
	    pIn->acct_id << " AND ap_acl = '" <<
	    acl << "'";
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_2.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
    }
    else if (strcmp(pIn->table_name, "ADDRESS") == 0)
    {
	addr_id = 0;
	line2[0] = 0; /* "" */

#ifdef ORACLE_ODBC
	char num_str[30];
#endif

	/*	SELECT ad_line2, ad_id
		FROM address, customer
		WHERE ad_id = c_ad_id
		AND c_id = %ld */
	/*	SELECT ad_line2, ad_id
		FROM address, company
		WHERE ad_id = co_ad_id
		AND co_id = %ld */

	ostringstream osDMF1_3;
#ifdef ORACLE_ODBC
	osDMF1_3 << "SELECT ad_line2, TO_CHAR(ad_id) FROM address, ";
#else
	osDMF1_3 << "SELECT ad_line2, ad_id FROM address, ";
#endif
	if (pIn->c_id != 0) /* customer */
	{
	    osDMF1_3 << "customer WHERE ad_id = c_ad_id AND c_id = " <<
		pIn->c_id;
	}
	else /* company */
	{
	    osDMF1_3 << "company WHERE ad_id = co_ad_id AND co_id = " <<
		pIn->co_id;
	}
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_3.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLBindCol(m_Stmt, 1, SQL_C_CHAR, line2, 80+1, &m_DummyInd);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
	rc = SQLBindCol(m_Stmt, 2, SQL_C_CHAR, num_str, 30, NULL);
#else
	rc = SQLBindCol(m_Stmt, 2, SQL_C_SBIGINT, &addr_id, 0, NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
#ifdef ORACLE_ODBC
	addr_id = atoll(num_str);
#endif

	/*	UPDATE address
		SET ad_line2 = '%s'
		WHERE ad_id = %ld */

	ostringstream osDMF1_4;
	osDMF1_4 << "UPDATE address SET ad_line2 = '";
	if (strcmp(line2, "Apt. 10C") != 0)
	{
	    osDMF1_4 << "Apt. 10C";
	}
	else
	{
	    osDMF1_4 << "Apt. 22";
	}
	osDMF1_4 << "' WHERE ad_id = " << addr_id;
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_4.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
    }
    else if (strcmp(pIn->table_name, "COMPANY") == 0)
    {
	sprate[0] = 0; /* "" */

	/*	SELECT co_sp_rate
		FROM company
		WHERE co_id = %ld */

	ostringstream osDMF1_5;
	osDMF1_5 << "SELECT co_sp_rate FROM company WHERE co_id = " << pIn->co_id;
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_5.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLBindCol(m_Stmt, 1, SQL_C_CHAR, sprate, 4+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);


	/*	UPDATE company
		SET co_sp_rate = '%s'
		WHERE co_id = %ld */

	ostringstream osDMF1_6;
	osDMF1_6 << "UPDATE company SET co_sp_rate = '";
	if (strcmp(sprate, "ABA") != 0)
	{
	    osDMF1_6 << "ABA";
	}
	else
	{
	    osDMF1_6 << "AAA";
	}
	osDMF1_6 << "' WHERE co_id = " << pIn->co_id;
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_6.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
    }
    else if (strcmp(pIn->table_name, "CUSTOMER") == 0)
    {
	email2[0] = 0; /* "" */
	len = 0;
	lenMindspring = strlen("@mindspring.com");

	/*	SELECT c_email_2
		FROM customer
		WHERE c_id = %ld */

	ostringstream osDMF1_7;
	osDMF1_7 << "SELECT c_email_2 FROM customer WHERE c_id = " << pIn->c_id;
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_7.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLBindCol(m_Stmt, 1, SQL_C_CHAR, email2, 50+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);


	len = strlen(email2);

	if ((len - lenMindspring) > 0
	    && (strstr(email2, "@mindspring.com") - email2) == (len - lenMindspring))
	{
	    strcpy(rindex(email2,(int)'@'),"@earthlink.com");
	}
	else
	{
	    char *p_index;
	    p_index = rindex(email2,(int)'@');
	    if (p_index) { /* email2 must contain '@' */
		strcpy(p_index,"@mindspring.com");
	    } else {
		strcpy(&email2[len],"@mindspring.com"); /* cat */
	    }
	}


	/*	UPDATE customer
		SET c_email_2 = '%s'
		WHERE c_id = %ld */

	ostringstream osDMF1_8;
	osDMF1_8 << "UPDATE customer SET c_email_2 = '" << email2 <<
	    "' WHERE c_id = " << pIn->c_id;
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_8.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
    }
    else if (strcmp(pIn->table_name, "CUSTOMER_TAXRATE") == 0)
    {
	/*      SELECT old_tax_rate=cx_tx_id
                FROM customer_taxrate
                WHERE cx_c_id = %ld
		AND (cx_tx_id LIKE 'US%' OR cx_tx_id LIKE 'CN%') */

	ostringstream osDMF1_9;
	osDMF1_9 << "SELECT cx_tx_id FROM customer_taxrate WHERE cx_c_id = " <<
	    pIn->c_id << " AND (cx_tx_id LIKE 'US%' OR cx_tx_id LIKE 'CN%')";
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_9.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLBindCol(m_Stmt, 1, SQL_C_CHAR, old_tax_rate, 4+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);

	if(strncmp(old_tax_rate, "US", 2) == 0)
	{
	    if (old_tax_rate[2] == '5')
		strcpy(new_tax_rate, "US1");
	    else
		sprintf(new_tax_rate, "US%c", old_tax_rate[2] + 1);
	}
	else
	{
	    if (old_tax_rate[2] == '4')
		strcpy(new_tax_rate, "CN1");
	    else
		sprintf(new_tax_rate, "CN%c", old_tax_rate[2] + 1);
	}

	/*      UPDATE customer_taxrate
                SET cx_tx_id = '%s'
                WHERE cx_c_id = %ld
		AND cx_tx_id = '%s' */

	ostringstream osDMF1_10;
	osDMF1_10 << "UPDATE customer_taxrate SET cx_tx_id = '" << new_tax_rate <<
	    "' WHERE cx_c_id = " << pIn->c_id << " AND cx_tx_id = '" << old_tax_rate <<
	    "'";

	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_10.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
    }
    else if (strcmp(pIn->table_name, "DAILY_MARKET") == 0)
    {
	/*	UPDATE daily_market
		SET dm_vol = dm_vol + %d
		WHERE dm_s_symb = '%s'
		AND EXTRACT(DAY FROM dm_date) = %d */

	ostringstream osDMF1_11;
	osDMF1_11 << "UPDATE daily_market SET dm_vol = dm_vol + " << pIn->vol_incr <<
	    " WHERE dm_s_symb = '" << pIn->symbol <<
	    "' AND EXTRACT(DAY FROM dm_date) = " << pIn->day_of_month;
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_11.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
    }
    else if (strcmp(pIn->table_name, "EXCHANGE") == 0)
    {
	rowcount = 0;
	gettimestamp(now_dts, STRFTIME_FORMAT, TIMESTAMP_LEN);

	/*	SELECT COUNT(*)
		FROM exchange
		WHERE ex_desc LIKE '%LAST UPDATED%' */

	ostringstream osDMF1_12;
	osDMF1_12 << "SELECT COUNT(ex_id) FROM exchange WHERE ex_desc LIKE '%LAST UPDATED%'";
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_12.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLBindCol(m_Stmt, 1, SQL_C_LONG, &rowcount, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);


	ostringstream osDMF1_13;
	if (rowcount == 0)
	{
	    /*	UPDATE exchange
		SET ex_desc = ex_desc || ' LAST UPDATED ' || now() */
#ifdef PGSQL_ODBC
	    osDMF1_13 << "UPDATE exchange SET ex_desc = ex_desc || ' LAST UPDATED " <<
		now_dts << "'";
#else
	    osDMF1_13 << "UPDATE exchange SET ex_desc = CONCAT(ex_desc, ' LAST UPDATED " <<
		now_dts << "')";
#endif
	}
	else
	{
	    int now_dts_len = strlen(now_dts);

	    /*	UPDATE exchange
		SET ex_desc = SUBSTRING(ex_desc || ' LAST UPDATED ' || NOW()
		FROM 1 FOR (CHAR_LENGTH(ex_desc) -
		CHAR_LENGTH(NOW()))) || NOW() */
#ifdef MYSQL_ODBC
	    osDMF1_13 << "UPDATE exchange SET ex_desc = INSERT(ex_desc, LENGTH(ex_desc) - " <<
		(now_dts_len - 1) << ", " << now_dts_len << ",'" << now_dts << "')";
#elif PGSQL_ODBC
	    osDMF1_13 << "UPDATE exchange SET ex_desc = SUBSTRING(ex_desc FROM 1 FOR (CHAR_LENGTH(ex_desc) - " <<
		now_dts_len << ")) || '" << now_dts << "'";
#elif ORACLE_ODBC
	    osDMF1_13 << "UPDATE exchange SET ex_desc = CONCAT(SUBSTR(ex_desc, 1, LENGTH(ex_desc) - " <<
		now_dts_len << "), '" << now_dts << "')";
#endif
	}
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_13.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
    }
    else if (strcmp(pIn->table_name, "FINANCIAL") == 0)
    {
	rowcount = 0;

	/*	SELECT COUNT(*)
		FROM financial
		WHERE fi_co_id = %ld
		AND EXTRACT(DAY FROM fi_qtr_start_date) = 1 */

	ostringstream osDMF1_14;
	osDMF1_14 << "SELECT COUNT(fi_co_id) FROM financial WHERE fi_co_id = " << pIn->co_id <<
	    " AND EXTRACT(DAY FROM fi_qtr_start_date) = 1";
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_14.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLBindCol(m_Stmt, 1, SQL_C_LONG, &rowcount, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);


	/*	UPDATE financial
		SET fi_qtr_start_date = fi_qtr_start_date {+,-} INTERVAL '1' day
		WHERE fi_co_id = %ld */

	ostringstream osDMF1_15;
	osDMF1_15 << "UPDATE financial SET fi_qtr_start_date = fi_qtr_start_date ";
	if (rowcount > 0)
	{
	    osDMF1_15 << "+";
	}
	else
	{
	    osDMF1_15 << "-";
	}
#ifdef PGSQL_ODBC
	osDMF1_15 << " INTERVAL '1 day' WHERE fi_co_id = " << pIn->co_id;
#else
	osDMF1_15 << " INTERVAL '1' day WHERE fi_co_id = " << pIn->co_id;
#endif
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_15.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
    }
    else if (strcmp(pIn->table_name, "NEWS_ITEM") == 0)
    {
	/*	UPDATE  NEWS_ITEM
		SET     NI_DTS = NI_DTS + interval '1' day
		WHERE   NI_ID IN (SELECT NX_NI_ID
		FROM    NEWS_XREF
		WHERE   NX_CO_ID = comp_id); */
	// OR
	/*	UPDATE news_item, news_xref
		SET ni_dts = ni_dts + INTERVAL '1' day
		WHERE ni_id = nx_ni_id
		AND nx_co_id = comp_id */

	ostringstream osDMF1_16;
#ifdef MYSQL_ODBC
	osDMF1_16 << "UPDATE news_item, news_xref SET ni_dts = ni_dts + INTERVAL '1' day WHERE ni_id = nx_ni_id AND nx_co_id = " <<
	    pIn->co_id;
#elif PGSQL_ODBC
	osDMF1_16 << "UPDATE news_item SET ni_dts = ni_dts + INTERVAL '1 day' WHERE ni_id IN (SELECT nx_ni_id FROM news_xref WHERE nx_co_id = " <<
	    pIn->co_id << ")";
#elif ORACLE_ODBC
	osDMF1_16 << "UPDATE news_item SET ni_dts = ni_dts + INTERVAL '1' day WHERE ni_id IN (SELECT nx_ni_id FROM news_xref WHERE nx_co_id = " <<
	    pIn->co_id << ")";
#endif
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_16.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
    }
    else if (strcmp(pIn->table_name, "SECURITY") == 0)
    {
	/*	UPDATE security
		SET s_exch_date = s_exch_date + INTERVAL '1' day
		WHERE s_symb = '%s' */

	ostringstream osDMF1_17;
#ifdef PGSQL_ODBC
	osDMF1_17 << "UPDATE security SET s_exch_date = s_exch_date + INTERVAL '1 day' WHERE s_symb = '" <<
	    pIn->symbol << "'";
#else
	osDMF1_17 << "UPDATE security SET s_exch_date = s_exch_date + INTERVAL '1' day WHERE s_symb = '" <<
	    pIn->symbol << "'";
#endif
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_17.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
    }
    else if (strcmp(pIn->table_name, "TAXRATE") == 0)
    {
	tax_name[0] = 0; /* "" */

	/*	SELECT tx_name
		FROM taxrate
		WHERE tx_id = '%s' */

	ostringstream osDMF1_18;
	osDMF1_18 << "SELECT tx_name FROM taxrate WHERE tx_id = '" << pIn->tx_id <<
	    "'";
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_18.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLBindCol(m_Stmt, 1, SQL_C_CHAR, tax_name, 50+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);


	if ((pt = strstr(tax_name, " Tax ")) > (char *) 0)
	    pt[1] = 't';
	else if ((pt = strstr(tax_name, " tax ")) > (char *) 0)
	    pt[1] = 'T';


	/*      UPDATE taxrate
                SET tx_name = '%s'
                WHERE tx_id = '%s' */

	ostringstream osDMF1_19;
	osDMF1_19 << "UPDATE taxrate SET tx_name = '" << tax_name <<
	    "' WHERE tx_id = '" << pIn->tx_id << "'";

	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_19.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);
    }
    else if (strcmp(pIn->table_name, "WATCH_ITEM") == 0)
    {
	rowcount = 0;
	old_symbol[0] = new_symbol[0] = 0; /* "" */

	/*      SELECT count(*)
                FROM watch_item, watch_list
                WHERE wl_c_id = %ld
		AND wi_wl_id = wl_id */

	ostringstream osDMF1_20;
	osDMF1_20 << "SELECT count(wi_wl_id) FROM watch_item, watch_list WHERE wl_c_id = " <<
	    pIn->c_id << " AND wi_wl_id = wl_id";
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_20.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLBindCol(m_Stmt, 1, SQL_C_LONG, &rowcount, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);


	rowcount = (rowcount + 1) / 2; //This is ROWNUM!!


	/*      SELECT wi_s_symb
		FROM watch_item, watch_list
		WHERE wl_c_id = %ld
		AND wi_wl_id = wl_id
		ORDER BY wi_s_symb ASC LIMIT 1 OFFSET %d */

	ostringstream osDMF1_21;
#ifdef ORACLE_ODBC
	osDMF1_21 << "SELECT wi_s_symb FROM (SELECT wi_s_symb, ROW_NUMBER() OVER (ORDER BY wi_s_symb ASC) AS rn FROM watch_item, watch_list WHERE wl_c_id = " << pIn->c_id <<
	    " AND wi_wl_id = wl_id ) WHERE rn = " << rowcount;
#else
	osDMF1_21 << "SELECT wi_s_symb FROM watch_item, watch_list WHERE wl_c_id = " << pIn->c_id <<
	    " AND wi_wl_id = wl_id ORDER BY wi_s_symb ASC LIMIT 1 OFFSET " << (rowcount - 1);
#endif
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_21.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLBindCol(m_Stmt, 1, SQL_C_CHAR, old_symbol, 15+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);


	/*      SELECT s_symb
                FROM security
                WHERE s_symb > '%s'
                  AND s_symb NOT IN (SELECT wi_s_symb
                                     FROM watch_item, watch_list
                                     WHERE wl_c_id = %ld
                                       AND wi_wl_id = wl_id)
                ORDER BY s_symb ASC
                LIMIT 1 */

	ostringstream osDMF1_22;
#ifdef ORACLE_ODBC
	osDMF1_22 << "SELECT * FROM (SELECT s_symb FROM security WHERE s_symb > '" << old_symbol <<
	    "' AND s_symb NOT IN (SELECT wi_s_symb FROM watch_item, watch_list WHERE wl_c_id = " <<
	    pIn->c_id << " AND wi_wl_id = wl_id) ORDER BY s_symb ASC) WHERE ROWNUM <= 1";
#else
	osDMF1_22 << "SELECT s_symb FROM security WHERE s_symb > '" << old_symbol <<
	    "' AND s_symb NOT IN (SELECT wi_s_symb FROM watch_item, watch_list WHERE wl_c_id = " <<
	    pIn->c_id << " AND wi_wl_id = wl_id) ORDER BY s_symb ASC LIMIT 1";
#endif
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_22.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLBindCol(m_Stmt, 1, SQL_C_CHAR, new_symbol, 15+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	SQLCloseCursor(m_Stmt);


	/*      UPDATE watch_item
                SET wi_s_symb = '%s'
		WHERE wi_s_symb = '%s'
		  AND wi_wl_id IN (SELECT wl_id FROM watch_list
		                   WHERE wl_c_id = %ld) */
	//OR
	/*      UPDATE watch_item, watch_list
		SET wi_s_symb = '%s'
		WHERE wi_s_symb = '%s'
		AND wi_wl_id = wl_id
		AND wl_c_id = %ld */

	ostringstream osDMF1_23;
#ifdef MYSQL_ODBC
	osDMF1_23 << "UPDATE watch_item, watch_list SET wi_s_symb = '" << new_symbol <<
	    "' WHERE wi_s_symb = '" << old_symbol <<
	    "' AND wi_wl_id = wl_id AND wl_c_id = " << pIn->c_id;
#else
	osDMF1_23 << "UPDATE watch_item SET wi_s_symb = '" << new_symbol <<
	    "' WHERE wi_s_symb = '" << old_symbol <<
	    "' AND wi_wl_id IN (SELECT wl_id FROM watch_list WHERE wl_c_id = " << pIn->c_id << ")";
#endif
	rc = SQLExecDirect(m_Stmt, (SQLCHAR*)(osDMF1_23.str().c_str()), SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, m_Stmt, __FILE__, __LINE__);
	rc = SQLFetch(m_Stmt);
    }

    CommitTxn();

    pOut->status = CBaseTxnErr::SUCCESS;
}
