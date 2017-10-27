// BrokerVolumeDB.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

CBrokerVolumeDB::CBrokerVolumeDB(CDBConnection *pDBConn)
    : CTxnDBBase(pDBConn)
{
}

CBrokerVolumeDB::~CBrokerVolumeDB()
{
}

void CBrokerVolumeDB::DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn,
					   TBrokerVolumeFrame1Output *pOut)
{
#ifdef DEBUG
    cout<<"CBrokerVolumeDB::DoBrokerVolumeFrame1"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;

    char   broker_name[cB_NAME_len+1];
    double volume;

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


    /* SELECT b_name, SUM(tr_qty * tr_bid_price)
       FROM trade_request, sector, industry, company, broker, security
       WHERE tr_b_id = b_id
         AND tr_s_symb = s_symb
         AND s_co_id = co_id
         AND co_in_id = in_id
         AND sc_id = in_sc_id
         AND b_name IN (%s..)
         AND sc_name = '%s'
       GROUP BY b_name
       ORDER BY 2 DESC */

    stmt = m_Stmt;
    ostringstream osBVF1_1;
#ifdef ORACLE_ODBC
    osBVF1_1 << "SELECT /*+ USE_NL(trade_request sector industry company broker security) */ b_name, SUM(tr_qty * tr_bid_price) price_sum " <<
	"FROM trade_request, sector, industry, company, broker, security " <<
	"WHERE tr_b_id = b_id AND tr_s_symb = s_symb AND s_co_id = co_id AND co_in_id = in_id " <<
	"AND sc_id = in_sc_id AND b_name IN ('";
#else
    osBVF1_1 << "SELECT b_name, SUM(tr_qty * tr_bid_price) AS price_sum " <<
	"FROM trade_request, sector, industry, company, broker, security " <<
	"WHERE tr_b_id = b_id AND tr_s_symb = s_symb AND s_co_id = co_id AND co_in_id = in_id " <<
	"AND sc_id = in_sc_id AND b_name IN ('";
#endif
    for(int i = 0; i < max_broker_list_len; ++i)
    {
	if(pIn->broker_list[i][0] == 0)
	    break;

	if(i)
	    osBVF1_1 << "', '";

	osBVF1_1 << pIn->broker_list[i];
    }
    osBVF1_1 << "') AND sc_name = '" << pIn->sector_name <<
	"' GROUP BY b_name ORDER BY price_sum DESC";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osBVF1_1.str().c_str()), SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, broker_name, cB_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_DOUBLE, &volume, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    int i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < max_broker_list_len)
    {
	strncpy(pOut->broker_name[i], broker_name, cB_NAME_len+1);
	pOut->volume[i] = volume;
	i++;

	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

    CommitTxn();

    pOut->list_len = i;
    pOut->status = CBaseTxnErr::SUCCESS;
}
