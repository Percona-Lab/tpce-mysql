// DMSUT.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

extern eGlobalState GlobalState;
extern CRtHistogram g_RtHistogram[TRADE_CLEANUP];

CDMSUT::CDMSUT(const char *szHost, const char *szDBName,
               const char *szDBUser, const char *szDBPass)
{
    m_pDBConnection = new CDBConnection(szHost, szDBName,
					szDBUser, szDBPass, 3);
    for(int i=0; i<4; i++)
	m_CountDataMaintenance[i]=0;

    clk_tck = sysconf(_SC_CLK_TCK);
}

CDMSUT::~CDMSUT()
{
    delete m_pDBConnection;
}

bool CDMSUT::DataMaintenance( PDataMaintenanceTxnInput pTxnInput )
{
    CDataMaintenanceDB DataMaintenanceDB(m_pDBConnection);
    CDataMaintenance DataMaintenance(&DataMaintenanceDB);
    TDataMaintenanceTxnOutput TxnOutput;

    clock_t clk1,clk2;
    struct tms tbuf;

    for (int i = 0; i < MAX_RETRY; i++)
    {
#ifdef PRINT_DEADLOCK
	if (i)
	    cout << "  Retry DM:" << i << endl;
#endif

	try
	{
	    clk1 = times( &tbuf );
	    DataMaintenance.DoTxn( pTxnInput, &TxnOutput );
	    clk2 = times( &tbuf );
	}
	catch (const CODBCERR* e)
	{
	    m_pDBConnection->RollbackTxn();

	    if (e->m_bDeadLock)
	    {
#ifdef PRINT_DEADLOCK
		cout << "  " << e->what() << endl;
#endif
		delete e;
		if(GlobalState == MEASURING)
		    ++m_CountDataMaintenance[2]; //Retried
		continue;
	    }
	    else
	    {
		cout << "ODBCERR at CDMSUT::DataMaintenance()" << endl << "  " << e->what() << endl;
		delete e;
		break;
	    }
	}

	if(GlobalState == MEASURING)
	{
	    if((clk2 - clk1) / clk_tck < 2) //Not Defined ?
		++m_CountDataMaintenance[0]; //Succeed
	    else
		++m_CountDataMaintenance[1]; //Lated

	    g_RtHistogram[DATA_MAINTENANCE].Put(clk2 - clk1);
	}

	return( TxnOutput.status==CBaseTxnErr::SUCCESS );
    }

    if(GlobalState == MEASURING)
	++m_CountDataMaintenance[3]; //Failed

    cout << "CDMSUT::DataMaintenance() failed." << endl;
    return(false);
}

bool CDMSUT::TradeCleanup( PTradeCleanupTxnInput pTxnInput )
{
    CTradeCleanupDB TradeCleanupDB(m_pDBConnection);
    CTradeCleanup TradeCleanup(&TradeCleanupDB);
    TTradeCleanupTxnOutput TxnOutput;

    for (int i = 0; i < MAX_RETRY; i++)
    {
#ifdef PRINT_DEADLOCK
	if (i)
	    cout << "  Retry TC:" << i << endl;
#endif

	try
	{
	    TradeCleanup.DoTxn( pTxnInput, &TxnOutput );
	}
	catch (const CODBCERR* e)
	{
	    m_pDBConnection->RollbackTxn();

	    if (e->m_bDeadLock)
	    {
#ifdef PRINT_DEADLOCK
		cout << "  " << e->what() << endl;
#endif
		delete e;
		continue; //Retry
	    }
	    else
	    {
		cout << "ODBCERR at CDMSUT::TradeCleanup()" << endl << "  " << e->what() << endl;
		delete e;
		break; //Fail
	    }
	}

	return( TxnOutput.status==CBaseTxnErr::SUCCESS );
    }

    cout << "CDMSUT::TradeCleanup() failed." << endl;
    return(false);
}
