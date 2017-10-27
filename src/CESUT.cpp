// CESUT.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

extern eGlobalState GlobalState;
extern CRtHistogram g_RtHistogram[TRADE_CLEANUP];

CCESUT::CCESUT(const char *szHost, const char *szDBName,
	       const char *szDBUser, const char *szDBPass)
{
    m_pDBConnection = new CDBConnection(szHost, szDBName,
					szDBUser, szDBPass, 1);

    for(int i=0; i<4; i++)
    {
        m_CountBrokerVolume[i]=0;
        m_CountCustomerPosition[i]=0;
        m_CountMarketWatch[i]=0;
        m_CountSecurityDetail[i]=0;
        m_CountTradeLookup[i]=0;
        m_CountTradeOrder[i]=0;
        m_CountTradeStatus[i]=0;
        m_CountTradeUpdate[i]=0;
    }
    clk_tck = sysconf(_SC_CLK_TCK);
}

CCESUT::~CCESUT()
{
    delete m_pDBConnection;
}

bool CCESUT::BrokerVolume( PBrokerVolumeTxnInput pTxnInput )
{
    CBrokerVolumeDB BrokerVolumeDB(m_pDBConnection);
    CBrokerVolume BrokerVolume(&BrokerVolumeDB);
    TBrokerVolumeTxnOutput TxnOutput;

    clock_t clk1,clk2;
    struct tms tbuf;

    for (int i = 0; i < MAX_RETRY; i++)
    {
#ifdef PRINT_DEADLOCK
	if (i)
	    cout << "  Retry BV:" << i << endl;
#endif

	try
	{
	    clk1 = times( &tbuf );
	    BrokerVolume.DoTxn( pTxnInput, &TxnOutput );
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
		    ++m_CountBrokerVolume[2]; //Retried
		continue;
	    }
	    else
	    {
		cout << "ODBCERR at CCESUT::BrokerVolume()" << endl << "  " << e->what() << endl;
		delete e;
		break;
	    }
	}

	if(GlobalState == MEASURING)
	{
	    if((clk2 - clk1) / clk_tck < 3)
		++m_CountBrokerVolume[0]; //Succeed
	    else
		++m_CountBrokerVolume[1]; //Lated

	    g_RtHistogram[BROKER_VOLUME].Put(clk2 - clk1);
	}

	return( TxnOutput.status==CBaseTxnErr::SUCCESS );
    }

    if(GlobalState == MEASURING)
	++m_CountBrokerVolume[3]; //Failed

    cout << "CCESUT::BrokerVolume() failed." << endl;
    return(false);
}

bool CCESUT::CustomerPosition( PCustomerPositionTxnInput pTxnInput )
{
    CCustomerPositionDB CustomerPositionDB(m_pDBConnection);
    CCustomerPosition CustomerPosition(&CustomerPositionDB);
    TCustomerPositionTxnOutput TxnOutput;

    clock_t clk1,clk2;
    struct tms tbuf;

    for (int i = 0; i < MAX_RETRY; i++)
    {
#ifdef PRINT_DEADLOCK
	if (i)
	    cout << "  Retry CP:" << i << endl;
#endif

	try
	{
	    clk1 = times( &tbuf );
	    CustomerPosition.DoTxn( pTxnInput, &TxnOutput );
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
		    ++m_CountCustomerPosition[2]; //Retried
		continue;
	    }
	    else
	    {
		cout << "ODBCERR at CCESUT::CustomerPosition()" << endl << "  " << e->what() << endl;
		delete e;
		break;
	    }
	}

	if(GlobalState == MEASURING)
	{
	    if((clk2 - clk1) / clk_tck < 3)
		++m_CountCustomerPosition[0]; //Succeed
	    else
		++m_CountCustomerPosition[1]; //Lated

	    g_RtHistogram[CUSTOMER_POSITION].Put(clk2 - clk1);
	}

	return( TxnOutput.status==CBaseTxnErr::SUCCESS );
    }

    if(GlobalState == MEASURING)
	++m_CountCustomerPosition[3]; //Failed

    cout << "CCESUT::CustomerPosition() failed." << endl;
    return(false);
}

bool CCESUT::MarketWatch( PMarketWatchTxnInput pTxnInput )
{
    CMarketWatchDB MarketWatchDB(m_pDBConnection);
    CMarketWatch MarketWatch(&MarketWatchDB);
    TMarketWatchTxnOutput TxnOutput;

    clock_t clk1,clk2;
    struct tms tbuf;

    for (int i = 0; i < MAX_RETRY; i++)
    {
#ifdef PRINT_DEADLOCK
	if (i)
	    cout << "  Retry MW:" << i << endl;
#endif

	try
	{
	    clk1 = times( &tbuf );
	    MarketWatch.DoTxn( pTxnInput, &TxnOutput );
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
		    ++m_CountMarketWatch[2]; //Retried
		continue;
	    }
	    else
	    {
		cout << "ODBCERR at CCESUT::MarketWatch()" << endl << "  " << e->what() << endl;
		delete e;
		break;
	    }
	}

	if(GlobalState == MEASURING)
	{
	    if((clk2 - clk1) / clk_tck < 3)
		++m_CountMarketWatch[0]; //Succeed
	    else
		++m_CountMarketWatch[1]; //Lated

	    g_RtHistogram[MARKET_WATCH].Put(clk2 - clk1);
	}

	return( TxnOutput.status==CBaseTxnErr::SUCCESS );
    }

    if(GlobalState == MEASURING)
	++m_CountMarketWatch[3]; //Failed

    cout << "CCESUT::MarketWatch() failed." << endl;
    return(false);
}

bool CCESUT::SecurityDetail( PSecurityDetailTxnInput pTxnInput )
{
    CSecurityDetailDB SecurityDetailDB = CSecurityDetailDB(m_pDBConnection);
    CSecurityDetail SecurityDetail(&SecurityDetailDB);
    TSecurityDetailTxnOutput TxnOutput;

    clock_t clk1,clk2;
    struct tms tbuf;

    for (int i = 0; i < MAX_RETRY; i++)
    {
#ifdef PRINT_DEADLOCK
	if (i)
	    cout << "  Retry SD:" << i << endl;
#endif

	try
	{
	    clk1 = times( &tbuf );
	    SecurityDetail.DoTxn( pTxnInput, &TxnOutput );
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
		    ++m_CountSecurityDetail[2]; //Retried
		continue;
	    }
	    else
	    {
		cout << "ODBCERR at CCESUT::SecurityDetail()" << endl << "  " << e->what() << endl;
		delete e;
		break;
	    }
	}

	if(GlobalState == MEASURING)
	{
	    if((clk2 - clk1) / clk_tck < 3)
		++m_CountSecurityDetail[0]; //Succeed
	    else
		++m_CountSecurityDetail[1]; //Lated

	    g_RtHistogram[SECURITY_DETAIL].Put(clk2 - clk1);
	}

	return( TxnOutput.status==CBaseTxnErr::SUCCESS );
    }

    if(GlobalState == MEASURING)
	++m_CountSecurityDetail[3]; //Failed

    cout << "CCESUT::SecurityDetail() failed." << endl;
    return(false);
}

bool CCESUT::TradeLookup( PTradeLookupTxnInput pTxnInput )
{
    CTradeLookupDB TradeLookupDB(m_pDBConnection);
    CTradeLookup TradeLookup(&TradeLookupDB);
    TTradeLookupTxnOutput TxnOutput;

    clock_t clk1,clk2;
    struct tms tbuf;

    for (int i = 0; i < MAX_RETRY; i++)
    {
#ifdef PRINT_DEADLOCK
	if (i)
	    cout << "  Retry TL:" << i << endl;
#endif

	try
	{
	    clk1 = times( &tbuf );
	    TradeLookup.DoTxn( pTxnInput, &TxnOutput );
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
		    ++m_CountTradeLookup[2]; //Retried
		continue;
	    }
	    else
	    {
		cout << "ODBCERR at CCESUT::TradeLookup()" << endl << "  " << e->what() << endl;
		delete e;
		break;
	    }
	}

	if(GlobalState == MEASURING)
	{
	    if((clk2 - clk1) / clk_tck < 3)
		++m_CountTradeLookup[0]; //Succeed
	    else
		++m_CountTradeLookup[1]; //Lated

	    g_RtHistogram[TRADE_LOOKUP].Put(clk2 - clk1);
	}

	return( TxnOutput.status==CBaseTxnErr::SUCCESS );
    }

    if(GlobalState == MEASURING)
	++m_CountTradeLookup[3]; //Failed

    cout << "CCESUT::TradeLookup() failed." << endl;
    return(false);
}

bool CCESUT::TradeStatus( PTradeStatusTxnInput pTxnInput )
{
    CTradeStatusDB TradeStatusDB(m_pDBConnection);
    CTradeStatus TradeStatus(&TradeStatusDB);
    TTradeStatusTxnOutput TxnOutput;

    clock_t clk1,clk2;
    struct tms tbuf;

    for (int i = 0; i < MAX_RETRY; i++)
    {
#ifdef PRINT_DEADLOCK
	if (i)
	    cout << "  Retry TS:" << i << endl;
#endif

	try
	{
	    clk1 = times( &tbuf );
	    TradeStatus.DoTxn( pTxnInput, &TxnOutput );
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
		    ++m_CountTradeStatus[2]; //Retried
		continue;
	    }
	    else
	    {
		cout << "ODBCERR at CCESUT::TradeStatus()" << endl << "  " << e->what() << endl;
		delete e;
		break;
	    }
	}

	if(GlobalState == MEASURING)
	{
	    if((clk2 - clk1) / clk_tck < 1)
		++m_CountTradeStatus[0]; //Succeed
	    else
		++m_CountTradeStatus[1]; //Lated

	    g_RtHistogram[TRADE_STATUS].Put(clk2 - clk1);
	}

	return( TxnOutput.status==CBaseTxnErr::SUCCESS );
    }

    if(GlobalState == MEASURING)
	++m_CountTradeStatus[3]; //Failed

    cout << "CCESUT::TradeStatus() failed." << endl;
    return(false);
}

bool CCESUT::TradeOrder( PTradeOrderTxnInput pTxnInput, INT32 iTradeType,
			 bool bExecutorIsAccountOwner )
{
    //MEMO: What should we use iTradeType, bExecutorIsAccountOwner for?

    CSendToMarket SendToMarket;
    CTradeOrderDB TradeOrderDB(m_pDBConnection);
    CTradeOrder TradeOrder(&TradeOrderDB, &SendToMarket);
    TTradeOrderTxnOutput TxnOutput;

    clock_t clk1,clk2;
    struct tms tbuf;

    for (int i = 0; i < MAX_RETRY; i++)
    {
#ifdef PRINT_DEADLOCK
	if (i)
	    cout << "  Retry TO:" << i << endl;
#endif

	try
	{
	    clk1 = times( &tbuf );
	    TradeOrder.DoTxn( pTxnInput, &TxnOutput );
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
		    ++m_CountTradeOrder[2]; //Retried
		continue;
	    }
	    else
	    {
		cout << "ODBCERR at CCESUT::TradeOrder()" << endl << "  " << e->what() << endl;
		delete e;
		break;
	    }
	}

	if(GlobalState == MEASURING)
	{
	    if((clk2 - clk1) / clk_tck < 2)
		++m_CountTradeOrder[0]; //Succeed
	    else
		++m_CountTradeOrder[1]; //Lated

	    g_RtHistogram[TRADE_ORDER].Put(clk2 - clk1);
	}

	return( TxnOutput.status==CBaseTxnErr::SUCCESS );
    }

    if(GlobalState == MEASURING)
	++m_CountTradeOrder[3]; //Failed

    cout << "CCESUT::TradeOrder() failed." << endl;
    return(false);
}

bool CCESUT::TradeUpdate( PTradeUpdateTxnInput pTxnInput )
{
    CTradeUpdateDB TradeUpdateDB(m_pDBConnection);
    CTradeUpdate TradeUpdate(&TradeUpdateDB);
    TTradeUpdateTxnOutput TxnOutput;

    clock_t clk1,clk2;
    struct tms tbuf;

    for (int i = 0; i < MAX_RETRY; i++)
    {
#ifdef PRINT_DEADLOCK
	if (i)
	    cout << "  Retry TU:" << i << endl;
#endif

	try
	{
	    clk1 = times( &tbuf );
	    TradeUpdate.DoTxn( pTxnInput, &TxnOutput );
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
		    ++m_CountTradeUpdate[2]; //Retried
		continue;
	    }
	    else
	    {
		cout << "ODBCERR at CCESUT::TradeUpdate()" << endl << "  " << e->what() << endl;
		delete e;
		break;
	    }
	}

	if(GlobalState == MEASURING)
	{
	    if((clk2 - clk1) / clk_tck < 3)
		++m_CountTradeUpdate[0]; //Succeed
	    else
		++m_CountTradeUpdate[1]; //Lated

	    g_RtHistogram[TRADE_UPDATE].Put(clk2 - clk1);
	}

	return( TxnOutput.status==CBaseTxnErr::SUCCESS );
    }

    if(GlobalState == MEASURING)
	++m_CountTradeUpdate[3]; //Failed

    cout << "CCESUT::TradeUpdate() failed." << endl;
    return(false);
}
