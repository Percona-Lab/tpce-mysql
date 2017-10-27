// MEESUT.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

extern eGlobalState GlobalState;
extern CRtHistogram g_RtHistogram[TRADE_CLEANUP];

typedef struct TThreadRequest
{
    INT32 TxnType;
    sem_t Sem;
    TTradeResultTxnInput  TradeResultTxnInput;
    TTradeResultTxnOutput TradeResultTxnOutput;
    TMarketFeedTxnInput   MarketFeedTxnInput;
    TMarketFeedTxnOutput  MarketFeedTxnOutput;
} *PThreadRequest;

int MEESUT_num = 0;
int MEESUT_ThreadCount = 0;
int MEESUT_MaxThreadCount = 0;

char MEESUT_szHost[iMaxHostname];
char MEESUT_szDBName[iMaxDBName];
char MEESUT_szDBUser[iMaxDBName];
char MEESUT_szDBPass[iMaxDBName];

CMutex MEESUT_Lock;

CMutex MEESUT_CounterLock;
unsigned int* MEESUT_CountTradeResult = NULL;
unsigned int* MEESUT_CountMarketFeed = NULL;

list<PThreadRequest> MEEThreadPool;

void* MEEAsyncThread(void* data);

CMEESUT::CMEESUT(const char *szHost, const char *szDBName,
		 const char *szDBUser, const char *szDBPass,
		 INT32 InitialThreads, INT32 MaxThreads)
{
    strncpy(m_szHost, szHost, iMaxHostname);
    strncpy(m_szDBName, szDBName, iMaxDBName);
    strncpy(m_szDBUser, szDBUser, iMaxDBName);
    strncpy(m_szDBPass, szDBPass, iMaxDBName);

    for(int i=0; i<4; i++)
    {
	m_CountTradeResult[i]=0;
	m_CountMarketFeed[i]=0;
    }

    MEESUT_Lock.lock();
    MEESUT_num++;
    if(MEESUT_num == 1)
    {
	//This is the first MEESUT

	MEESUT_Lock.unlock();

	strncpy(MEESUT_szHost, szHost, iMaxHostname);
	strncpy(MEESUT_szDBName, szDBName, iMaxDBName);
	strncpy(MEESUT_szDBUser, szDBUser, iMaxDBName);
	strncpy(MEESUT_szDBPass, szDBPass, iMaxDBName);

	MEESUT_CountTradeResult = m_CountTradeResult;
	MEESUT_CountMarketFeed = m_CountMarketFeed;

	MEESUT_MaxThreadCount = MaxThreads;

	//Create initial Therads for MEEThreadPool
	for (int i=0; i<InitialThreads; i++)
	{
	    PThreadRequest pThreadRequest = new TThreadRequest;
	    if(sem_init(&(pThreadRequest->Sem), 0, 0))
	    {
		cerr<<"error: sem_init"<<endl;
		delete pThreadRequest;
		break;
	    }

	    pthread_t tid;
	    pthread_attr_t attr;

	    if(pthread_attr_init(&attr)) {
		cerr<<"error: pthread_attr_init"<<endl;
		delete pThreadRequest;
		break;
	    }
	    if(pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED)) {
		cerr<<"error: pthread_attr_setdetachstate"<<endl;
		delete pThreadRequest;
		break;
	    }
	    if(pthread_create(&tid,&attr,MEEAsyncThread,(void*)pThreadRequest)) {
		cerr<<"error: pthread_create MEEAsyncThread"<<endl;
		delete pThreadRequest;
		break;
	    }
	    pthread_attr_destroy(&attr);

	    MEESUT_Lock.lock();
	    MEESUT_ThreadCount++;
	    MEEThreadPool.push_back(pThreadRequest);
	    MEESUT_Lock.unlock();
	}
    }
    else
    {
	MEESUT_Lock.unlock();
    }
}

CMEESUT::~CMEESUT()
{
    MEESUT_Lock.lock();
    MEESUT_num--;
    if(!MEESUT_num)
    {
	while(!MEEThreadPool.empty())
	{
	    PThreadRequest pThreadRequest;
	    pThreadRequest = MEEThreadPool.front();
	    pThreadRequest->TxnType = CCETxnMixGenerator::INVALID_TRANSACTION_TYPE; //Dummy
	    sem_post(&(pThreadRequest->Sem)); //Wake up to finish 
	    MEEThreadPool.pop_front();

	    MEESUT_Lock.unlock();
	    MEESUT_Lock.lock();
	}

	MEESUT_CountTradeResult = NULL;
	MEESUT_CountMarketFeed = NULL;
    }
    MEESUT_Lock.unlock();
}

void ReleaseThread(PThreadRequest pThreadRequest)
{
    MEESUT_Lock.lock();
    if(MEESUT_num)
    {
	MEEThreadPool.push_back(pThreadRequest);
	MEESUT_Lock.unlock();
    }
    else
    {
	//There is no MEESUT
	MEESUT_Lock.unlock();

	pThreadRequest->TxnType = CCETxnMixGenerator::INVALID_TRANSACTION_TYPE; //Dummy
	sem_post(&(pThreadRequest->Sem)); //Wake up to finish 
    }
}

void* MEEAsyncThread(void* data)
{
    PThreadRequest pThreadRequest = (PThreadRequest) data;
    long clk_tck = sysconf(_SC_CLK_TCK);
    clock_t clk1,clk2;
    struct tms tbuf;

    CDBConnection* pDBConn;

    //cout<<"MEEAsync "<<pThreadRequest<<endl;

    try
    {
	pDBConn = new CDBConnection(MEESUT_szHost, MEESUT_szDBName,
				    MEESUT_szDBUser, MEESUT_szDBPass, 2);
    }
    catch (const CODBCERR* e)
    {
	cerr << "ODBCERR at MEEAsyncThread(CDBConnection)" << endl << "  " << e->what() << endl;

	MEESUT_Lock.lock();
	MEESUT_ThreadCount--;
	MEESUT_Lock.unlock();

	delete e;
	pthread_exit(NULL);
    }

    CTradeResultDB TradeResultDB(pDBConn);
    CTradeResult TradeResult(&TradeResultDB);
    CSendToMarket SendToMarket;
    CMarketFeedDB MarketFeedDB(pDBConn);
    CMarketFeed MarketFeed(&MarketFeedDB, &SendToMarket);

    while(1)
    {
	//Wait a request for this thread
	sem_wait(&(pThreadRequest->Sem));
	MEESUT_Lock.lock();
	if(!MEESUT_num)
	{
	    MEESUT_Lock.unlock();

	    //cout<<"exiting MEEAsync "<<pThreadRequest<<endl;

	    //There is no MEESUT
	    sem_destroy(&(pThreadRequest->Sem));
	    delete pThreadRequest;
	    break;
	}
	MEESUT_Lock.unlock();

	if(pThreadRequest->TxnType == CCETxnMixGenerator::TRADE_RESULT)
	{
	    int succeed = false;
	    for (int i = 0; i < MAX_RETRY; i++)
	    {
#ifdef PRINT_DEADLOCK
		if (i)
		    cout << "  Retry TR:" << i << endl;
#endif

		try
		{
		    clk1 = times( &tbuf );
		    TradeResult.DoTxn(&(pThreadRequest->TradeResultTxnInput),
				      &(pThreadRequest->TradeResultTxnOutput));
		    clk2 = times( &tbuf );
		}
		catch (const CODBCERR* e)
		{
		    pDBConn->RollbackTxn();

		    if (e->m_bDeadLock)
		    {
#ifdef PRINT_DEADLOCK
			cout << "  " << e->what() << endl;
#endif
			delete e;
			if(GlobalState == MEASURING && MEESUT_CountTradeResult)
			{
			    MEESUT_CounterLock.lock();
			    ++MEESUT_CountTradeResult[2]; //Retried
			    MEESUT_CounterLock.unlock();
			}
			continue;
		    }
		    else
		    {
			cout << "ODBCERR at MEEAsyncThread(TradeResult)" << endl 
			     << "  " << e->what() << endl;
			delete e;
			break;
		    }
		}

		if(GlobalState == MEASURING && MEESUT_CountTradeResult)
		{
		    MEESUT_CounterLock.lock();
		    if((clk2 - clk1) / clk_tck < 2)
			++MEESUT_CountTradeResult[0]; //Succeed
		    else
			++MEESUT_CountTradeResult[1]; //Lated
		    MEESUT_CounterLock.unlock();

		    g_RtHistogram[TRADE_RESULT].Put(clk2 - clk1);
		}

		succeed = true;
		break;
	    } // for (int i = 0; i < MAX_RETRY; i++)

	    if(!succeed)
	    {
		if(GlobalState == MEASURING && MEESUT_CountTradeResult)
		{
		    MEESUT_CounterLock.lock();
		    ++MEESUT_CountTradeResult[3]; //Failed
		    MEESUT_CounterLock.unlock();
		}
	    }
	}
	else if(pThreadRequest->TxnType == CCETxnMixGenerator::MARKET_FEED)
	{
	    // Market-Feed frame consists of several transactions.
	    // So almost of ODBCERRs are caught in the frame for the each transactions' retries.

	    try
	    {
		clk1 = times( &tbuf );
		MarketFeed.DoTxn(&(pThreadRequest->MarketFeedTxnInput),
				 &(pThreadRequest->MarketFeedTxnOutput));
		clk2 = times( &tbuf );

		if(GlobalState == MEASURING && MEESUT_CountMarketFeed)
		{
		    MEESUT_CounterLock.lock();
		    if((clk2 - clk1) / clk_tck < 2)
			++MEESUT_CountMarketFeed[0]; //Succeed
		    else
			++MEESUT_CountMarketFeed[1]; //Lated
		    MEESUT_CounterLock.unlock();

		    g_RtHistogram[MARKET_FEED].Put(clk2 - clk1);
		}
	    }
	    catch (const CODBCERR* e)
	    {
		cout << "ODBCERR at MEEAsyncThread(MarketFeed)" << endl 
		     << "  " << e->what() << endl;

		pDBConn->RollbackTxn();

		delete e;

		if(GlobalState == MEASURING && MEESUT_CountMarketFeed)
		{
		    MEESUT_CounterLock.lock();
		    ++MEESUT_CountMarketFeed[3]; //Failed
		    MEESUT_CounterLock.unlock();
		}
	    }
	}
	else
	{
	    cout<<"MEEAsyncThread: wrong TxnType"<<endl;
	}

	ReleaseThread(pThreadRequest);
    }

    delete pDBConn;

    MEESUT_Lock.lock();
    MEESUT_ThreadCount--;
    MEESUT_Lock.unlock();

    pthread_exit(NULL);
}

PThreadRequest GetThread()
{
    PThreadRequest pThreadRequest;

    MEESUT_Lock.lock();
    if(!MEESUT_num)
    {
	//There is no MEESUT
	MEESUT_Lock.unlock();
	return(NULL);
    }

    if(MEEThreadPool.empty())
    {
	if(MEESUT_ThreadCount >= MEESUT_MaxThreadCount)
	{
	    MEESUT_Lock.unlock();

	    //Wait ReleaseThread()
	    while(1) {
		if(!MEESUT_num)
		{
		    //There is no MEESUT
		    return(NULL);
		}
		if(MEEThreadPool.empty())
		{
		    sched_yield();
		    continue;
		}
		MEESUT_Lock.lock();
		if(!MEEThreadPool.empty())
		    break;
		MEESUT_Lock.unlock();
	    }
	    pThreadRequest = MEEThreadPool.front();
	    MEEThreadPool.pop_front();

	    MEESUT_Lock.unlock();

	    return(pThreadRequest);
	}

	MEESUT_Lock.unlock();

	pThreadRequest = new TThreadRequest;
	if(sem_init(&(pThreadRequest->Sem), 0, 0))
	{
	    cerr<<"error: sem_init"<<endl;
	    delete pThreadRequest;
	    return(NULL);
	}

	pthread_t tid;
	pthread_attr_t attr;

	if(pthread_attr_init(&attr)) {
	    cerr<<"error: pthread_attr_init"<<endl;
	    delete pThreadRequest;
	    return(NULL);
	}
	if(pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED)) {
	    cerr<<"error: pthread_attr_setdetachstate"<<endl;
	    delete pThreadRequest;
	    return(NULL);
	}
	if(pthread_create(&tid,&attr,MEEAsyncThread,(void*)pThreadRequest)) {
	    cerr<<"error: pthread_create MEEAsyncThread"<<endl;
	    delete pThreadRequest;
	    return(NULL);
	}
	pthread_attr_destroy(&attr);

	MEESUT_Lock.lock();
	MEESUT_ThreadCount++;
	MEESUT_Lock.unlock();
    }
    else
    {
	pThreadRequest = MEEThreadPool.front();
	MEEThreadPool.pop_front();

	MEESUT_Lock.unlock();
    }

    return(pThreadRequest);
}

bool CMEESUT::TradeResult( PTradeResultTxnInput pTxnInput )
{
    PThreadRequest pThreadRequest;

    pThreadRequest = GetThread();
    if(!pThreadRequest)
	return(false);

    pThreadRequest->TxnType = CCETxnMixGenerator::TRADE_RESULT;
    pThreadRequest->TradeResultTxnInput = *pTxnInput;
    sem_post(&(pThreadRequest->Sem)); //Wake the thread up

    return(true);
}

bool CMEESUT::MarketFeed( PMarketFeedTxnInput pTxnInput )
{
    PThreadRequest pThreadRequest;

    pThreadRequest = GetThread();
    if(!pThreadRequest)
	return(false);

    pThreadRequest->TxnType = CCETxnMixGenerator::MARKET_FEED;
    pThreadRequest->MarketFeedTxnInput = *pTxnInput;
    sem_post(&(pThreadRequest->Sem)); //Wake the thread up

    return(true);
}
