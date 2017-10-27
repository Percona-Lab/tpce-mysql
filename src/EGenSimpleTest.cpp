// EGenSimpleTest.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

extern int MEESUT_ThreadCount; //MEESUT.cpp

char szInDir[iMaxPath] = "flat_in";
char szHost[iMaxHostname] = "localhost";
char szDBName[iMaxDBName] = "tpce";
char szDBUser[iMaxDBName] = "tpce";
char szDBPass[iMaxDBName] = "tpce";
TIdent iConfiguredCustomerCount = iDefaultCustomerCount;
TIdent iActiveCustomerCount = iDefaultCustomerCount;
int iScaleFactor = 500;
int iDaysOfInitialTrades = 300;
int iLoadUnitSize = iDefaultLoadUnitSize;
int iTestDuration = 0;
int iRampupDuration = 0;
int iSleep = 1000; // msec between thread creation
int iUsers = 0; // # users
int iPacingDelay = 0; //10?
char outputDirectory[iMaxPath] = "."; // path to output files

CLogFormatTab g_fmt;
#ifndef DEBUG
CEGenNullLogger* g_pLog;
#else
CEGenLogger* g_pLog;
#endif

eGlobalState GlobalState = INITIAL;

//TradeRequest message queue
list<TTradeRequest> ReqQueue;
int                 ReqQueue_size = 0;
CMutex              ReqQueue_Lock;
sem_t               ReqQueue_sem;

//DMTrigger
sem_t   DMTrigger_sem;

//ALL SUT
void** g_ppSUT;

unsigned int g_TxnCount[TRADE_CLEANUP][4]; //TRADE_CLEANUP isn't counted

CRtHistogram g_RtHistogram[TRADE_CLEANUP]; //Hmm.. it isn't clean...

//Alarm
int timer_count = 0;
//PRINT_INTERVAL must be divisor of 60
#define PRINT_INTERVAL 10


void push_message(PTradeRequest pMessage)
{
    ReqQueue_Lock.lock();
    ReqQueue.push_back(*pMessage);
    ++ReqQueue_size;
    ReqQueue_Lock.unlock();
    sem_post(&ReqQueue_sem);
}

/*
 * Prints program usage to std error.
 */
void Usage()
{
    cerr <<
	"\nUsage: EGenSimpleTest {options}" << endl << endl <<
	"  where" << endl <<
	"   Option      Default                Description" << endl <<
	"   =========   ===================    =============================================" << endl <<
	"   -e string   " << szInDir  << "\t\t      Path to EGen input files" << endl <<
#ifdef ODBC_WRAPPER
	"   -S string   " << szHost   << "\t      Database server" << endl <<
	"   -D string   " << szDBName << "\t\t      Database name" << endl <<
#else
	"   -D string   " << szDBName << "\t\t      Data source name" << endl <<
#endif
	"   -U string   " << szDBUser << "\t\t      Database user" << endl <<
	"   -P string   " << szDBPass << "\t\t      Database password" << endl <<
	"   -c number   " << iConfiguredCustomerCount << "\t\t      Configured customer count" << endl <<
	"   -a number   " << iActiveCustomerCount << "\t\t      Active customer count" << endl <<
	"   -f number   " << iScaleFactor << "\t\t      # of customers for 1 TRTPS" << endl <<
	"   -d number   " << iDaysOfInitialTrades << "\t\t      # of Days of Initial Trades" << endl <<
	"   -l number   " << iLoadUnitSize << "\t\t      # of customers in one load unit" << endl <<
	"   -t number                          Duration of the test (seconds)" << endl <<
	"   -r number                          Duration of ramp up period (seconds)" << endl <<
	"   -u number                          # of Users" << endl
#ifdef DEBUG
	 << "   -o string   " << outputDirectory << "\t\t      # directory for output files" << endl
#endif
	;
}

void ParseCommandLine( int argc, char *argv[] )
{
    int   arg;
    char  *sp;
    char  *vp;

    /*
     *  Scan the command line arguments
     */
    for ( arg = 1; arg < argc; ++arg ) {

	/*
	 *  Look for a switch
	 */
	sp = argv[arg];
	if ( *sp == '-' ) {
	    ++sp;
	}

	/*
	 *  Find the switch's argument.  It is either immediately after the
	 *  switch or in the next argv
	 */
	vp = sp + 1;
	// Allow for switched that don't have any parameters.
	// Need to check that the next argument is in fact a parameter
	// and not the next switch that starts with '-'.
	//
	if ( (*vp == 0) && ((arg + 1) < argc) && (argv[arg + 1][0] != '-') )
	{
	    vp = argv[++arg];
	}

	/*
	 *  Parse the switch
	 */
	switch ( *sp ) {
	    case 'e':       // input files path
		strncpy(szInDir, vp, sizeof(szInDir));
		break;
	    case 'S':       // Database host name.
#ifdef ODBC_WRAPPER
		strncpy(szHost, vp, sizeof(szHost));
		break;
#endif
	    case 'D':       // Database name.
		strncpy(szDBName, vp, sizeof(szDBName));
		break;
	    case 'U':       // Database user.
		strncpy(szDBUser, vp, sizeof(szDBName));
		break;
	    case 'P':       // Database password.
		strncpy(szDBPass, vp, sizeof(szDBName));
		break;
	    case 'c':
		sscanf(vp, "%"PRId64, &iConfiguredCustomerCount);
		break;
	    case 'a':
		sscanf(vp, "%"PRId64, &iActiveCustomerCount);
		break;
	    case 'f':
		sscanf(vp, "%d", &iScaleFactor);
		break;
	    case 'd':
		sscanf(vp, "%d", &iDaysOfInitialTrades);
		break;
	    case 'l':
		sscanf(vp, "%d", &iLoadUnitSize);
		break;
	    case 't':
		sscanf(vp, "%d", &iTestDuration);
		break;
	    case 'r':
		sscanf(vp, "%d", &iRampupDuration);
		break;
	    case 's':
		sscanf(vp, "%d", &iSleep);
		break;
	    case 'u':
		sscanf(vp, "%d", &iUsers);
		break;
	    case 'p':
		sscanf(vp, "%d", &iPacingDelay);
		break;
	    case 'o':
		strncpy(outputDirectory, vp,
			sizeof(outputDirectory));
		break;
	    default:
		Usage();
		fprintf( stderr, "Error: Unrecognized option: %s\n",sp);
		exit( ERROR_BAD_OPTION );
	}
    }

}

bool ValidateParameters()
{
    bool bRet = true;

    //TODO:

    return bRet;
}

void* thread_CE(void *UniqueID)
{
    //cout<<"CE "<<(long)UniqueID<<endl;

    CInputFiles m_InputFiles;
    PDriverCETxnSettings m_pDriverCETxnSettings;
    CCESUT *m_pCCESUT;
    CCE *m_pCCE;

    m_pDriverCETxnSettings = new TDriverCETxnSettings;
    m_InputFiles.Initialize(eDriverCE, iConfiguredCustomerCount,
			    iActiveCustomerCount, szInDir);
    try
    {
	m_pCCESUT = new CCESUT(szHost, szDBName, szDBUser, szDBPass); //throw CODBCERR
    }
    catch (const CODBCERR* e)
    {
	cerr << "ODBCERR at thread_CE()" << endl << "  " << e->what() << endl;
	delete e;
	pthread_exit(NULL);
    }

    m_pCCE = new CCE(m_pCCESUT, g_pLog, m_InputFiles,
		     iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
		     iDaysOfInitialTrades, (long)UniqueID, m_pDriverCETxnSettings);

    g_ppSUT[(long)UniqueID] = (void*) m_pCCESUT;



    while(GlobalState < STOPPING)
    {
	m_pCCE->DoTxn();

	if(iPacingDelay)
	{
	    struct timespec req,rem;
	    req.tv_sec = iPacingDelay / 1000;
	    req.tv_nsec= (iPacingDelay % 1000) * 1000000;
	    while(nanosleep(&req, &rem) == -1)
	    {
		if (errno == EINTR) {
		    memcpy(&req, &rem, sizeof(timespec));
		} else {
		    cerr<<"error: nanosleep"<<endl;
		}
	    }
	}
	else
	{
	    //No pacing delay
	    sched_yield();
	}
    }



    g_ppSUT[(long)UniqueID] = NULL;

    delete m_pCCE;
    delete m_pCCESUT;
    delete m_pDriverCETxnSettings;

    pthread_exit(NULL);
}

void* thread_DM(void *UniqueID)
{
    //cout<<"DM "<<(long)UniqueID<<endl;

    CInputFiles m_InputFiles;
    CDMSUT *m_pCDMSUT;
    CDM *m_pCDM;

    m_InputFiles.Initialize(eDriverDM, iConfiguredCustomerCount,
			    iActiveCustomerCount, szInDir);

    try
    {
	m_pCDMSUT = new CDMSUT(szHost, szDBName, szDBUser, szDBPass); //throw CODBCERR
    }
    catch (const CODBCERR* e)
    {
	cerr << "ODBCERR at thread_DM()" << endl << "  " << e->what() << endl;
	delete e;
	GlobalState = STOPPING;
	pthread_exit(NULL);
    }

    m_pCDM = new CDM(m_pCDMSUT, g_pLog, m_InputFiles,
		     iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
		     iDaysOfInitialTrades, (long)UniqueID);

    g_ppSUT[(long)UniqueID] = (void*) m_pCDMSUT;


    //Trade-Cleanup transaction before starting the test
    m_pCDM->DoCleanupTxn();
    GlobalState = CLEANUPED;

    //Wait DMTrigger
    sem_wait(&DMTrigger_sem);

    while(GlobalState < STOPPING)
    {
	m_pCDM->DoTxn();

	//Wait DMTrigger
	sem_wait(&DMTrigger_sem);
    }


    g_ppSUT[(long)UniqueID] = NULL;

    delete m_pCDM;
    delete m_pCDMSUT;

    pthread_exit(NULL);
}

void* thread_MEE(void *UniqueID)
{
    //cout<<"MEE "<<(long)UniqueID<<endl;

    TTradeRequest tMessage;

    CInputFiles m_InputFiles;
    CMEESUT *m_pCMEESUT;
    CMEE *m_pCMEE;

    m_InputFiles.Initialize(eDriverMEE, iConfiguredCustomerCount,
			    iActiveCustomerCount, szInDir);

    //MEMO: 6th parameter setting may be essential for throughput (MaxTherads)
    m_pCMEESUT = new CMEESUT(szHost, szDBName, szDBUser, szDBPass, 30, 120);
    m_pCMEE = new CMEE( 0, m_pCMEESUT, g_pLog, m_InputFiles, (long)UniqueID);
    m_pCMEE->SetBaseTime();

    g_ppSUT[(long)UniqueID] = (void*) m_pCMEESUT;

    memset(&tMessage, 0, sizeof(TTradeRequest));

    while(GlobalState < STOPPING)
    {
	//Wait queued data.
	sem_wait(&ReqQueue_sem);
	if(GlobalState >= STOPPING)
	    break;

	//pop from queue
	ReqQueue_Lock.lock();
	if(ReqQueue.empty())
	{
	    ReqQueue_Lock.unlock();
	    continue;
	}
	tMessage = ReqQueue.front();
	ReqQueue.pop_front();
	--ReqQueue_size;
	ReqQueue_Lock.unlock();

#ifdef DEBUG
	cout<<"receive TradeRequest trade_id="<<tMessage.trade_id<<endl;
#endif
	m_pCMEE->SubmitTradeRequest( &tMessage ); //may not throw CODBCERR
    }

    g_ppSUT[(long)UniqueID] = NULL;

    delete m_pCMEE;
    delete m_pCMEESUT;

    pthread_exit(NULL);
}

void alarm_handler(int signum)
{
    static int DMCounter = 60 / PRINT_INTERVAL;
    unsigned int tmp[TRADE_CLEANUP][4];

    //Wake thread_DM up once in a minute.
    DMCounter--;
    if(DMCounter==0)
    {
	sem_post(&DMTrigger_sem);
	DMCounter = 60 / PRINT_INTERVAL;
    }


    if(GlobalState == MEASURING)
    {
	timer_count += PRINT_INTERVAL;

	for(int i=0; i<4; i++)
	{
	    tmp[DATA_MAINTENANCE][i] = ((CDMSUT*)g_ppSUT[0])->m_CountDataMaintenance[i];

	    tmp[TRADE_RESULT][i]     = ((CMEESUT*)g_ppSUT[1])->m_CountTradeResult[i];
	    tmp[MARKET_FEED][i]      = ((CMEESUT*)g_ppSUT[1])->m_CountMarketFeed[i];

	    tmp[BROKER_VOLUME][i] = 0;
	    tmp[CUSTOMER_POSITION][i] = 0;
	    tmp[MARKET_WATCH][i] = 0;
	    tmp[SECURITY_DETAIL][i] = 0;
	    tmp[TRADE_LOOKUP][i] = 0;
	    tmp[TRADE_ORDER][i] = 0;
	    tmp[TRADE_STATUS][i] = 0;
	    tmp[TRADE_UPDATE][i] = 0;

	    for(int j=0; j < iUsers; j++)
	    {
		tmp[BROKER_VOLUME][i]     +=((CCESUT*)g_ppSUT[j+2])->m_CountBrokerVolume[i];
		tmp[CUSTOMER_POSITION][i] +=((CCESUT*)g_ppSUT[j+2])->m_CountCustomerPosition[i];
		tmp[MARKET_WATCH][i]      +=((CCESUT*)g_ppSUT[j+2])->m_CountMarketWatch[i];
		tmp[SECURITY_DETAIL][i]   +=((CCESUT*)g_ppSUT[j+2])->m_CountSecurityDetail[i];
		tmp[TRADE_LOOKUP][i]      +=((CCESUT*)g_ppSUT[j+2])->m_CountTradeLookup[i];
		tmp[TRADE_ORDER][i]       +=((CCESUT*)g_ppSUT[j+2])->m_CountTradeOrder[i];
		tmp[TRADE_STATUS][i]      +=((CCESUT*)g_ppSUT[j+2])->m_CountTradeStatus[i];
		tmp[TRADE_UPDATE][i]      +=((CCESUT*)g_ppSUT[j+2])->m_CountTradeUpdate[i];
	    }
	}

	printf("%4d", timer_count);
	for(int i=0; i<11; i++)
	{
	    printf((i==0)?" | %5d":", %5d", (tmp[i][0] - g_TxnCount[i][0] + tmp[i][1] - g_TxnCount[i][1]));
	}
	printf(" | %d, %d\n", MEESUT_ThreadCount, ReqQueue_size);

	for(int i=0; i<11; i++)
	{
	    for(int j=0; j < 4; j++)
	    {
		g_TxnCount[i][j] = tmp[i][j];
	    }
	}

	printf("       ");
	for(int i=0; i<11; i++)
	{
	    printf((i==10)?"%5ld\n\n":"%5ld, ", g_RtHistogram[i].Check_point());
	}
	fflush(stdout);
    }
}


int main(int argc, char* argv[])
{
    long UniqueID = 0;
    pthread_t *t;
    struct itimerval itval;
    struct sigaction  sigact;

    // Output EGen version
    PrintEGenVersion();

#ifdef MYSQL_ODBC
    cout << "(for MySQL)" << endl;
#elif PGSQL_ODBC
    cout << "(for PosrgreSQL)" << endl;
#elif ORACLE_ODBC
    cout << "(for Oracle)" << endl;
#else
    cout << "(for ?)" << endl;
#endif

#ifdef USE_PREPARE
    cout << "(Prepared Statement)" << endl;
#else
    cout << "(Literal SQL)" << endl;
#endif

    // Parse command line
    ParseCommandLine(argc, argv);

    // Validate parameters
    if (!ValidateParameters())
    {
	return ERROR_INVALID_OPTION_VALUE;      // exit returning a non-zero code
    }

    cout<<endl<<"Using the following settings:"<<endl<<endl;
    cout<<"\tInput files location:\t\t"<<     szInDir <<endl;
#ifdef ODBC_WRAPPER
    cout<<"\tDatabase server:\t\t"<<          szHost <<endl;
    cout<<"\tDatabase name:\t\t\t"<<          szDBName <<endl;
#else
    cout<<"\tData source name:\t\t"<<       szDBName <<endl;
#endif
    cout<<"\tDatabase user:\t\t\t"<<          szDBUser <<endl;
    cout<<"\tDatabase password:\t\t"<<        szDBPass <<endl;
    cout<<"\tConfigured customer count:\t"<<  iConfiguredCustomerCount <<endl;
    cout<<"\tActive customer count:\t\t"<<    iActiveCustomerCount <<endl;
    cout<<"\tScale Factor:\t\t\t"<<           iScaleFactor <<endl;
    cout<<"\t#Days of initial trades:\t"<<    iDaysOfInitialTrades <<endl;
    cout<<"\tLoad unit size:\t\t\t"<<         iLoadUnitSize <<endl;
    cout<<"\tTest duration (sec):\t\t"<<      iTestDuration <<endl;
    cout<<"\tRamp up duration (sec):\t\t"<<   iRampupDuration <<endl;
    cout<<"\t# of Users:\t\t\t"<<             iUsers <<endl;
#ifdef DEBUG
    cout<<"\tDirectory for output files:\t"<< outputDirectory <<endl<<endl;
#endif

    /* Set up Logger */
#ifndef DEBUG
    g_pLog = new CEGenNullLogger(eDriverAll, 0, NULL, &g_fmt);
#else
    char filename[1024];
    sprintf(filename, "%s/SimpleTest.log", outputDirectory);
    g_pLog = new CEGenLogger(eDriverAll, 0, filename, &g_fmt);
#endif

    /* Init TradeRequest Queue */
    if(sem_init(&ReqQueue_sem, 0, 0))
    {
	cerr<<"error: sem_init"<<endl;
	exit(1);
    }

    /* Init DMTrigger */
    if(sem_init(&DMTrigger_sem, 0, 0))
    {
	cerr<<"error: sem_init"<<endl;
	exit(1);
    }
    
    /* Set up threads */
    //0:DMSUT 1:MEESUT 2-:CESUT
    g_ppSUT = (void **) malloc( sizeof(void *) * (iUsers + 2) ) ;  //CE*iUsers + DM + MEE
    if (!g_ppSUT)
    {
	cerr<<"error: malloc(void *)"<<endl;
	exit(1);
    }
    memset(g_ppSUT, 0, sizeof(void *) * (iUsers + 2));

    t = (pthread_t *) malloc( sizeof(pthread_t) * (iUsers + 2) ) ; //CE*iUsers + DM + MEE
    if (!t)
    {
	cerr<<"error: malloc(pthread_t)"<<endl;
	exit(1);
    }

    //Init Alarm
    itval.it_interval.tv_sec = PRINT_INTERVAL;
    itval.it_interval.tv_usec = 0;
    itval.it_value.tv_sec = PRINT_INTERVAL;
    itval.it_value.tv_usec = 0;
    sigact.sa_handler = alarm_handler;
    sigact.sa_flags = 0;
    sigemptyset(&sigact.sa_mask);
    if( sigaction( SIGALRM, &sigact, NULL ) == -1 ) {
	cerr<<"error: sigaction"<<endl;
	exit(1);
    }

    //DM
    if(pthread_create( &t[0], NULL, thread_DM, (void *)UniqueID))
    {
	cerr<<"error: pthread_create "<<endl;
	exit(1);
    }
    UniqueID++;

    //MEE
    if(pthread_create( &t[1], NULL, thread_MEE, (void *)UniqueID))
    {
	cerr<<""<<endl;
	exit(1);
    }
    UniqueID++;

    //Wait the end of CDM->DoCleanupTxn()
    cout<<"Waiting for Trade-Cleanup transaction finished... "<<endl;
    while(GlobalState < CLEANUPED)
	usleep(100 * 1000);
    cout<<"... done"<<endl<<flush;

    if(GlobalState > CLEANUPED)
    {
	//It may be DM error.
	pthread_join( t[0], NULL );
	pthread_join( t[1], NULL );

	free(t);
	free(g_ppSUT);

	sem_destroy(&ReqQueue_sem);
	sem_destroy(&DMTrigger_sem);

	pthread_exit(NULL); //exit with waiting detached threads.
    }

    //CE
    for(int i=0; i < iUsers; i++)
    {
	if(pthread_create( &t[i+2], NULL, thread_CE, (void *)UniqueID))
	{
	    cerr<<""<<endl;
	    exit(1);
	}
	UniqueID++;
    }


    //Start Alarm
    if( setitimer(ITIMER_REAL, &itval, NULL) == -1 ) {
	cerr<<"error: setitimer"<<endl;
    }


    //Ramp up
    for(int i=0; i< (iRampupDuration / PRINT_INTERVAL); i++) {
	pause();
    }


    //Measure
    GlobalState = MEASURING;
    cout<<"Start measuring."<<endl<<endl;
    cout<<"     |     [MEE]    | [DM] |                         [CE]                          |"<<endl;
    cout<<"sec. |    TR,    MF |   DM |   BV,    CP,    MW,    SD,    TL,    TO,    TS,    TU | MEEThreads, ReqQueue"<<endl;
    cout<<"      (1st line: count, 2nd line: 90%ile response [msec.])"<<endl<<endl<<flush;
    for(int i=0; i< (iTestDuration / PRINT_INTERVAL); i++) {
	pause();
    }


    //Stop Alarm
    itval.it_interval.tv_sec = 0;
    itval.it_interval.tv_usec = 0;
    itval.it_value.tv_sec = 0;
    itval.it_value.tv_usec = 0;
    if( setitimer(ITIMER_REAL, &itval, NULL) == -1 ) {
	cerr<<"error: setitimer"<<endl;
    }


    GlobalState = STOPPING;
    cout<<"Stopping all threads..."<<endl;
    sem_post(&ReqQueue_sem); //Dummy signal to avoid freezing.
    sem_post(&DMTrigger_sem); //Wake DMSUT up
    for(int i=0; i < iUsers + 2; i++)
    {
	pthread_join( t[i], NULL );
    }

    cout << endl << "[Histograms]" << endl;
    for(int i=0; i<11; i++)
    {
	switch(i)
	{
	    case TRADE_RESULT:
		cout << "=== Trade-Result ===" << endl;
		break;
	    case MARKET_FEED:
		cout << "=== Market-Feed ===" << endl;
		break;
	    case DATA_MAINTENANCE:
		cout << "=== Data-Maintenance ===" << endl;
		break;
	    case BROKER_VOLUME:
		cout << "=== Broker-Volume ===" << endl;
		break;
	    case CUSTOMER_POSITION:
		cout << "=== Customer-Position ===" << endl;
		break;
	    case MARKET_WATCH:
		cout << "=== Market-Watch ===" << endl;
		break;
	    case SECURITY_DETAIL:
		cout << "=== Security-Detail ===" << endl;
		break;
	    case TRADE_LOOKUP:
		cout << "=== Trade-Lookup ===" << endl;
		break;
	    case TRADE_ORDER:
		cout << "=== Trade-Order ===" << endl;
		break;
	    case TRADE_STATUS:
		cout << "=== Trade-Status ===" << endl;
		break;
	    case TRADE_UPDATE:
		cout << "=== Trade-Update ===" << endl;
		break;
	    default:
		cout << "=== [Unknown] ==="<< endl;
	}
	g_RtHistogram[i].Print();
    }

    cout << endl << "[Deadlocks]" << endl;
    for(int i=0; i<11; i++)
    {
	cout << g_TxnCount[i][2];
	if(i<10)
	    cout << ", ";
	else
	    cout << endl << endl;
    }

    cout << endl << "[TradeResult(TR) transaction]" << endl;
    cout << "Succeed: " << g_TxnCount[TRADE_RESULT][0] << endl;
    cout << "Lated:   " << g_TxnCount[TRADE_RESULT][1] << endl;
    cout << "Retried: " << g_TxnCount[TRADE_RESULT][2] << endl;
    cout << "Failed:  " << g_TxnCount[TRADE_RESULT][3] << endl << endl;

    cout<< (double)(g_TxnCount[TRADE_RESULT][0] + g_TxnCount[TRADE_RESULT][1])
	/ (double)timer_count << " TpsE" << endl << endl;


    free(t);
    free(g_ppSUT);

    sem_destroy(&ReqQueue_sem);
    sem_destroy(&DMTrigger_sem);

    pthread_exit(NULL); //exit with waiting detached threads.
}
