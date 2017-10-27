// RtHistogram.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

CRtHistogram::CRtHistogram()
{
    for(int i=0; i<(RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC); i++)
    {
	m_Total_Hist[i] = m_Current_Hist[i] = 0;
    }
    clk_tck = sysconf(_SC_CLK_TCK);

    m_Total_Max = m_Current_Max = 0;
}

CRtHistogram::~CRtHistogram()
{
}

void CRtHistogram::Put(long rtclk)
{
    long msec = rtclk * 1000 / clk_tck;
    long i = msec / RTHIST_CLASS_MSEC;
    if (i >= (RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC))
	i = (RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC) - 1;

    m_RtHistLock.lock();

    ++m_Current_Hist[i];

    if(msec > m_Current_Max)
	m_Current_Max = msec;

    m_RtHistLock.unlock();
}

long CRtHistogram::Check_point()
{
    long total = 0;
    long tmp = 0;
    long line = RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC;

    m_RtHistLock.lock();

    for(long i=0; i < (RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC); i++)
	total += m_Current_Hist[i];

    for(long i=(RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC)-1; i >= 0 ; i--)
    {
	if( (tmp * 10) <= total )
	{
	    tmp += m_Current_Hist[i];
	    line = i + 1;
	}

	m_Total_Hist[i] += m_Current_Hist[i];
	m_Current_Hist[i] = 0;
    }
    if(tmp==0)
	line=0;

    if(m_Current_Max > m_Total_Max)
	m_Total_Max = m_Current_Max;

    m_Current_Max = 0;

    m_RtHistLock.unlock();

    return( (line == RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC)?(-1):(line * RTHIST_CLASS_MSEC) );
}

void CRtHistogram::Print()
{
    long total = 0;
    long tmp = 0;
    long line = RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC;

    for(long i=0; i < (RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC); i++)
	total += m_Total_Hist[i];

    for(long i=(RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC)-1; i >= 0 ; i--)
    {
	if( (tmp * 10) <= total )
	{
	    tmp += m_Total_Hist[i];
	    line = i + 1;
	}
	else
	{
	    break;
	}
    }
    cout << "90%ile : " << (long)((line == RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC)?(-1):(line * RTHIST_CLASS_MSEC)) <<
	" msec." << endl;
    cout << "max    : " << m_Total_Max << " msec." << endl;

    // print histogram limit (90%ile * 2)
    for(long i=0; (i < (RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC)) && (i < line * 2); i++)
	printf("%5ld, %ld\n", i * RTHIST_CLASS_MSEC, m_Total_Hist[i]);
    printf("\n");
}
