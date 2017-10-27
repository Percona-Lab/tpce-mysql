// MEESUT.h
//   2008 Yasufumi Kinoshita

#ifndef MEE_SUT_H
#define MEE_SUT_H

#include "MEESUTInterface.h"

namespace TPCE
{

class CMEESUT : public CMEESUTInterface
{
 private:
    char m_szHost[iMaxHostname];
    char m_szDBName[iMaxDBName];
    char m_szDBUser[iMaxDBName];
    char m_szDBPass[iMaxDBName];

 public:
    unsigned int m_CountTradeResult[4];
    unsigned int m_CountMarketFeed[4];

    CMEESUT(const char *szHost, const char *szDBName,
	    const char *szDBUser, const char *szDBPass,
	    INT32 InitialThreads, INT32 MaxThreads);
    ~CMEESUT();

    virtual bool TradeResult( PTradeResultTxnInput pTxnInput );
    virtual bool MarketFeed( PMarketFeedTxnInput pTxnInput );
};

}   // namespace TPCE

#endif //MEE_SUT_H
