// CESUT.h
//   2008 Yasufumi Kinoshita

#ifndef CE_SUT_H
#define CE_SUT_H

#include "CESUTInterface.h"

namespace TPCE
{

class CCESUT : public CCESUTInterface
{
 private:
    CDBConnection* m_pDBConnection;
    long clk_tck;

 public:
    unsigned int m_CountBrokerVolume[4];
    unsigned int m_CountCustomerPosition[4];
    unsigned int m_CountMarketWatch[4];
    unsigned int m_CountSecurityDetail[4];
    unsigned int m_CountTradeLookup[4];
    unsigned int m_CountTradeOrder[4];
    unsigned int m_CountTradeStatus[4];
    unsigned int m_CountTradeUpdate[4];

    CCESUT(const char *szHost, const char *szDBName,
	   const char *szDBUser, const char *szDBPass);
    ~CCESUT();

    virtual bool BrokerVolume( PBrokerVolumeTxnInput pTxnInput );
    virtual bool CustomerPosition( PCustomerPositionTxnInput pTxnInput );
    virtual bool MarketWatch( PMarketWatchTxnInput pTxnInput );
    virtual bool SecurityDetail( PSecurityDetailTxnInput pTxnInput );
    virtual bool TradeLookup( PTradeLookupTxnInput pTxnInput );
    virtual bool TradeOrder( PTradeOrderTxnInput pTxnInput, INT32 iTradeType,
			     bool bExecutorIsAccountOwner );
    virtual bool TradeStatus( PTradeStatusTxnInput pTxnInput );
    virtual bool TradeUpdate( PTradeUpdateTxnInput pTxnInput );
};

}   // namespace TPCE

#endif  // CE_SUT_H
