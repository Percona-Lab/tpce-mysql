// MarketFeedDB.h
//   2008 Yasufumi Kinoshita

#ifndef MARKET_FEED_DB_H
#define MARKET_FEED_DB_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CMarketFeedDB : public CTxnDBBase, public CMarketFeedDBInterface
{
 public:
    CMarketFeedDB(CDBConnection *pDBConn);
    ~CMarketFeedDB();

    virtual void DoMarketFeedFrame1(const TMarketFeedFrame1Input *pIn,
				    TMarketFeedFrame1Output *pOut,
				    CSendToMarketInterface *pSendToMarket);
};

}   //namespace TPCE

#endif //MARKET_FEED_DB_H
