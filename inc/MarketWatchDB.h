// MarketWatchDB.h
//   2008 Yasufumi Kinoshita

#ifndef MARKET_WATCH_DB_H
#define MARKET_WATCH_DB_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CMarketWatchDB : public CTxnDBBase, public CMarketWatchDBInterface
{
 public:
    CMarketWatchDB(CDBConnection *pDBConn);
    ~CMarketWatchDB();

    virtual void DoMarketWatchFrame1(const TMarketWatchFrame1Input *pIn,
				     TMarketWatchFrame1Output *pOut);
};

}   //namespace TPCE

#endif //MARKET_WATCH_DB_H
