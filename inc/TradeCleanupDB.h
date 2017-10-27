// TradeCleanupDB.h
//   2008 Yasufumi Kinoshita

#ifndef TRADE_CLEANUP_DB_H
#define TRADE_CLEANUP_DB_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CTradeCleanupDB : public CTxnDBBase, public CTradeCleanupDBInterface
{
 public:
    CTradeCleanupDB(CDBConnection *pDBConn);
    ~CTradeCleanupDB();

    virtual void DoTradeCleanupFrame1(const TTradeCleanupFrame1Input *pIn,
				      TTradeCleanupFrame1Output *pOut);
};

}   // namespace TPCE

#endif //TRADE_CLEANUP_DB_H
