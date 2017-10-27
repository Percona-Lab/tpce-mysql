// TradeStatusDB.h
//   2008 Yasufumi Kinoshita

#ifndef TRADE_STATUS_DB_H
#define TRADE_STATUS_DB_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CTradeStatusDB : public CTxnDBBase, public CTradeStatusDBInterface
{
 public:
    CTradeStatusDB(CDBConnection *pDBConn);
    ~CTradeStatusDB();

    virtual void DoTradeStatusFrame1(const TTradeStatusFrame1Input *pIn,
				     TTradeStatusFrame1Output *pOut);
};

}   // namespace TPCE

#endif //TRADE_STATUS_DB_H
