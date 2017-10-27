// TradeUpdateDB.h
//   2008 Yasufumi Kinoshita

#ifndef TRADE_UPDATE_DB_H
#define TRADE_UPDATE_DB_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CTradeUpdateDB : public CTxnDBBase, public CTradeUpdateDBInterface
{
 public:
    CTradeUpdateDB(CDBConnection *pDBConn);
    ~CTradeUpdateDB();

    virtual void DoTradeUpdateFrame1(const TTradeUpdateFrame1Input *pIn,
				     TTradeUpdateFrame1Output *pOut);
    virtual void DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn,
				     TTradeUpdateFrame2Output *pOut);
    virtual void DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn,
				     TTradeUpdateFrame3Output *pOut);
};

}   //namespace TPCE

#endif //TRADE_UPDATE_DB_H
