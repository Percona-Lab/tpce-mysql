// TradeOrderDB.h
//   2008 Yasufumi Kinoshita

#ifndef TRADE_ORDER_DB_H
#define TRADE_ORDER_DB_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CTradeOrderDB : public CTxnDBBase, public CTradeOrderDBInterface
{
 public:
    CTradeOrderDB(CDBConnection *pDBConn);
    ~CTradeOrderDB();

    virtual void DoTradeOrderFrame1(const TTradeOrderFrame1Input *pIn,
				    TTradeOrderFrame1Output *pOut);
    virtual void DoTradeOrderFrame2(const TTradeOrderFrame2Input *pIn,
				    TTradeOrderFrame2Output *pOut);
    virtual void DoTradeOrderFrame3(const TTradeOrderFrame3Input *pIn,
				    TTradeOrderFrame3Output *pOut);
    virtual void DoTradeOrderFrame4(const TTradeOrderFrame4Input *pIn,
				    TTradeOrderFrame4Output *pOut);
    virtual void DoTradeOrderFrame5(TTradeOrderFrame5Output *pOut);
    virtual void DoTradeOrderFrame6(TTradeOrderFrame6Output *pOut);
};

}   //namespace TPCE

#endif //TRADE_ORDER_DB_H
