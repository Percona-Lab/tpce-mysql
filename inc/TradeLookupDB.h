// TradeLookupDB.h
//   2008 Yasufumi Kinoshita

#ifndef TRADE_LOOKUP_DB_H
#define TRADE_LOOKUP_DB_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CTradeLookupDB : public CTxnDBBase, public CTradeLookupDBInterface
{
public:
    CTradeLookupDB(CDBConnection *pDBConn);
    ~CTradeLookupDB();

    virtual void DoTradeLookupFrame1(const TTradeLookupFrame1Input *pIn,
				     TTradeLookupFrame1Output *pOut);
    virtual void DoTradeLookupFrame2(const TTradeLookupFrame2Input *pIn,
				     TTradeLookupFrame2Output *pOut);
    virtual void DoTradeLookupFrame3(const TTradeLookupFrame3Input *pIn,
				     TTradeLookupFrame3Output *pOut);
    virtual void DoTradeLookupFrame4(const TTradeLookupFrame4Input *pIn,
				     TTradeLookupFrame4Output *pOut);
};

}   // namespace TPCE

#endif //TRADE_LOOKUP_DB_H
