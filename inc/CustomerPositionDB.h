// CustomerPositionDB.h
//   2008 Yasufumi Kinoshita

#ifndef CUSTOMER_POSITION_DB_H
#define CUSTOMER_POSITION_DB_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CCustomerPositionDB : public CTxnDBBase, public CCustomerPositionDBInterface
{
 public:
    CCustomerPositionDB(CDBConnection *pDBConn);
    ~CCustomerPositionDB();

    virtual void DoCustomerPositionFrame1(const TCustomerPositionFrame1Input *pIn,
					  TCustomerPositionFrame1Output *pOut);
    virtual void DoCustomerPositionFrame2(const TCustomerPositionFrame2Input *pIn,
					  TCustomerPositionFrame2Output *pOut);
    virtual void DoCustomerPositionFrame3(TCustomerPositionFrame3Output *pOut);
};

}   // namespace TPCE

#endif //CUSTOMER_POSITION_DB_H
