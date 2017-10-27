// SecurityDetailDB.h
//   2008 Yasufumi Kinoshita

#ifndef SECURITY_DETAIL_DB_H
#define SECURITY_DETAIL_DB_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CSecurityDetailDB : public CTxnDBBase, public CSecurityDetailDBInterface
{
 public:
    CSecurityDetailDB(CDBConnection *pDBConn);
    ~CSecurityDetailDB();

    virtual void DoSecurityDetailFrame1(const TSecurityDetailFrame1Input *pIn,
					TSecurityDetailFrame1Output *pOut);
};

}   //namespace TPCE

#endif //SECURITY_DETAIL_DB_H
