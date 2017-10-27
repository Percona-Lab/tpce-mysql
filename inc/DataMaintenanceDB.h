// DataMaintenanceDB.h
//   2008 Yasufumi Kinoshita

#ifndef DATA_MAINTENANCE_DB_H
#define DATA_MAINTENANCE_DB_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CDataMaintenanceDB : public CTxnDBBase, public CDataMaintenanceDBInterface
{
 public:
    CDataMaintenanceDB(CDBConnection *pDBConn);
    ~CDataMaintenanceDB();

    void DoDataMaintenanceFrame1(const TDataMaintenanceFrame1Input *pIn,
				 TDataMaintenanceFrame1Output *pOut);
};

}   //namespace TPCE

#endif //DATA_MAINTENANCE_DB_H
