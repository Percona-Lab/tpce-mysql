// BrokerVolumeDB.h
//   2008 Yasufumi Kinoshita

#ifndef BROKER_VOLUME_DB_H
#define BROKER_VOLUME_DB_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CBrokerVolumeDB : public CTxnDBBase, public CBrokerVolumeDBInterface
{
 public:
    CBrokerVolumeDB(CDBConnection *pDBConn);
    ~CBrokerVolumeDB();

    virtual void DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn,
				      TBrokerVolumeFrame1Output *pOut);
};

}   // namespace TPCE

#endif //BROKER_VOLUME_DB_H
