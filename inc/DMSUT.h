// DMSUT.h
//   2008 Yasufumi Kinoshita

#ifndef DM_SUT_H
#define DM_SUT_H

#include "DMSUTInterface.h"

namespace TPCE
{

class CDMSUT : public CDMSUTInterface
{
 private:
    CDBConnection* m_pDBConnection;
    long clk_tck;

 public:
    unsigned int m_CountDataMaintenance[4];

    CDMSUT(const char *szHost, const char *szDBName,
	   const char *szDBUser, const char *szDBPass);
    ~CDMSUT();

    virtual bool DataMaintenance( PDataMaintenanceTxnInput pTxnInput );
    virtual bool TradeCleanup( PTradeCleanupTxnInput pTxnInput );
};

}   // namespace TPCE

#endif //DM_SUT_H
