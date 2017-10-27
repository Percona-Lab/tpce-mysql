// TxnDBBase.h
//   2008 Yasufumi Kinoshita

#ifndef TXN_DB_BASE_H
#define TXN_DB_BASE_H

#define MAX_RETRY 20

namespace TPCE
{

class CTxnDBBase
{
protected:
    CDBConnection* m_pDBConnection;
    SQLHDBC        m_Conn;
    SQLHSTMT       m_Stmt;
    SQLHSTMT       m_Stmt2;
    SQLLEN         m_DummyInd;

public:
    CTxnDBBase(CDBConnection *pDBConn);
    ~CTxnDBBase();

    void BeginTxn();
    void CommitTxn();
    void RollbackTxn();
    void ThrowError( CODBCERR::ACTION eAction, SQLSMALLINT HandleType, SQLHANDLE Handle,
		     const char* FileName = NULL, unsigned int Line = 0);
    void ThrowError( CODBCERR::ACTION eAction,
		     const char* FileName = NULL, unsigned int Line = 0)
	{
	    ThrowError(eAction, 0, SQL_NULL_HANDLE, FileName, Line);
	};
};

}   //namespace TPCE

#endif //TXN_DB_BASE_H
