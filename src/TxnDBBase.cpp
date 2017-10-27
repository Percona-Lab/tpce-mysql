// TxnDBBase.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

CTxnDBBase::CTxnDBBase(CDBConnection *pDBConn)
    : m_pDBConnection(pDBConn)
{
    m_Conn = m_pDBConnection->m_Conn;
    m_Stmt = m_pDBConnection->m_Stmt;
    m_Stmt2 = m_pDBConnection->m_Stmt2;
}

CTxnDBBase::~CTxnDBBase()
{
}

void CTxnDBBase::BeginTxn()
{
    m_pDBConnection->BeginTxn();
}

void CTxnDBBase::CommitTxn()
{
    m_pDBConnection->CommitTxn();
}

void CTxnDBBase::RollbackTxn()
{
    m_pDBConnection->RollbackTxn();
}

void CTxnDBBase::ThrowError( CODBCERR::ACTION eAction, SQLSMALLINT HandleType, SQLHANDLE Handle,
			     const char* FileName, unsigned int Line)
{
    m_pDBConnection->ThrowError(eAction, HandleType, Handle, FileName, Line);
}
