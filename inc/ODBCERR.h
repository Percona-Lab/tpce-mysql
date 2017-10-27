// ODBCERR.h
//   2008 Yasufumi Kinoshita

#ifndef ODBC_ERROR_H
#define ODBC_ERROR_H

#include "error.h"


namespace TPCE
{

#define ERR_TYPE_ODBC                           6       //odbc generated error

class CODBCERR : public CBaseErr
{
    public:
        enum ACTION
        {
            eNone,
            eUnknown,
            eAllocConn,         // error from SQLAllocConnect
            eAllocHandle,       // error from SQLAllocHandle
            eBcpBind,           // error from bcp_bind
            eBcpControl,        // error from bcp_control
            eBcpInit,           // error from bcp_init
            eBcpBatch,          // error from bcp_batch
            eBcpDone,           // error from bcp_done
            eConnOption,        // error from SQLSetConnectOption
            eConnect,           // error from SQLConnect
            eAllocStmt,         // error from SQLAllocStmt
            eExecDirect,        // error from SQLExecDirect
            eBindParam,         // error from SQLBindParameter
            eBindCol,           // error from SQLBindCol
            eFetch,             // error from SQLFetch
            eFetchScroll,       // error from SQLFetchScroll
            eMoreResults,       // error from SQLMoreResults
            ePrepare,           // error from SQLPrepare
            eExecute,           // error from SQLExecute
            eBcpSendrow,        // error from bcp_sendrow
            eSetConnectAttr,    // error from SQLSetConnectAttr
            eSetEnvAttr,        // error from SQLSetEnvAttr
            eSetStmtAttr,       // error from SQLSetStmtAttr
            eSetCursorName,     // error from SQLSetCursorName
            eSQLSetPos,         // error from SQLSetPos
            eEndTran,           // error from SQLEndTran
            eNumResultCols,     // error from SQLNumResultCols
            eCloseCursor,       // error from SQLCloseCursor
            eFreeStmt           // error from SQLFreeStmt
        };

        CODBCERR(char const * szLoc = "")
            : CBaseErr(szLoc)

        {
            m_eAction = eNone;
            m_NativeError = 0;
            m_bDeadLock = false;
            m_odbcerrstr = NULL;
        };

        ~CODBCERR() throw()
        {
            if (m_odbcerrstr != NULL)
                delete [] m_odbcerrstr;
        };

        ACTION  m_eAction;
        int     m_NativeError;
        bool    m_bDeadLock;
        char   *m_odbcerrstr;

        int ErrorType() {return ERR_TYPE_ODBC;};
        int ErrorNum() {return m_NativeError;};
        const char *ErrorText() const {return m_odbcerrstr;};

};

}   // namespace TPCE

#endif //ODBC_ERROR_H
