// odbc_wrapper.h
//   2008 Yasufumi Kinoshita

#ifndef _ODBC_WRAPPER_H
#define _ODBC_WRAPPER_H

#include <my_global.h>
#include <mysql.h>
#include <my_sys.h>

// This odbc_wrapper needs unixODBC headers.
#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

// Trick to avoid g++ error
#undef max
#undef min

namespace TPCE
{

extern "C" {

enum MY_STATE { ST_UNKNOWN, ST_PREPARED, ST_PRE_EXECUTED, ST_EXECUTED, ST_DIRECT_EXECUTED };

typedef struct st_my_hdbc
{
    MYSQL* mysql;

    int connected;
} MY_HDBC;

typedef struct st_my_hstmt
{
    MYSQL_STMT*   mysql_stmt;

    enum MY_STATE state;

    int nColumns;
    int nParameters;
    MYSQL_RES*  meta_result;
    MYSQL_BIND* columnP;
    MYSQL_BIND* paramP;

    int prepared;
    int res_binded;
} MY_HSTMT;

typedef struct st_param_bind
{
  SQLSMALLINT   SqlType,CType;
  void *        buffer;
  char *        pos_in_query,*value;
  SQLINTEGER    ValueMax;
  SQLINTEGER *  actual_len;
  SQLINTEGER    value_length;
  bool          alloced,used;
  bool          real_param_done;
} PARAM_BIND;

}

}

SQLRETURN  SQL_API SQLConnect_DIRECT(SQLHDBC ConnectionHandle,
				SQLCHAR *ServerName, SQLSMALLINT NameLength1,
				SQLCHAR *DatabaseName, SQLSMALLINT NameLength4,
				SQLCHAR *UserName, SQLSMALLINT NameLength2,
				SQLCHAR *Authentication, SQLSMALLINT NameLength3);



#endif /* _ODBC_WRAPPER_H */
