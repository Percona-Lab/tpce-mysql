// odbc_wrapper.cpp
//   2008 Yasufumi Kinoshita

#ifdef ODBC_WRAPPER
#include "../inc/odbc_wrapper.h"
#include <string.h>

using namespace TPCE;

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT HandleType,
				 SQLHANDLE InputHandle,
				 SQLHANDLE *OutputHandle)
{
    switch(HandleType)
    {
	case SQL_HANDLE_ENV:
	    /* MySQL API don't have environment handle. So this is dummy. */
	    (*OutputHandle) = (SQLHANDLE*) 1; //Dummy value (!= 0)
	    return SQL_SUCCESS;
	    break;

	case SQL_HANDLE_DBC:
	    (*OutputHandle) = (SQLHANDLE*)malloc(sizeof(MY_HDBC));
	    if( *OutputHandle != 0 )
	    {
		memset(*OutputHandle, 0, sizeof(MY_HDBC));
		(*(MY_HDBC**)OutputHandle)->connected = FALSE;
		return SQL_SUCCESS;
	    }
	    else
	    {
		return SQL_ERROR;
	    }
	    break;

	case SQL_HANDLE_STMT:
	    (*OutputHandle) = (SQLHANDLE*)malloc(sizeof(MY_HSTMT));
	    if( *OutputHandle != 0 )
	    {
		memset(*OutputHandle, 0, sizeof(MY_HSTMT));

		(*(MY_HSTMT**)OutputHandle)->mysql_stmt = mysql_stmt_init(((MY_HDBC*)InputHandle)->mysql);
		if((*(MY_HSTMT**)OutputHandle)->mysql_stmt == 0)
		{
		    free(*OutputHandle);
		    *OutputHandle = 0;
		    return SQL_ERROR;
		}

		(*(MY_HSTMT**)OutputHandle)->state = ST_UNKNOWN;
		(*(MY_HSTMT**)OutputHandle)->prepared = FALSE;
		(*(MY_HSTMT**)OutputHandle)->res_binded = FALSE;
		return SQL_SUCCESS;
	    }
	    else
	    {
		return SQL_ERROR;
	    }
	    break;

	default:
	    return SQL_ERROR;
    }
    return SQL_ERROR;
}

SQLRETURN SQL_API SQLBindCol(SQLHSTMT StatementHandle,
			     SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
			     SQLPOINTER TargetValue, SQLLEN BufferLength,
			     SQLLEN *StrLen_or_Ind)
{
    // We must use SQLBindCol() after SQLPrepare().

    SQLSMALLINT ipar = ColumnNumber - 1;

    if(ColumnNumber < 1)
    {
	return SQL_ERROR;
    }

    if(((MY_HSTMT*)StatementHandle)->prepared)
    {
	if(ipar < ((MY_HSTMT*)StatementHandle)->nColumns)
	{
	    switch(TargetType)
	    {
		case SQL_C_SBIGINT:
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_type = MYSQL_TYPE_LONGLONG;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].is_unsigned = FALSE;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_length = sizeof(long long);
		    break;
		case SQL_C_UBIGINT:
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_type = MYSQL_TYPE_LONGLONG;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].is_unsigned = TRUE;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_length = sizeof(long long);
		    break;

		case SQL_C_LONG:
		case SQL_C_SLONG:
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_type = MYSQL_TYPE_LONG;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].is_unsigned = FALSE;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_length = sizeof(int);
		    break;
		case SQL_C_ULONG:
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_type = MYSQL_TYPE_LONG;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].is_unsigned = TRUE;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_length = sizeof(int);
		    break;

		case SQL_C_SHORT:
		case SQL_C_SSHORT:
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_type = MYSQL_TYPE_SHORT;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].is_unsigned = FALSE;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_length = sizeof(short);
		    break;
		case SQL_C_USHORT:
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_type = MYSQL_TYPE_SHORT;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].is_unsigned = TRUE;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_length = sizeof(short);
		    break;

		case SQL_C_TINYINT:
		case SQL_C_STINYINT:
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_type = MYSQL_TYPE_TINY;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].is_unsigned = FALSE;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_length = sizeof(char);
		    break;

		case SQL_C_UTINYINT:
		case SQL_C_BIT:
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_type = MYSQL_TYPE_TINY;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].is_unsigned = TRUE;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_length = sizeof(char);
		    break;

		case SQL_C_DOUBLE:
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_type = MYSQL_TYPE_DOUBLE;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_length = sizeof(double);
		    break;

		case SQL_C_CHAR:
		case SQL_C_BINARY:
		default:
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_type = MYSQL_TYPE_STRING;
		    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer_length = BufferLength;
	    }
	    ((MY_HSTMT*)StatementHandle)->columnP[ipar].buffer = TargetValue;
	    ((MY_HSTMT*)StatementHandle)->columnP[ipar].length = (long unsigned int*)StrLen_or_Ind;

	    // NOTE: This code cannot treat NULL.
	}
	else
	{
	    return SQL_ERROR;
	}
    }
    else
    {
	//Sorry... SQLBindCol() needs SQLPrepared() executed.
	return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLBindParam(SQLHSTMT StatementHandle,
			       SQLUSMALLINT ParameterNumber, SQLSMALLINT ValueType,
			       SQLSMALLINT ParameterType, SQLULEN LengthPrecision,
			       SQLSMALLINT ParameterScale, SQLPOINTER ParameterValue,
			       SQLLEN *StrLen_or_Ind)
{
    // We must use SQLBindParam() after SQLPrepare().

    SQLSMALLINT ipar = ParameterNumber - 1;

    if(ParameterNumber < 1)
    {
	return SQL_ERROR;
    }

    if(((MY_HSTMT*)StatementHandle)->prepared)
    {
	if(ipar < ((MY_HSTMT*)StatementHandle)->nParameters)
	{
	    switch(ValueType)
	    {
		case SQL_C_SBIGINT:
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_type = MYSQL_TYPE_LONGLONG;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].is_unsigned = FALSE;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_length = sizeof(long long);
		    break;
		case SQL_C_UBIGINT:
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_type = MYSQL_TYPE_LONGLONG;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].is_unsigned = TRUE;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_length = sizeof(long long);
		    break;

		case SQL_C_LONG:
		case SQL_C_SLONG:
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_type = MYSQL_TYPE_LONG;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].is_unsigned = FALSE;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_length = sizeof(int);
		    break;
		case SQL_C_ULONG:
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_type = MYSQL_TYPE_LONG;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].is_unsigned = TRUE;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_length = sizeof(int);
		    break;

		case SQL_C_SHORT:
		case SQL_C_SSHORT:
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_type = MYSQL_TYPE_SHORT;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].is_unsigned = FALSE;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_length = sizeof(short);
		    break;
		case SQL_C_USHORT:
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_type = MYSQL_TYPE_SHORT;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].is_unsigned = TRUE;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_length = sizeof(short);
		    break;

		case SQL_C_TINYINT:
		case SQL_C_STINYINT:
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_type = MYSQL_TYPE_TINY;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].is_unsigned = FALSE;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_length = sizeof(char);
		    break;

		case SQL_C_UTINYINT:
		case SQL_C_BIT:
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_type = MYSQL_TYPE_TINY;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].is_unsigned = TRUE;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_length = sizeof(char);
		    break;

		case SQL_C_DOUBLE:
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_type = MYSQL_TYPE_DOUBLE;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_length = sizeof(double);
		    break;

		case SQL_C_CHAR:
		case SQL_C_BINARY:
		default:
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_type = MYSQL_TYPE_STRING;
		    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer_length = LengthPrecision;
	    }
	    ((MY_HSTMT*)StatementHandle)->paramP[ipar].buffer = ParameterValue;
	    ((MY_HSTMT*)StatementHandle)->paramP[ipar].length = (long unsigned int*)StrLen_or_Ind;

	    // NOTE: This code cannot treat NULL.
	}
	else
	{
	    return SQL_ERROR;
	}
    }
    else
    {
	//Sorry... SQLBindParam() needs SQLPrepared() executed.
	return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT StatementHandle)
{
    if(StatementHandle)
    {
	if(((MY_HSTMT*)StatementHandle)->mysql_stmt)
	{
	    mysql_stmt_free_result(((MY_HSTMT*)StatementHandle)->mysql_stmt);
	}

	if(((MY_HSTMT*)StatementHandle)->state == ST_DIRECT_EXECUTED)
	{
	    if(((MY_HSTMT*)StatementHandle)->nParameters)
	    {
		free(((MY_HSTMT*)StatementHandle)->paramP);
		((MY_HSTMT*)StatementHandle)->nParameters = 0;
	    }

	    if(((MY_HSTMT*)StatementHandle)->nColumns)
	    {
		free(((MY_HSTMT*)StatementHandle)->columnP);
		((MY_HSTMT*)StatementHandle)->nColumns = 0;
	    }

	    ((MY_HSTMT*)StatementHandle)->state = ST_UNKNOWN;
	    ((MY_HSTMT*)StatementHandle)->prepared = FALSE;
	    ((MY_HSTMT*)StatementHandle)->res_binded = FALSE;
	}

	return SQL_SUCCESS;
    }
    else
    {
	return SQL_ERROR;
    }
}

SQLRETURN  SQL_API SQLConnect_DIRECT(SQLHDBC ConnectionHandle,
			      SQLCHAR *ServerName, SQLSMALLINT NameLength1,
			      SQLCHAR *DatabaseName, SQLSMALLINT NameLength4,
			      SQLCHAR *UserName, SQLSMALLINT NameLength2,
			      SQLCHAR *Authentication, SQLSMALLINT NameLength3)
{

    ((MY_HDBC*)ConnectionHandle)->mysql = mysql_init(NULL);
    if(((MY_HDBC*)ConnectionHandle)->mysql)
    {
	((MY_HDBC*)ConnectionHandle)->connected = TRUE;

	MYSQL* resp = mysql_real_connect(((MY_HDBC*)ConnectionHandle)->mysql,
				(const char*)ServerName, (const char*)UserName,
				(const char*)Authentication, (const char*)DatabaseName,
				3306, NULL, 0);

	if(resp)
	{
	    //NOTE: autocommit is disabled here.
	    mysql_autocommit(((MY_HDBC*)ConnectionHandle)->mysql, 0);
	    return SQL_SUCCESS;
	}
	else
	{
	    mysql_close(((MY_HDBC*)ConnectionHandle)->mysql);
	    ((MY_HDBC*)ConnectionHandle)->connected = FALSE;
	    return SQL_ERROR;
	}
    }
    else
    {
	return SQL_ERROR;
    }
}

SQLRETURN  SQL_API SQLDisconnect(SQLHDBC ConnectionHandle)
{
    if(((MY_HDBC*)ConnectionHandle)->connected)
    {
	mysql_close(((MY_HDBC*)ConnectionHandle)->mysql);
	((MY_HDBC*)ConnectionHandle)->connected = FALSE;
    }
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLEndTran(SQLSMALLINT HandleType, SQLHANDLE Handle,
			      SQLSMALLINT CompletionType)
{
    SQLRETURN result_code;
    int mysql_return;

    if(HandleType != SQL_HANDLE_DBC)
	return SQL_ERROR;

    switch(CompletionType)
    {
	case SQL_ROLLBACK:
	    mysql_return = mysql_rollback(((MY_HDBC*)Handle)->mysql);
	    break;
	case SQL_COMMIT:
	    mysql_return = mysql_commit(((MY_HDBC*)Handle)->mysql);
	    break;
	default:
	    return SQL_ERROR; //unknown
    }

    result_code = (mysql_return == 0) ? SQL_SUCCESS : SQL_ERROR;
    return result_code;
}

SQLRETURN  SQL_API SQLPrepare(SQLHSTMT StatementHandle,
                                  SQLCHAR *StatementText, SQLINTEGER TextLength)
{
    if(((MY_HSTMT*)StatementHandle)->prepared)
    {
	if(SQLCloseCursor(StatementHandle) == SQL_ERROR)
	{
	    return SQL_ERROR;
	}
    }

    int res = mysql_stmt_prepare(((MY_HSTMT*)StatementHandle)->mysql_stmt,(const char*)StatementText,strlen((const char*)StatementText));
    if(res){
	return SQL_ERROR;
    }

    ((MY_HSTMT*)StatementHandle)->nParameters = mysql_stmt_param_count(((MY_HSTMT*)StatementHandle)->mysql_stmt);

    ((MY_HSTMT*)StatementHandle)->meta_result = mysql_stmt_result_metadata(((MY_HSTMT*)StatementHandle)->mysql_stmt);
    if(((MY_HSTMT*)StatementHandle)->meta_result != 0){
	((MY_HSTMT*)StatementHandle)->nColumns = mysql_num_fields(((MY_HSTMT*)StatementHandle)->meta_result);
	mysql_free_result(((MY_HSTMT*)StatementHandle)->meta_result);
	((MY_HSTMT*)StatementHandle)->meta_result = 0;
    }else{
	((MY_HSTMT*)StatementHandle)->nColumns = 0;
    }

    if(((MY_HSTMT*)StatementHandle)->nParameters){
	((MY_HSTMT*)StatementHandle)->paramP = (MYSQL_BIND*)malloc( sizeof(MYSQL_BIND) * ((MY_HSTMT*)StatementHandle)->nParameters );
	memset(((MY_HSTMT*)StatementHandle)->paramP, 0, sizeof(MYSQL_BIND) * ((MY_HSTMT*)StatementHandle)->nParameters);
    }

    if(((MY_HSTMT*)StatementHandle)->nColumns){
	((MY_HSTMT*)StatementHandle)->columnP = (MYSQL_BIND*)malloc( sizeof(MYSQL_BIND) * ((MY_HSTMT*)StatementHandle)->nColumns );
	memset(((MY_HSTMT*)StatementHandle)->columnP, 0, sizeof(MYSQL_BIND) * ((MY_HSTMT*)StatementHandle)->nColumns);
    }

    ((MY_HSTMT*)StatementHandle)->state = ST_PREPARED;
    ((MY_HSTMT*)StatementHandle)->prepared = TRUE;

    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLExecute(SQLHSTMT StatementHandle)
{
    if(((MY_HSTMT*)StatementHandle)->nParameters){
	my_bool resb = mysql_stmt_bind_param(((MY_HSTMT*)StatementHandle)->mysql_stmt,((MY_HSTMT*)StatementHandle)->paramP);
	if(resb){
	    return SQL_ERROR;
	}
    }

    int mysql_return = mysql_stmt_execute(((MY_HSTMT*)StatementHandle)->mysql_stmt);
    if(mysql_return)
    {
	return SQL_ERROR;
    }

    if(((MY_HSTMT*)StatementHandle)->nColumns){
	int res = mysql_stmt_store_result(((MY_HSTMT*)StatementHandle)->mysql_stmt);
	if(res){
	    return SQL_ERROR;
	}
    }

    ((MY_HSTMT*)StatementHandle)->state = ST_EXECUTED;
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLExecDirect(SQLHSTMT StatementHandle,
				 SQLCHAR *StatementText, SQLINTEGER TextLength)
{
    if(SQLPrepare(StatementHandle, StatementText, TextLength) == SQL_ERROR)
    {
	return SQL_ERROR;
    }

    if(SQLExecute(StatementHandle) == SQL_ERROR)
    {
	return SQL_ERROR;
    }

    ((MY_HSTMT*)StatementHandle)->state = ST_DIRECT_EXECUTED;
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLRowCount(SQLHSTMT StatementHandle,
			       SQLLEN *RowCount)
{
    my_ulonglong row_count;

    row_count = mysql_stmt_affected_rows(((MY_HSTMT*)StatementHandle)->mysql_stmt);
    if(row_count < 0)
	row_count = 0;

    *RowCount = (SQLLEN) row_count;

    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLFetch(SQLHSTMT StatementHandle)
{
    if(mysql_stmt_num_rows(((MY_HSTMT*)StatementHandle)->mysql_stmt) == 0)
    {
	return SQL_NO_DATA_FOUND;
    }

    my_bool resb = mysql_stmt_bind_result(((MY_HSTMT*)StatementHandle)->mysql_stmt,((MY_HSTMT*)StatementHandle)->columnP);
    if(resb){
	return SQL_ERROR;
    }
    ((MY_HSTMT*)StatementHandle)->res_binded = TRUE;

    int mysql_return = mysql_stmt_fetch(((MY_HSTMT*)StatementHandle)->mysql_stmt);
    switch(mysql_return)
    {
	case 0: //SUCCESS
	    return SQL_SUCCESS;
	    break;
	case 1: //ERROR
	    return SQL_ERROR;
	    break;
	case MYSQL_NO_DATA: //NO MORE DATA
	    return SQL_NO_DATA_FOUND;
	    break;
	// Disabled for debugging
//	case MYSQL_DATA_TRUNCATED:
//	    return SQL_SUCCESS_WITH_INFO;
//	    break;
	default:
	    return SQL_ERROR;
    }
}

SQLRETURN  SQL_API SQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle)
{
    switch(HandleType)
    {
	case SQL_HANDLE_ENV:
	    //dummy
	    return SQL_SUCCESS;
	    break;

	case SQL_HANDLE_DBC:
	    if(Handle)
	    {
		if(((MY_HDBC*)Handle)->connected)
		{
		    mysql_close(((MY_HDBC*)Handle)->mysql);
		    ((MY_HDBC*)Handle)->connected = FALSE;
		}
		free(Handle);
	    }
	    return SQL_SUCCESS;
	    break;

	case SQL_HANDLE_STMT:
	    if(Handle)
	    {
		if(((MY_HSTMT*)Handle)->meta_result)
		{
		    mysql_free_result(((MY_HSTMT*)Handle)->meta_result);
		    ((MY_HSTMT*)Handle)->meta_result = 0;
		}

		if(((MY_HSTMT*)Handle)->mysql_stmt)
		{
		    mysql_stmt_free_result(((MY_HSTMT*)Handle)->mysql_stmt);
		    mysql_stmt_close(((MY_HSTMT*)Handle)->mysql_stmt);
		    ((MY_HSTMT*)Handle)->mysql_stmt = 0;
		}

		if(((MY_HSTMT*)Handle)->nParameters)
		{
		    free(((MY_HSTMT*)Handle)->paramP);
		    ((MY_HSTMT*)Handle)->nParameters = 0;
		}

		if(((MY_HSTMT*)Handle)->nColumns)
		{
		    free(((MY_HSTMT*)Handle)->columnP);
		    ((MY_HSTMT*)Handle)->nColumns = 0;
		}

		free(Handle);
	    }
	    return SQL_SUCCESS;
	    break;

	default:
	    return SQL_ERROR;
    }
    return SQL_ERROR;
}

SQLRETURN  SQL_API SQLSetConnectAttr(SQLHDBC ConnectionHandle,
				     SQLINTEGER Attribute, SQLPOINTER Value,
				     SQLINTEGER StringLength)
{
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLSetEnvAttr(SQLHENV EnvironmentHandle,
				 SQLINTEGER Attribute, SQLPOINTER Value,
				 SQLINTEGER StringLength)
{
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                     SQLSMALLINT RecNumber, SQLCHAR *Sqlstate,
                                     SQLINTEGER *NativeError, SQLCHAR *MessageText,
                                     SQLSMALLINT BufferLength, SQLSMALLINT *TextLength)
{
    if(RecNumber > 1)
	return SQL_NO_DATA;

    switch(HandleType)
    {
	case SQL_HANDLE_ENV:
	    // dummy
	    return SQL_ERROR;
	    break;

	case SQL_HANDLE_DBC:
	    if(RecNumber != 1)
		return SQL_ERROR;
	    strncpy((char*)Sqlstate, mysql_sqlstate(((MY_HDBC*)Handle)->mysql), SQL_SQLSTATE_SIZE+1);
	    *NativeError = mysql_errno(((MY_HDBC*)Handle)->mysql);
	    strncpy((char*)MessageText, mysql_error(((MY_HDBC*)Handle)->mysql), BufferLength);
	    return SQL_SUCCESS;
	    break;

	case SQL_HANDLE_STMT:
	    if(RecNumber != 1)
		return SQL_ERROR;
	    strncpy((char*)Sqlstate, mysql_stmt_sqlstate(((MY_HSTMT*)Handle)->mysql_stmt), SQL_SQLSTATE_SIZE+1);
	    *NativeError = mysql_stmt_errno(((MY_HSTMT*)Handle)->mysql_stmt);
	    strncpy((char*)MessageText, mysql_stmt_error(((MY_HSTMT*)Handle)->mysql_stmt),BufferLength );
	    return SQL_SUCCESS;
	    break;

	default:
	    return SQL_ERROR;
    }
    return SQL_ERROR;
}
#endif

