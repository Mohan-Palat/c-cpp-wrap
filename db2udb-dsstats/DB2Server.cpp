// U\
// *********************************************************************** 
// *                                                                     *
// * CLI Wrapper for C API Calls in DB2Server Class          Mohan Palat *
// *                                                                     *
// * Libraries Required in Windows :                                     * 
// *       db2cli.lib, asnlib.lib, db2api.lib, db2apie.lib               *
// *                                                                     *
// * Compiling in UNIX                                                   *
// * -----------------                                                   *
// * # Need the $DB2DIR=/usr/opt/db2_08_01                               *
// * DB2PATH=/usr/opt/db2_08_01                                          *
// * LIB=lib                                                             *
// * # Compile the program.                                              *
// * xlC $EXTRA_CFLAG -I$DB2PATH/include -c $oos.cpp                     *
// * (Ensure that the C in xlC is in caps. (lower case is a C compiler)  *
// *********************************************************************** 
// Address
// o SQL_SUCCESS_WITH_INFO.
// o SQLGetDiagField() not returning estimates.
// o Auto Commit as property setting
// o Continue On Error property
// o CurrSql()
// ***********************************************************************

#include "DB2Server.h"

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif


void DB2Server::SetDebugMode(bool mode)
{
    debug_mode = mode;
    return; 
}

char * DB2Server::dbg_prefix(void)
{
    time_t ltime;
    time(&ltime);
    struct tm *today;
    today = localtime(&ltime);
    memset(dbg_pfx, '\0', BUFSIZ);
    strftime(dbg_pfx, 27, "\n[%Y-%m-%d %H:%M:%S]", today);
    strcat(dbg_pfx, "[DBG]");
    strcat(dbg_pfx, server_handle);

    return dbg_pfx;
}

char * DB2Server::err_prefix(void)
{
    time_t ltime;
    time(&ltime);
    struct tm *today;
    today = localtime(&ltime);
    memset(err_pfx, '\0', BUFSIZ);
    strftime(err_pfx, 27, "\n[%Y-%m-%d %H:%M:%S]", today);
    strcat(err_pfx, "[ERR]");
    strcat(err_pfx, server_handle);

    return err_pfx;
}

SQLSMALLINT DB2Server::SqlType2CType(SQLSMALLINT st) // SQL type to map to a C type
{
    switch(st) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
            return SQL_C_CHAR;
            
        case SQL_DECIMAL:
        case SQL_NUMERIC:
            return SQL_C_DOUBLE; // Can be SQL_C_CHAR

        case SQL_BIGINT:
            return SQL_C_LONG; // Can be SQL_C_CHAR

        case SQL_BIT:
            return SQL_C_BIT;
            
        case SQL_TINYINT:
            return SQL_C_TINYINT;
            
        case SQL_SMALLINT:
            return SQL_C_SHORT;
            
        case SQL_INTEGER:
            return SQL_C_LONG;
        
        case SQL_REAL:
            return SQL_C_FLOAT;
            
        case SQL_FLOAT:
        case SQL_DOUBLE:
            return SQL_C_DOUBLE;
            
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
            return SQL_C_BINARY;
            
        case SQL_DATE:
            return SQL_C_DATE;
            
        case SQL_TIME:
            return SQL_C_TIME;
            
        case SQL_TIMESTAMP:
            return SQL_C_TIMESTAMP;
            
        default:
            return SQL_C_DEFAULT;
    }
}

char * DB2Server::data_type(SQLSMALLINT dt)
{
    static char dtc[BUFSIZ];

         if (dt == SQL_BIGINT) strcpy(dtc, "SQL_BIGINT");
    else if (dt == SQL_BLOB) strcpy(dtc, "SQL_BLOB");
    else if (dt == SQL_BLOB_LOCATOR) strcpy(dtc, "SQL_BLOB_LOCATOR");
    else if (dt == SQL_CHAR) strcpy(dtc, "SQL_CHAR");
    else if (dt == SQL_TINYINT) strcpy(dtc, "SQL_TINYINT");
    else if (dt == SQL_BINARY) strcpy(dtc, "SQL_BINARY");
    else if (dt == SQL_BIT) strcpy(dtc, "SQL_BIT");
    else if (dt == SQL_CLOB) strcpy(dtc, "SQL_CLOB");
    else if (dt == SQL_CLOB_LOCATOR) strcpy(dtc, "SQL_CLOB_LOCATOR");
    else if (dt == SQL_DATALINK) strcpy(dtc, "SQL_DATALINK");
    else if (dt == SQL_TYPE_DATE) strcpy(dtc, "SQL_TYPE_DATE");
    else if (dt == SQL_DBCLOB) strcpy(dtc, "SQL_DBCLOB");
    else if (dt == SQL_DBCLOB_LOCATOR) strcpy(dtc, "SQL_DBCLOB_LOCATOR");
    else if (dt == SQL_DECIMAL) strcpy(dtc, "SQL_DECIMAL");
    else if (dt == SQL_DOUBLE) strcpy(dtc, "SQL_DOUBLE");
    else if (dt == SQL_FLOAT) strcpy(dtc, "SQL_FLOAT");
    else if (dt == SQL_GRAPHIC) strcpy(dtc, "SQL_GRAPHIC");
    else if (dt == SQL_INTEGER) strcpy(dtc, "SQL_INTEGER");
    else if (dt == SQL_LONGVARCHAR) strcpy(dtc, "SQL_LONGVARCHAR");
    else if (dt == SQL_LONGVARBINARY) strcpy(dtc, "SQL_LONGVARBINARY");
    else if (dt == SQL_LONGVARGRAPHIC) strcpy(dtc, "SQL_LONGVARGRAPHIC");
    else if (dt == SQL_WLONGVARCHAR) strcpy(dtc, "SQL_WLONGVARCHAR");
    else if (dt == SQL_NUMERIC) strcpy(dtc, "SQL_NUMERIC");
    else if (dt == SQL_REAL) strcpy(dtc, "SQL_REAL");
    else if (dt == SQL_SMALLINT) strcpy(dtc, "SQL_SMALLINT");
    else if (dt == SQL_TYPE_TIME) strcpy(dtc, "SQL_TYPE_TIME");
    else if (dt == SQL_TYPE_TIMESTAMP) strcpy(dtc, "SQL_TYPE_TIMESTAMP");
    else if (dt == SQL_VARCHAR) strcpy(dtc, "SQL_VARCHAR");
    else if (dt == SQL_VARBINARY) strcpy(dtc, "SQL_VARBINARY");
    else if (dt == SQL_VARGRAPHIC) strcpy(dtc, "SQL_VARGRAPHIC");
    else if (dt == SQL_WVARCHAR) strcpy(dtc, "SQL_WVARCHAR");
    else if (dt == SQL_WCHAR) strcpy(dtc, "SQL_WCHAR");

    return dtc;
}

bool DB2Server::CheckRetCode(SQLRETURN retcode, SQLSMALLINT htype, SQLHANDLE hndl)
{
    bool rc = false;
    switch (retcode) {
        case SQL_SUCCESS:
            rc = false;
            break;
        case SQL_INVALID_HANDLE:
            fprintf(logfp, "%s -CLI INVALID HANDLE-----", err_prefix());
            rc = true;
            break;
        case SQL_ERROR:
            fprintf(logfp, "%s -CLI SQL ERROR--------------", err_prefix());
            Diagnostics(htype, hndl);
            rc = true;
            break;
        case SQL_SUCCESS_WITH_INFO:
            rc = false;
            break;
        case SQL_STILL_EXECUTING:
            rc = false;
            break;
        case SQL_NEED_DATA:
            rc = false;
            break;
        case SQL_NO_DATA_FOUND:
            rc = false;
            break;
        default:
            fprintf(logfp, "%s -CLI UNKNOWN ERROR---------", err_prefix());
            rc = true;
            break;
    }
    return rc;    
}

void DB2Server::Diagnostics(SQLSMALLINT htype, SQLHANDLE hndl)
{
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR sqlstate[SQL_SQLSTATE_SIZE + 1];
    SQLINTEGER sqlcode;
    SQLSMALLINT length, i;

    fprintf(logfp, "%s -------Diagnostics Begin----------------", err_prefix());
    i = 1;
    /* get multiple field settings of diagnostic record */
    while (SQLGetDiagRec(htype, hndl, i, sqlstate, &sqlcode, message,
                       SQL_MAX_MESSAGE_LENGTH + 1, &length) == SQL_SUCCESS) {
        fprintf(logfp, "%s SQLSTATE          = %s",err_prefix(),sqlstate);
        fprintf(logfp, "%s Native Error Code = %d\n",err_prefix(),sqlcode);
        fprintf(logfp, "%s %s",err_prefix(),message);
        i++;
    }
    fprintf(logfp, "%s --------Diagnostics End-----------------", err_prefix());
}

DB2Server::DB2Server(FILE *plogfp)
{
    RetCode = SQL_SUCCESS;
    EnvHandle=0;
    num_cols=0;
    strcpy(dbAlias, " ");
    SetDebugMode(false);
    *server_handle='\0';

    logfp = plogfp ? plogfp : stderr;

    fprintf(logfp, "%s SQLAllocHandle(void constructor).", dbg_prefix());

    RetCode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &EnvHandle);
    if (CheckRetCode(RetCode, SQL_HANDLE_ENV, EnvHandle)) {
        fprintf(logfp, "%s Unable to SQLAllocHandle(void constructor). RetCode = %d\n", err_prefix(), RetCode);
        exit(1);
    }
    if (!EnvHandle) {
        fprintf(logfp, "%s The Environment Handle is %d", err_prefix(), EnvHandle);
        exit(1);
    }

    fprintf(logfp, "%s Set SQLSetEnvAttr(ODBC Version 3.x).", dbg_prefix());
    RetCode = SQLSetEnvAttr(EnvHandle, SQL_ATTR_ODBC_VERSION,
                             (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_UINTEGER);
    if (CheckRetCode(RetCode, SQL_HANDLE_ENV, EnvHandle)) {
        fprintf(logfp, "%s Setting SQLSetEnvAttr(ODBC Version 3.x). RetCode = %d\n", err_prefix(), RetCode);
        exit(1);
    }

    connected = false;

    return;
}

DB2Server::~DB2Server()
{
    if (connected)
        Disconnect();

    if (EnvHandle) {   
        if (debug_mode)
            fprintf(logfp, "%s Executing in SQLFreeHandle().\n", dbg_prefix());
        RetCode = SQLFreeHandle(SQL_HANDLE_ENV, EnvHandle);
        if (CheckRetCode(RetCode, SQL_HANDLE_ENV, EnvHandle)) {
            fprintf(logfp, "%s Executing in SQLFreeHandle(). RetCode = %d", err_prefix(), RetCode);
        }
    }
}

DB2Server::DB2Server(char *pdb, char *puser, char *ppswd, FILE *plogfp, char *phandle, 
                                   bool pdebug_mode, int num_tries)
{
    RetCode = SQL_SUCCESS;
    EnvHandle=0;
    num_cols=0;
    strcpy(dbAlias, " ");
    SetDebugMode(false);

    if (phandle)
        strcpy(server_handle, phandle);
    else
        *server_handle='\0';

    logfp = plogfp ? plogfp : stderr;

    fprintf(logfp, "%s Executing SQLAllocHandle(Params Constructor).", dbg_prefix());
    RetCode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &EnvHandle);
    if (CheckRetCode(RetCode, SQL_HANDLE_ENV, EnvHandle)) {
        fprintf(logfp, "%s Executing SQLAllocHandle(Params Constructor). RetCode = %d", err_prefix(), RetCode);
        exit(1);
    }
    if (!EnvHandle) {
        fprintf(logfp, "%s The Environment Handle is %d", err_prefix(), EnvHandle);
        exit(1);
    }

    fprintf(logfp, "%s Executing SQLSetEnvAttr(ODBC Version to 3.x).", dbg_prefix());
    RetCode = SQLSetEnvAttr(EnvHandle, SQL_ATTR_ODBC_VERSION,
                             (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_UINTEGER);
    if (CheckRetCode(RetCode, SQL_HANDLE_ENV, EnvHandle)) {
        fprintf(logfp, "%s Executing SQLSetEnvAttr(ODBC Version to 3.x). RetCode = %d\n", err_prefix(), RetCode);
        exit(1);
    }

    if (Connect(pdb, puser, ppswd, phandle, pdebug_mode, num_tries))
        exit(1);
}

bool DB2Server::GetConnProp(char *prop_file, char *pdb, char *puser)
{
    //
    // Search for CONNECTION, USER, PASSWORD
    //

    FILE *pfp;

    if (!(pfp=fopen(prop_file, "rt"))) {
        fprintf(logfp, "%s Opening %s", err_prefix(), prop_file);
        return true;
    }

    if (debug_mode)
        fprintf(logfp, "%s Loading prop file %s for %s in %s.", 
                                    dbg_prefix(), prop_file, puser, pdb);

    char par[BUFSIZ], val[BUFSIZ], buf[BUFSIZ];
    char *udb = pdb ? strdup(pdb) : NULL;

    // DB Property (Parameter - CONNECTION)

    *dbAlias = '\0';
    while(fgets(buf, BUFSIZ, pfp)) {
        if (sscanf(buf,"%s%s", par, val)==2) {
            upper(par); upper(val); 
            if(!strcmp(upper(par), "CONNECTION")) {
                if (pdb) { // Specific connection from a list
                    if (strcmp(val, upper(udb)))
                        continue;
                    else {
                        strcpy(dbAlias, val);
                        break;
                    }
                } else { // Pick the first user entry
                    strcpy(dbAlias, val);
                    break;
                }
            }
        }
    }
    if (udb)
        free(udb);
    if (*dbAlias == '\0') {
        fprintf(logfp, "%s %s not in %s", err_prefix(), pdb, prop_file);
        return true;
    }

    // USER Property (Parameter - USER)

    *user = '\0';
    while(fgets(buf, BUFSIZ, pfp)) {
        if (sscanf(buf,"%s%s", par, val)==2) { 
            if(!strcmp(upper(par), "USER")) {
                if (puser) { // Specific user from a list
                    if (strcmp(val, puser)) 
                        continue;
                    else {
                        strcpy(user, val);
                        break;
                    }    
                } else { // Pick the first user entry
                    strcpy(user, val);
                    break;
                }
            }
        }
    }
    if (*user == '\0') {
        if (puser)
            fprintf(logfp, "%s %s not in %s", err_prefix(), puser, prop_file);
        else
            fprintf(logfp, "%s No users in %s", err_prefix(), prop_file);
        return true;
    }

    // PASSWD Property (Parameter PASSWORD)

    *pswd = '\0';
    while(fgets(buf, BUFSIZ, pfp)) {
        if (sscanf(buf,"%s%s", par, val)==2) { 
            if(!strcmp(upper(par), "PASSWORD")) {
                strcpy(pswd, val);
                break;
            }
        }
    }
    if (*pswd == '\0') {
        fprintf(logfp, "%s Password for %s not in %s", err_prefix(), user, prop_file);
        return true;
    }

    fclose(pfp);

    return false;
}

bool DB2Server::Connect(char *pdb, char *puser, char *ppswd, char *phandle, 
                                   bool pdebug_mode, int num_tries)
{
    if (pdb)
        strcpy(dbAlias, pdb);
    if (puser)
        strcpy(user, puser);
    if (ppswd)
        strcpy(pswd, ppswd);

    fprintf(logfp, "%s Connecting to '%s' User '%s'.", dbg_prefix(), dbAlias, user);

    fprintf(logfp, "%s Setting Debug Mode to '%s'", dbg_prefix(), pdebug_mode ? "TRUE" : "FALSE");
    
    SetDebugMode(pdebug_mode);

    if (debug_mode)
        fprintf(logfp, "%s Executing SQLAllocHandle(DB Conn).", dbg_prefix());
    RetCode = SQLAllocHandle(SQL_HANDLE_DBC, EnvHandle, &DBConHandle);
    if (CheckRetCode(RetCode, SQL_HANDLE_DBC, DBConHandle)) {
        fprintf(logfp, "%s \nUnable to allocate DB Connection Handle. RetCode = %d\n", err_prefix(), RetCode);
        return true;
    }

    if (debug_mode)
        fprintf(logfp, "%s Setting SQLSetConnectAttr(Autocommit).", dbg_prefix());
    RetCode = SQLSetConnectAttr(DBConHandle, SQL_ATTR_AUTOCOMMIT,
                     (SQLPOINTER)SQL_AUTOCOMMIT_ON, SQL_NTS); // or SQL_AUTOCOMMIT_OFF
    if (CheckRetCode(RetCode, SQL_HANDLE_DBC, DBConHandle)) {
        fprintf(logfp, "%s Setting SQLSetConnectAttr(Autocommit). RetCode = %d\n", err_prefix(), RetCode);
        return true;
    }

    if (debug_mode)
        fprintf(logfp, "%s SQLConnect(%s). User %s...", dbg_prefix(), dbAlias, user);
    RetCode = SQLConnect(DBConHandle, (SQLCHAR *)dbAlias, SQL_NTS,
                                (SQLCHAR *)user, SQL_NTS, (SQLCHAR *)pswd, SQL_NTS);
    if (CheckRetCode(RetCode, SQL_HANDLE_DBC, DBConHandle)) {
        fprintf(logfp, "%s Unable SQLConnect(). RetCode = %d", err_prefix(), RetCode);
        return true;
    }

    if (debug_mode)
        fprintf(logfp, "%s SQLAllocHandle(Statement)", dbg_prefix());
    RetCode = SQLAllocHandle(SQL_HANDLE_STMT, DBConHandle, &StmtHandle);
    if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
        fprintf(logfp, "%s SQLAllocHandle(Statement). RetCode = %d",err_prefix(), RetCode);
        return true;
    }

    connected = true;
    sql = NULL;
    old_sql = NULL;

    return false;
}

bool DB2Server::Disconnect(void)
{
    if (StmtHandle) {
        if (debug_mode)
            fprintf(logfp, "%s Executing SQLFreeHandle(Statement).", dbg_prefix());
        RetCode = SQLFreeHandle(SQL_HANDLE_STMT, StmtHandle);
        if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
            fprintf(logfp, "%s Executing SQLFreeHandle(Statement). RetCode = %d", err_prefix(), RetCode);
            return true;
        }
    }

    if (DBConHandle) {
        if (debug_mode)
            fprintf(logfp, "%s Executing SQLDisconnect(%s)...", dbg_prefix(), dbAlias);
        RetCode = SQLDisconnect(DBConHandle);
        if (CheckRetCode(RetCode, SQL_HANDLE_DBC, DBConHandle)) {
            fprintf(logfp, "%s Unable to SQLDisconnect(). RetCode = %d\n", err_prefix(), RetCode);
            return true;
        }
        if (debug_mode)
            fprintf(logfp, "%s Executing SQLFreeHandle(DB Conn Handle).", dbg_prefix());
        RetCode = SQLFreeHandle(SQL_HANDLE_DBC, DBConHandle);
        if (CheckRetCode(RetCode, SQL_HANDLE_DBC, DBConHandle)) {
            fprintf(logfp, "%s Bad SQLFreeHandle(DB Conn Handle).. RetCode = %d", err_prefix(), RetCode);
            return true;
        }
    }
    
    strcpy(dbAlias, " ");
    
    connected = false;

    return false;
}


bool DB2Server::Bind(int col_num, void *bind_var, SQLINTEGER bind_len)
{
    SQLCHAR     c[BUFSIZ+1];
    SQLSMALLINT nl;
    SQLSMALLINT dt;
    SQLUINTEGER cs;
    SQLSMALLINT dd;
    SQLSMALLINT n;

    if (debug_mode)
        fprintf(logfp, "%s Binding Col # %d", dbg_prefix(), col_num);

    RetCode = SQLDescribeCol(StmtHandle, col_num, c, BUFSIZ, &nl, &dt, &cs, &dd, &n);
    if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
        fprintf(logfp, "%s Executing SQLDescribeCol(%d). RetCode = %d", err_prefix(), col_num, RetCode);
        return true;
    }

    SQLSMALLINT ct = SqlType2CType(dt);

    if (debug_mode)
        fprintf(logfp, "%s Executing SQLBindCol(%d) BufLen is %d", dbg_prefix(), col_num, bind_len);

    RetCode = SQLBindCol(StmtHandle, col_num, ct, bind_var, bind_len, NULL);
    if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
        fprintf(logfp, "%s Executing SQLBindCol(%d). RetCode = %d", err_prefix(), col_num, RetCode);
        return true;
    }

    return false;
}

SQLRETURN DB2Server::Fetch(void)
{
    RetCode = SQLFetch(StmtHandle);
    if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
        fprintf(logfp, "%s Unable to SQLFetch(). RetCode, %d", err_prefix(), RetCode);
        return SQL_ERROR;
    }
    return RetCode;
}

SQLRETURN DB2Server::CloseCursor(void)
{
    if (debug_mode)
        fprintf(logfp, "%s Executing SQLCloseCursor()", dbg_prefix());

    RetCode = SQLCloseCursor(StmtHandle);
    if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
        fprintf(logfp, "%s Unable to SQLCloseCursor(). RetCode, %d", err_prefix(), RetCode);
        return SQL_ERROR;
    }
    return RetCode;
}

bool DB2Server::CommitTran(void)
{     
    if (debug_mode)
        fprintf(logfp, "%s Executing SQLEndTran(SQL_COMMIT).", dbg_prefix());

    RetCode = SQLEndTran(SQL_HANDLE_DBC, DBConHandle, SQL_COMMIT); 
    if (CheckRetCode(RetCode, SQL_HANDLE_DBC, DBConHandle)) {
        fprintf(logfp, "%s SQLEndTran(SQL_COMMIT) Failed. RetCode = %d", err_prefix(), RetCode);
        return true;
    }

    return false;
}

bool DB2Server::RollbackTran(void)
{     
    if (debug_mode)
        fprintf(logfp, "%s Executing SQLEndTran(SQL_ROLLBACK).", dbg_prefix());

    RetCode = SQLEndTran(SQL_HANDLE_DBC, DBConHandle, SQL_ROLLBACK);
    if (CheckRetCode(RetCode, SQL_HANDLE_DBC, DBConHandle)) {
        fprintf(logfp, "%s SQLEndTran(SQL_ROLLBACK) Failed. RetCode = %d", err_prefix(), RetCode);
        return true;
    }

    return false;
}

bool DB2Server::SetSql(const char *format, ...)
{
    char psql[BUFSIZ*4];
    va_list args;
    va_start(args, format);
    vsprintf(psql, format, args);
    va_end(args);

    char *temp_sql;

    if (debug_mode)
            fprintf(logfp, "%s SetSql().\n", dbg_prefix());

    // If Sql is already set, append to it
    if (sql) {
        temp_sql = strdup(sql);
        if (!temp_sql) {
            fprintf(logfp, "%s strdup(sql) Failed.\n", err_prefix());
            return true;
        }  
        
        free(sql);
        // Additional spaces for a blank space and \0
        sql = (char *) malloc(sizeof(char) * (strlen(temp_sql)+strlen(psql)+2));
        if (!sql) {
            fprintf(logfp, "%s malloc(1) Failed.\n", err_prefix());
            return true;
        }  
        strcpy(sql, temp_sql);
        strcat(sql, "\n");
        strcat(sql, psql);
        free(temp_sql);
    } else {
        sql = strdup(psql);
        if (!sql) {
            fprintf(logfp, "%s malloc(2) Failed.\n", err_prefix());
            return true;
        }  
    }

    return false;
}
bool DB2Server::ReadSql(char *file_name)
{
    if (debug_mode)
            fprintf(logfp, "%s ReadSql() from '%s'.\n", dbg_prefix(), file_name);
    FILE *sfp;
    if (!(sfp=fopen(file_name, "rt")))  {
        fprintf(logfp, "%s Unable to Read SQL File %s.\n", err_prefix(), file_name);
        return true;
    }  

    char buf[BUFSIZ];
    while(fgets(buf, BUFSIZ, sfp)) {
        if (debug_mode)
            fprintf(logfp, "%s ReadSql({%s}).\n", dbg_prefix(), buf);
        if (SetSql(buf)) {
            ClearSql();
            fclose(sfp);
            return true;
        }
    }

    fclose(sfp);

    return false;
}

// ********************************
// Remove any SQL set in the object
// ********************************

bool DB2Server::ClearSql(void)
{
    if (debug_mode)
            fprintf(logfp, "%s ClearSql().\n", dbg_prefix());

    if (sql) {
        if (old_sql)
            free(old_sql);
        old_sql = strdup(sql);
        if (!old_sql) {
            fprintf(logfp, "%s strdup(old_sql) Failed.\n", err_prefix());
            return true;
        }  
        free(sql);
        sql = NULL;
    } 

    return false;
}


bool DB2Server::Prepare(void)
{
    if (debug_mode)
        fprintf(logfp, "%s Executing SQLPrepare()", dbg_prefix());
    RetCode = SQLPrepare(StmtHandle, (SQLCHAR *)sql, SQL_NTS);
    if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
        fprintf(logfp, "%s \nUnable to SQLPrepare(). RetCode = %d\n", err_prefix(), RetCode);
        return true;
    }

    return false;
}

bool DB2Server::BindInpCharParam( SQLUSMALLINT par_num,
                                  SQLPOINTER   par_val_ptr,
                                  SQLUINTEGER  col_siz,
                                  SQLINTEGER   buf_len
                                )
{
    if (debug_mode)
        fprintf(logfp, "%s Executing BindCharParam()", dbg_prefix());
    if (BindParameter(par_num, SQL_PARAM_INPUT, SQL_C_CHAR,
                SQL_CHAR, col_siz, 0, par_val_ptr, buf_len, NULL))
        return true;
   
    return false;
}
                     

bool DB2Server::BindParameter( SQLUSMALLINT par_num,
                               SQLSMALLINT  io_type,
                               SQLSMALLINT  val_type,
                               SQLSMALLINT  par_type,
                               SQLUINTEGER  col_siz,
                               SQLSMALLINT  dec_dig,
                               SQLPOINTER   par_val_ptr,
                               SQLINTEGER   buf_len,
                               SQLINTEGER   *s_or_i
                             )
{
    if (debug_mode)
        fprintf(logfp, "%s Executing SQLBindParameter()", dbg_prefix());
    RetCode = SQLBindParameter(StmtHandle, (unsigned short) par_num, io_type, val_type,
        par_type, col_siz, dec_dig, par_val_ptr, buf_len, s_or_i);
    if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
        fprintf(logfp, "%s \nUnable to SQLBindParameter(). RetCode = %d\n", 
                           err_prefix(), RetCode);
        return true;
    }
   
    return false;
}
                               
                               

bool DB2Server::Exec(bool collect_stat)
{
    if (debug_mode) 
        printf("\nSQL BEING EXECUTED IS:\n<BOS>\n%s\n<EOS>\n", sql);

    if (debug_mode) 
        fprintf(logfp, "%s Executing SQLExecDirect()", dbg_prefix());
    RetCode = SQLExecDirect(StmtHandle, (SQLCHAR *)sql, SQL_NTS);
    if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
        fprintf(logfp, "%s \nUnable to SQLExecDirect(). RetCode = %d\n", err_prefix(), RetCode);
        return true;
    }

    if (!collect_stat)
        return false;

    if (debug_mode)
        fprintf(logfp, "%s Executing SQLNumResultCols()", dbg_prefix()); 
    num_cols=0;
    RetCode = SQLNumResultCols(StmtHandle, &num_cols);
    if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
        fprintf(logfp, "%s \nUnable to execute SQLNumResultCols(). RetCode = %d", err_prefix(), RetCode);
        return true;
    }

    if (debug_mode)
        if (DisplayColProps())
            return true;

    RetCode = SQLRowCount(StmtHandle, &num_rows);
    if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
        fprintf(logfp, "%s \nUnable to execute SQLRowCount(). RetCode = %d", err_prefix(), RetCode);
        return true;
    }

    return false;
}

SQLSMALLINT DB2Server::GetNumCols(void)
{
    return num_cols;
}

bool DB2Server::DisplayColProps(void)
{
    SQLCHAR     c[BUFSIZ+1];
    SQLSMALLINT nl;
    SQLSMALLINT dt;
    SQLUINTEGER cs;
    SQLSMALLINT dd;
    SQLSMALLINT n;

    for (int cn=1; cn<=GetNumCols(); cn++) {
        RetCode = SQLDescribeCol(StmtHandle, cn, c, BUFSIZ, &nl, &dt, &cs, &dd, &n);
        if (CheckRetCode(RetCode, SQL_HANDLE_STMT, StmtHandle)) {
            fprintf(logfp, "%s Unable to SQLDescribeCol(col#=%d). RetCode = %d\n", err_prefix(), cn, RetCode);
            return true;
        }
        fprintf(logfp, "\n\t\tDescribing Column %d of %d", cn, GetNumCols());
        fprintf(logfp, "\n\t\t------------------------------");
        fprintf(logfp, "\n\t\tColumnName %s", c);
        fprintf(logfp, "\n\t\tNameLength %d", nl);
        fprintf(logfp, "\n\t\tDataType %s", data_type(dt));
        fprintf(logfp, "\n\t\tColumnSize %d", cs);
        fprintf(logfp, "\n\t\tDecimalDigits %d", dd);
        fprintf(logfp, "\n\t\tNullable %d", n);
    }

    return false;
}

bool DB2Server::SetLogFile(FILE *fp, char *file_name)
{
    strcpy(logfname, file_name);
    fprintf(logfp, "%s '%s' Log File - Switching to %s.", dbg_prefix(), dbAlias, logfname);
    logfp=fp;

    return false;
}

void DB2Server::Rtrim(SQLCHAR *str)
{
    char tstr[BUFSIZ*4];
    sprintf(tstr, "%s", str);
    
    while(strlen(tstr))
        if (isspace(*(tstr+strlen(tstr)-1)))
            *(tstr+strlen(tstr)-1) = '\0';
        else
            break;
    SQLCHAR *t=str;
    for (char *f=tstr; *f; f++) {
        *t = *f;
        t++;
    }
    *t = '\0';
        
    return;
}

char * DB2Server::SqlChar2Char(SQLCHAR *str)
{
    static char tstr[BUFSIZ*4];
    sprintf(tstr, "%s", str);
       
    return tstr;
}


