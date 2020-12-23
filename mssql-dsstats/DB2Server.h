// U\
// *********************************************************************** 
// *                                                                     *
// * CLI Wrapper for C API Calls in DB2Server Class          Mohan Palat *
// *                                                            05/07/06 *
// * Libraries Required in Windows :                                     * 
// *       db2cli.lib, asnlib.lib, db2api.lib, db2apie.lib               *
// *                                                                     *
// * Compiling in UNIX                                                   *
// * -----------------                                                   *
// * # Need the $DB2DIR=/usr/opt/db2_08_01                               *
// * DB2PATH=/usr/opt/db2_08_01                                          *
// * LIB=lib                                                             *
// * # Compile the program.                                              *
// * xlC $EXTRA_CFLAG -I$DB2PATH/include -c $1.cpp                       *
// * # Link the program.                                                 *
// * xlC -o $1 $1.o -L$DB2PATH/$LIB -ldb2                                *
// *                                                                     *
// *********************************************************************** 

#ifndef _OOS_HEADER_
#define _OOS_HEADER_
#define _USE_32BIT_TIME_T

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstdarg>
#include <cctype>
#include <ctime>

// #include <windows.h> // MSSQLServer
#include <odbcinst.h>
#include <sqlext.h> // MSSQLServer

#define MAX_UID_LENGTH 18
#define MAX_PWD_LENGTH 30
#define MAX_STMT_LEN 255
#define MAX_COLUMNS 255
#ifdef DB2WIN
#define MAX_TABLES 50
#else
#define MAX_TABLES 255
#endif

#define MAXSQLSIZ BUFSIZ*8

class DB2Server {
    private: 
        SQLHANDLE EnvHandle;   // environment handle 
        SQLHANDLE DBConHandle; // connection handle  
        SQLHANDLE StmtHandle;  // statement handle   
        SQLRETURN RetCode;
        char server_handle[BUFSIZ];
        char dbAlias[SQL_MAX_DSN_LENGTH + 1]; // Server
        char user[MAX_UID_LENGTH + 1];
        char pswd[MAX_PWD_LENGTH + 1];
        // char SqlStmt[MAXSQLSIZ + 1];
        SQLSMALLINT num_cols;
        SQLLEN num_rows; // SQLINTEGER num_rows; 2017-07-21 Compiling with odbc library and include from folder /usr
        bool auto_commit;
        bool connected;
        FILE *logfp;
        char logfname[BUFSIZ];
        char err_pfx[BUFSIZ], dbg_pfx[BUFSIZ];
        bool debug_mode;
        char *sql;
        char *old_sql;
        char *upper(char *s) {char *p=s; while(*p) {*p=toupper(*p);p++;} return s;}
    public:
        DB2Server(FILE *plogfp=NULL); // Contruct
        DB2Server(char *db, char *user, char *pswd, FILE *plogfp=NULL, char *phandle=NULL, 
                                   bool pdebug_mode=false, int num_tries=0);
        ~DB2Server(); // Destruct
        bool Connected(void) { return connected; }
        void SetDebugMode(bool mode);
        bool CheckRetCode(SQLRETURN retcode, SQLSMALLINT htype, SQLHANDLE hndl);
        void Diagnostics(SQLSMALLINT htype, SQLHANDLE hndl);
        char * dbg_prefix(void);
        char * err_prefix(void);
        char *data_type(SQLSMALLINT dt);
        SQLSMALLINT SqlType2CType(SQLSMALLINT st);
        bool GetConnProp(char *prop_file, char *pdb=NULL, char *puser=NULL);
        bool Connect(char *db=NULL, char *user=NULL, char *pswd=NULL, 
                       char *phandle=NULL, bool pdebug_mode=false, int num_tries=0);
        char * GetServer(char *str) { strcpy(str, dbAlias); return str; }
        char * GetLogin(char *str) { strcpy(str, user); return str; }
        char * GetPasswd(char *str) { strcpy(str, pswd); return str; }
        SQLINTEGER RowCount(void) { return num_rows; }
        bool Disconnect(void);
        bool Bind(int col_num, void *bind_var, SQLINTEGER bind_len=0);
        SQLRETURN Fetch(void);
        SQLRETURN CloseCursor(void);
        bool CommitTran(void);
        bool RollbackTran(void);
        bool SetSql(const char *format, ...);
        bool ReadSql(char *file_name);
        bool ClearSql(void);
        void ShowSql(void) { printf("\n%s\n", sql); return; }
        bool Exec(bool collect_stat=true);
        bool Prepare(void);
        bool BindParameter( SQLUSMALLINT par_num, SQLSMALLINT  io_type,
                            SQLSMALLINT val_type, SQLSMALLINT  par_type,
                            SQLUINTEGER col_siz, SQLSMALLINT  dec_dig,
                            SQLPOINTER par_val_ptr, SQLINTEGER   buf_len,
                            SQLLEN       *s_or_i // SQLINTEGER  *s_or_i; 2017-07-21 Compiling with odbc library and include from folder /usr 
                          );
        bool BindInpCharParam( SQLUSMALLINT par_num,
                               SQLPOINTER   par_val_ptr,
                               SQLUINTEGER  col_siz,
                               SQLINTEGER   buf_len
                             );
        SQLSMALLINT GetNumCols(void);
        bool DisplayColProps(void);
        bool SetLogFile(FILE *fp=stderr, char *file_name="UNKNOWN");
        void Rtrim(SQLCHAR *str);
        char * SqlChar2Char(SQLCHAR *str);
};

#endif // _OOS_HEADER_

