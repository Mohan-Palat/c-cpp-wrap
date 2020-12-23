// U\
// U\
// ******************************************************************************
// *                                                                            *
// * APP History Init Row Generator for Client Recognition         Mohan Palat *
// * AppHistInitForClient.cpp                                         08/01/18 *
// *                                                                            *
// * Parameters                                                                 *
// * ----------                                                                 *
// * 1. DB Connections Properties File                                          *
// * 2. Process Log File                                                        *
// *                                                                            *
// * Modification History                                                       *
// * --------------------                                                       *
// * By          Date     Description                                           *
// * ----------- -------- ----------------------------------------------------- *
// * Mohan       08/03/18 Initial Release 1.0U                                  *
// * Mohan       08/07/18 Release 1.1U                                          *
// *                      Added the DELETE Clause for Reruns                    *
// * Mohan       08/09/18 Release 1.2U                                          *
// *                      Include WHERE for UPDT_DTM only if field present      *
// * Mohan       08/28/18 Release 1.3U                                          *
// *                      Include selective columninsert for tables associated  *
// *                      with temporal history table                           *
// * Mohan       08/28/18 Release 1.4U                                          *
// *                      Include Exception Table List Input File               *
// * Mohan       08/29/18 Release 1.5U                                          *
// *                      Include ability to create individual DDL scripts      *
// * Mohan       09/24/18 Release 1.6U                                          *
// *                      Parameter File AppHistInitForClient.Schemas.txt      *
// * Mohan       09/25/18 Release 1.7U                                          *
// *                      RANK() OVER() to optimize the SQL Generated           *
// * Mohan       09/25/18 Release 1.8U                                          *
// *                      RANK() OVER() ALIAS in Inner Query (Bug Fix)          *
// * Mohan       10/29/18 Release 1.9U                                          *
// *                      Upper() for Primary Key Char fields                   *
// *                      ORDER BY EFFECTIVE_DATE - Removed the DESC Clause     *
// * Mohan       11/04/18 Release 2.0U                                          *
// *                      For Schema MYDBPUB, if Key has CHAR/VARCHAR           *
// *                      Add additional join from the same table               *
// *                      This will be a filter for DATA_SOURCE_ID              *
// * Mohan       11/17/18 Release 2.1U                                          *
// *                      1.                                                    *
// *                      Changed ORDER BY EFFECTIVE_DATE, UPDATE_DATETIME      *
// *                      to ORDER BY EFFECTIVE_DATE, UPDATE_DATETIME DESC      *
// *                      2.                                                    *
// *                      Removed 2 files because Farooq keeps 2 tailored       *
// *                      files which were also getting wiped out               *
// *                      INIT_APP_FOR_CLIENT_HIST.MYDBAPP.                   *                         
// *                        T_MASTER_INVESTMENT_CADIS_ID_XREF_HISTORY.sql       *
// *                      INIT_APP_FOR_CLIENT_HIST.MYDBAPP.                   *
// *                        T_MASTER_INVESTMENT_APP_PRICE_ID_XREF_HISTORY.sql  *
// ******************************************************************************
//

#define APP_HIST_INIT_FOR_CLIENT_VER  "2.1U"

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "DB2Server.h"
#include "Log.h"

enum err_cd_list {
    WORKED_FINE, // 0
    LOG_CREATE_ERR, // 1
    UNKNOWN_PAR_SWITCH, // 2
    PAR_VAL_ERR, // 3
    SQLTPL_PAR_MISSING, // 4
    PARVAL_PAR_MISSING, // 5
    DB2PROP_PAR_MISSING, // 6
    CREATE_SERVEROBJ_FAILED, // 7
    SERVER_CONNPROPS_MISSING, // 8
    DB_CONNECT_FAILED, // 9
    FOPEN_EXCL_FAILED, // 10
    FOPEN_DDL_FAILED, // 11
    FOPEN_SQLTPL_FAILED, // 12
    FROM_CLAUSE_EMBEDDED, // 13
    INSUFFICIENT_COMMANDLINE_PARS, // 14
    COUNT_EXEC_FAILED, // 15
    COUNT_BIND_FAILED, // 16
    COUNT_FETCH_FAILED, // 17
    COUNT_CLOSECURSOR_FAILED, // 18
    DELETE_EXEC_FAILED, // 19
    DB2_SQL_PREPARE_FAILED, // 20
    DB2_SQL_EXEC_FAILED, // 21
    DB2_SQL_BIND_FAILED, // 22
    DB2_SQL_FETCH_FAILED, // 23
    DB2_SQL_CLOSE_CURSOR_FAILED, // 24
    MALLOC_FAILED, // 25
    STRDUP_FAILED, // 26
    FOPEN_TLIST_FAILED, //27
    FOPEN_SCHEMA_FAILED, // 28
    END_OF_LIST
};

class AppHistInitForClient {
private:
    Log *l;
    DB2Server *d;
    bool debug_mode;
    char db_prop_file[BUFSIZ];
    char log_file[BUFSIZ];
    char work_folder[BUFSIZ];
    char excl_tbl_list_file[BUFSIZ];
    char schemas_file[BUFSIZ];
    char schemas_where[BUFSIZ];
    char **xtablist;
    int num_xtables;
    SQLRETURN RetCode;
    SQLCHAR cal_group_id[BUFSIZ];
    SQLCHAR candid_table[BUFSIZ];
    char **tablist;
    int num_tables, num_cols;
    char tabschema[BUFSIZ];
    char tabname[BUFSIZ];
    SQLINTEGER colno;
    SQLCHAR colname[BUFSIZ];
    SQLINTEGER keyseq;
    SQLINTEGER isstring;
    char min_where_clause[BUFSIZ * 8], max_where_clause[BUFSIZ * 8];
    bool rank_clause_first, upddtm_where_needed, data_src_id_jn_reqd;
    char data_src_id_jn_keyfld[BUFSIZ];
    char key_list[BUFSIZ * 8];
    FILE *dfp; // DDL File Pointer
public:
    AppHistInitForClient(int argc, char **argv);
    ~AppHistInitForClient();
    bool Replace(char *str, char *from_pat, char *to_pat);
    void ErrExit(int err_cd, const char* format, ...);
    bool ParseArgs(int argc, char **argv);
    void ConnectToDatabase(void);
    void Go(void);
    void GetSchemas(void);
    void GetExclTables(void);
    void GetTables(void);
    void GenerateSqlHead(int tno);
    void GenerateSqlBody(void);
    void GenerateSqlTail(void);
    void ProcessColumn(void);
    void GenerateSqlBodyPass1(void);
    void GenerateSqlBodyPass2(void);
    void GenerateSqlBodyPass3(void);
    void ClearOldWorkSqlFiles(void);
    void CreateTabListFile(void);
};

AppHistInitForClient::AppHistInitForClient(int argc, char **argv)
{
    d = NULL;
    l = NULL;
    // sample = (SQLINTEGER)0L;
    ParseArgs(argc, argv);
    ConnectToDatabase();
    if (*excl_tbl_list_file)
        GetExclTables();
    GetSchemas();
    GetTables();
    // ClearOldWorkSqlFiles(); Commented in Version 2.1
    CreateTabListFile();
}

AppHistInitForClient::~AppHistInitForClient()
{
    if (d)
        delete d;
    if (l)
        delete l;
    for (int i = 0; i < num_tables; i++)
        if (tablist[i])
            free(tablist[i]);
    if (*excl_tbl_list_file)
        for (int i = 0; i < num_xtables; i++)
            if (xtablist[i])
                free(xtablist[i]);
}

bool AppHistInitForClient::Replace(char *str, char *from_pat, char *to_pat)
{
    char *ptr = str;
    char buf[BUFSIZ];

    if (!strstr(ptr, from_pat))
        return true; // Pattern not found to replace

    while (true) {
        if (strstr(ptr, from_pat)) {
            strcpy(buf, ptr);
            *(strstr(buf, from_pat)) = '\0';
            strcat(buf, to_pat);
            strcat(buf, strstr(ptr, from_pat) + strlen(from_pat));
            strcpy(ptr, buf);
        }
        else
            break;
        ptr = strstr(ptr, to_pat) + strlen(to_pat);
    }

    return false; // Replaced
}

void AppHistInitForClient::ErrExit(int err_cd, const char* format, ...)
{
    // Usage:- ErrExit(25, "%d is successful with value %d", 50, 75);
    char buf[BUFSIZ], ebuf[BUFSIZ];
    va_list args;
    va_start(args, format);
    vsprintf(buf, format, args);
    va_end(args);
    sprintf(ebuf, "ErrorCode: %03d. %s", err_cd, buf);
    if (l)
        l->WriteErr(ebuf);
    printf("\n[%s]\n", ebuf);
    exit(err_cd);
}

bool AppHistInitForClient::ParseArgs(int argc, char **argv)
{
    if (argc != 5 && argc != 6) {
        printf("\nYou are executing the C++ binary %s Version %s\n", argv[0], APP_HIST_INIT_FOR_CLIENT_VER);
        ErrExit(INSUFFICIENT_COMMANDLINE_PARS, "Usage: %s <DB Connection Prop File> <Process Log> <Work Folder Name> <Schems List File> [<Exception Table List File>]", argv[0]);
    }
    strcpy(db_prop_file, argv[1]);
    strcpy(log_file, argv[2]);
    strcpy(work_folder, argv[3]);
    strcpy(schemas_file, argv[4]);

    if (!(l = new Log(log_file)))
        ErrExit(LOG_CREATE_ERR, "Unable to create Log Object");
    l->WriteFyi("Binary: %s, Version: %s", argv[0], APP_HIST_INIT_FOR_CLIENT_VER);
    l->WriteFyi("DB2 Connection Property File: %s", db_prop_file);
    l->WriteFyi("AppHistInitForClient Log: %s", log_file);
    l->WriteFyi("Work File Folder: %s", work_folder);
    l->WriteFyi("Schemas List File: %s", schemas_file);

    if (argc == 6) {
        sprintf(excl_tbl_list_file, argv[5]);
        l->WriteFyi("File with list of tables to be excluded: %s", excl_tbl_list_file);
    }
    else {
        *excl_tbl_list_file = '\0';
        l->WriteFyi("File with list of tables to be excluded: None");
    }

    l->Flush();

    return false;
}

void AppHistInitForClient::ConnectToDatabase(void)
{
    // Connection for DELETE SQL

    d = new DB2Server(l->GetFP());
    if (!d)
        ErrExit(CREATE_SERVEROBJ_FAILED, "Server object for DELETE could not be created");
    if (d->GetConnProp(db_prop_file))
        ErrExit(SERVER_CONNPROPS_MISSING, "No Connection Properties inside the -c file %s", db_prop_file);
    if (d->Connect())
        ErrExit(DB_CONNECT_FAILED, "Connect() wth Properties inside the -c file %s Failed", db_prop_file);

    return;
}

void AppHistInitForClient::GetSchemas(void)
{
    char buf[BUFSIZ], bufb[BUFSIZ];
    FILE *sfp = NULL;
    bool first = true;

    if (!(sfp = fopen(schemas_file, "rt")))
        ErrExit(FOPEN_SCHEMA_FAILED, "Unable to Open %s", schemas_file);

    *schemas_where = '\0';
    while (fgets(buf, BUFSIZ, sfp)) {
        if (strchr(buf, '\n'))
            *(strchr(buf, '\n')) = '\0';
        if (first)
            sprintf(bufb, "'%s'", buf);
        else
            sprintf(bufb, ", '%s'", buf);
        strcat(schemas_where, bufb);
        first = false;
    }

    l->WriteFyi("Schema List: %s", schemas_where);

    fclose(sfp);
    return;
}

void AppHistInitForClient::CreateTabListFile(void)
{
    FILE *fp = NULL;
    char fn[BUFSIZ];

    sprintf(fn, "%sAppHistInitForClient.TableList.txt", work_folder);
    if (!(fp = fopen(fn, "wt")))
        ErrExit(FOPEN_TLIST_FAILED, "Unable to Open %s", fn);

    for (int i = 0; i < num_tables; i++)
        fprintf(fp, "%s\n", tablist[i]);

    fclose(fp);
    l->WriteFyi("List of tables which were processed: %s", fn);

    return;
}


void AppHistInitForClient::GetExclTables(void)
{
    char buf[BUFSIZ];
    FILE *xfp=NULL;

    // ---------------------------------------------------
    // Pass 1, Count and Allocate Exclude Table List Array 
    // ---------------------------------------------------
    if (!(xfp=fopen(excl_tbl_list_file, "rt")))
        ErrExit(FOPEN_EXCL_FAILED, "Unable to Open %s", excl_tbl_list_file);
        
    num_xtables = 0;
    while (fgets(buf, BUFSIZ, xfp))
        num_xtables++;
    l->WriteFyi("Number of Exclude Tables %03d", num_xtables);

    // Create Array and Initialize Content
    // -----------------------------------
    xtablist = ((char **)malloc(sizeof(char *) * (num_xtables)));
    if (!(xtablist))
        ErrExit(MALLOC_FAILED, "Unable to malloc(Exclude Table List)");
    for (int i = 0; i < num_xtables; i++)
        xtablist[i] = NULL;

    // ----------------------------------------
    // Pass 2, Fill Up Exclude Table List Array 
    // ----------------------------------------
    rewind(xfp);
    int curr_xtable = 0;
    while (fgets(buf, BUFSIZ, xfp)) {
        if (strchr(buf, '\n')) 
            *(strchr(buf, '\n')) = '\0';
        xtablist[curr_xtable] = (char *)strdup((char *)buf);
        if (!(xtablist[curr_xtable]))
            ErrExit(STRDUP_FAILED, "Unable to strdup(Table) into the Exclude Table List Array");
        curr_xtable++;
    }

    // Log the filled up Table List Array
    for (int i = 0; i < num_xtables; i++)
        l->WriteFyi("Exclude Table %03d: %s", i+1, xtablist[i]);

    fclose(xfp);
    return;
}

void AppHistInitForClient::GetTables(void)
{
    char candid_table_s[BUFSIZ];
    bool exclude_this_tbl;

    // -------------------------------------------
    // Pass 1, Count and Allocate Table List Array 
    // -------------------------------------------

    d->ClearSql();
    d->SetSql(" SELECT DISTINCT RTRIM(T.TABSCHEMA)||'.'||RTRIM(T.TABNAME) CANDIDATE_TABLE  ");
    d->SetSql("   FROM SYSCAT.TABLES T, SYSCAT.COLUMNS C  ");
    d->SetSql("  WHERE T.TABSCHEMA = C.TABSCHEMA   ");
    d->SetSql("    AND T.TABNAME = C.TABNAME  ");
    d->SetSql("    AND T.TABSCHEMA IN (%s) ", schemas_where);
    d->SetSql("    AND T.TYPE = 'T'  ");
    d->SetSql("    AND RTRIM(C.COLNAME) = 'EFFECTIVE_DATE'  ");
    d->SetSql(" EXCEPT ");
    d->SetSql(" SELECT DISTINCT RTRIM(T.TABSCHEMA)||'.'||RTRIM(T.TABNAME)  ");
    d->SetSql("   FROM ( ");
    d->SetSql("             SELECT HISTORYTABSCHEMA TABSCHEMA, HISTORYTABNAME TABNAME  ");
    d->SetSql("               FROM SYSCAT.PERMYDB  ");
    d->SetSql("              WHERE TABSCHEMA IN (%s)  ", schemas_where);
    d->SetSql("                AND HISTORYTABNAME IS NOT NULL  ");
    d->SetSql("        ) T ");
    d->SetSql("   WITH UR ");
    if (d->Prepare())
        ErrExit(DB2_SQL_PREPARE_FAILED, "Unable to Prepare(SQL)");
    if (d->Exec())
        ErrExit(DB2_SQL_EXEC_FAILED, "Unable to Exec(SQL)");
    if (d->Bind(1, candid_table, sizeof(candid_table)))
        ErrExit(DB2_SQL_BIND_FAILED, "Unable to Bind(SQL)");
    num_tables = 0;
    memset(candid_table, '\0', BUFSIZ);

    while ((RetCode = d->Fetch()) != SQL_NO_DATA) {
        if (RetCode == SQL_ERROR)
            ErrExit(DB2_SQL_FETCH_FAILED, "Unable to Fetch(SQL)");
        sprintf(candid_table_s, "%s", candid_table);
        exclude_this_tbl = false;
        for (int i = 0; i < num_xtables; i++) {
            // printf("\n[%s]--[%s]", xtablist[i], candid_table_s);
            if (!strcmp(xtablist[i], candid_table_s)) {
                exclude_this_tbl = true;
                break;
            }
        }
        if (exclude_this_tbl)
            continue;
        num_tables++;
        // l->WriteFyi("Candidate Table %03d: %s", num_tables, candid_table);
        memset(candid_table, '\0', BUFSIZ);
    }
    if (d->CloseCursor())
        ErrExit(DB2_SQL_CLOSE_CURSOR_FAILED, "Unable to CloseCursor(SQL)");

    // Create Array and Initialize Content
    // -----------------------------------
    l->WriteFyi("Number of Candidate Tables %03d", num_tables);
    tablist = ((char **)malloc(sizeof(char *) * (num_tables)));
    if (!(tablist))
        ErrExit(MALLOC_FAILED, "Unable to malloc(Table List)");
    for (int i = 0; i < num_tables; i++)
        tablist[i] = NULL;

    // --------------------------------
    // Pass 2, Fill Up Table List Array 
    // --------------------------------

    if (d->Exec())
        ErrExit(DB2_SQL_EXEC_FAILED, "Unable to Exec(SQL)");
    if (d->Bind(1, candid_table, sizeof(candid_table)))
        ErrExit(DB2_SQL_BIND_FAILED, "Unable to Bind(SQL)");
    int curr_table = 0;
    memset(candid_table, '\0', BUFSIZ);
    while ((RetCode = d->Fetch()) != SQL_NO_DATA) {
        if (RetCode == SQL_ERROR)
            ErrExit(DB2_SQL_FETCH_FAILED, "Unable to Fetch(SQL)");
        sprintf(candid_table_s, "%s", candid_table);
        exclude_this_tbl = false;
        for (int i = 0; i < num_xtables; i++) {
            // printf("\n[%s]--[%s]", xtablist[i], candid_table_s);
            if (!strcmp(xtablist[i], candid_table_s)) {
                exclude_this_tbl = true;
                break;
            }
        }
        if (exclude_this_tbl)
            continue;
        tablist[curr_table] = (char *)strdup((char *)candid_table);
        if (!(tablist[curr_table]))
            ErrExit(STRDUP_FAILED, "Unable to strdup(Table) into the Table List Array");
        curr_table++;
        memset(candid_table, '\0', BUFSIZ);
    }
    if (d->CloseCursor())
        ErrExit(DB2_SQL_CLOSE_CURSOR_FAILED, "Unable to CloseCursor(SQL)");

    // Log the filled up Table List Array
    for (int i = 0; i < num_tables; i++)
        l->WriteFyi("Candidate Table %03d: %s", i+1, tablist[i]);

    return;
}

void AppHistInitForClient::GenerateSqlBodyPass1(void)
{
    if (d->Exec())
        ErrExit(DB2_SQL_EXEC_FAILED, "Unable to Exec(COL SQL)");

    while ((RetCode = d->Fetch()) != SQL_NO_DATA) {
        if (RetCode == SQL_ERROR)
            ErrExit(DB2_SQL_FETCH_FAILED, "Unable to Fetch(COL)");
        // l->WriteFyi("\tColumn %03d: %s, Column Seq %d, Key Seq %d", num_cols, colname, colno, keyseq);
        if (!colno) { // First row with no comma
            printf("\n(\n       %s", colname);
            fprintf(dfp, "\n(\n       %s", colname);
        }
        else {
            printf(",\n       %s", colname);
            fprintf(dfp, ",\n       %s", colname);
        }
        memset(colname, '\0', BUFSIZ);
    }

    if (d->CloseCursor())
        ErrExit(DB2_SQL_CLOSE_CURSOR_FAILED, "Unable to CloseCursor(COL)");

    printf("\n)");
    fprintf(dfp, "\n)");

    return;
}

void AppHistInitForClient::GenerateSqlBodyPass2(void)
{
    if (d->Exec())
        ErrExit(DB2_SQL_EXEC_FAILED, "Unable to Exec(COL SQL)");

    while ((RetCode = d->Fetch()) != SQL_NO_DATA) {
        if (RetCode == SQL_ERROR)
            ErrExit(DB2_SQL_FETCH_FAILED, "Unable to Fetch(COL)");
        // l->WriteFyi("\tColumn %03d: %s, Column Seq %d, Key Seq %d", num_cols, colname, colno, keyseq);
        if (!colno) { // First row with no comma
            printf("\nSELECT %s", colname);
            fprintf(dfp, "\nSELECT %s", colname);
        }
        else {
            printf(",\n       %s", colname);
            fprintf(dfp, ",\n       %s", colname);
        }
        memset(colname, '\0', BUFSIZ);
    }

    if (d->CloseCursor())
        ErrExit(DB2_SQL_CLOSE_CURSOR_FAILED, "Unable to CloseCursor(COL)");

    printf("\n  FROM (");
    fprintf(dfp, "\n  FROM (");

    return;
}

void AppHistInitForClient::GenerateSqlBodyPass3(void)
{
    if (d->Exec())
        ErrExit(DB2_SQL_EXEC_FAILED, "Unable to Exec(COL SQL)");

    num_cols = 0;
    char kdef[BUFSIZ];
    while ((RetCode = d->Fetch()) != SQL_NO_DATA) {
        num_cols++;
        if (RetCode == SQL_ERROR)
            ErrExit(DB2_SQL_FETCH_FAILED, "Unable to Fetch(COL)");
        // l->WriteFyi("\tColumn %03d: %s, Column Seq %d, Key Seq %d", num_cols, colname, colno, keyseq);
        if (keyseq != -1)
            sprintf(kdef, "(Key.%d)", keyseq);
        l->WriteFyi("\tColumn %03d: %s %s", num_cols, colname, keyseq == -1 ? " " : kdef);
        ProcessColumn();
        memset(colname, '\0', BUFSIZ);
    }
    if (d->CloseCursor())
        ErrExit(DB2_SQL_CLOSE_CURSOR_FAILED, "Unable to CloseCursor(COL)");
    return;
}

void AppHistInitForClient::GenerateSqlBody(void)
{
    d->ClearSql();
    d->SetSql(" SELECT A.COLNO, A.COLNAME, COALESCE(A.KEYSEQ, -1) KEYSEQ, ");
    d->SetSql("        CASE ");
    d->SetSql("        WHEN A.TYPENAME IN('CHARACTER', 'VARCHAR') ");
    d->SetSql("        THEN INTEGER(1) ");
    d->SetSql("        ELSE INTEGER(0) ");
    d->SetSql("         END ISSTRING ");
    d->SetSql("   FROM SYSCAT.COLUMNS A ");
    d->SetSql("  WHERE TABSCHEMA = '%s' ", tabschema);
    d->SetSql("    AND TABNAME = '%s' ", tabname);
    d->SetSql("    AND GENERATED = ' ' "); // A - Automatic
    d->SetSql("  ORDER BY A.COLNO ");
    d->SetSql("   WITH UR ");

    if (d->Prepare())
        ErrExit(DB2_SQL_PREPARE_FAILED, "Unable to Prepare(COL SQL)");

    if (d->Bind(1, &colno))
        ErrExit(DB2_SQL_BIND_FAILED, "Unable to Bind(colno)");
    if (d->Bind(2, colname, sizeof(colname)))
        ErrExit(DB2_SQL_BIND_FAILED, "Unable to Bind(colname)");
    if (d->Bind(3, &keyseq))
        ErrExit(DB2_SQL_BIND_FAILED, "Unable to Bind(keyseq)");
    if (d->Bind(4, &isstring))
        ErrExit(DB2_SQL_BIND_FAILED, "Unable to Bind(isstring)");

    GenerateSqlBodyPass1();
    GenerateSqlBodyPass2();
    GenerateSqlBodyPass3();

    return;
}

void AppHistInitForClient::GenerateSqlHead(int tno)
{
    printf("\n\n-- Initializing Table %03d of %03d %s.%s\n", tno, num_tables, tabschema, tabname);
    printf("\nDELETE FROM %s.%s \n WHERE EFFECTIVE_DATE = '1900-01-01' ", tabschema, tabname);
    printf("\n   AND LAST_ACTIVITY_OPERATOR_ID = 'DEFAULT_FOR_CLIENT_HISTGEN'\n  WITH UR;\n");
    printf("\nINSERT INTO %s.%s", tabschema, tabname);

    fprintf(dfp, "\n\n-- Initializing Table %03d of %03d %s.%s\n", tno, num_tables, tabschema, tabname);
    fprintf(dfp, "\nDELETE FROM %s.%s \n WHERE EFFECTIVE_DATE = '1900-01-01' ", tabschema, tabname);
    fprintf(dfp, "\n   AND LAST_ACTIVITY_OPERATOR_ID = 'DEFAULT_FOR_CLIENT_HISTGEN'\n  WITH UR;\n");
    fprintf(dfp, "\nINSERT INTO %s.%s", tabschema, tabname);

    return;
}

void AppHistInitForClient::GenerateSqlTail(void)
{
    // Sample:
    // RANK() OVER( partition by APP_PRICE_ID ORDER BY EFFECTIVE_DATE,UPDATE_DATETIME DESC) AS RNK
    //   FROM MYDBAPP.T_MASTER_INVESTMENT_PRICE_STRUCTURE_HISTORY A)
    //  WHERE RNK = 1
    //   WITH UR;

    if (upddtm_where_needed)
        printf(",\n                   RANK() OVER( PARTITION by %s ORDER BY EFFECTIVE_DATE, UPDATE_DATETIME DESC) AS RNK ", key_list);
    else
        printf(",\n                   RANK() OVER( PARTITION by %s ORDER BY EFFECTIVE_DATE ) AS RNK ", key_list);
    printf("\n              FROM %s.%s A ", tabschema, tabname);
    printf("\n       )");
    if (data_src_id_jn_reqd) {
        printf(" A,");
        printf("\n       (");
        printf("\n            SELECT DISTINCT %s AS %s_B,", data_src_id_jn_keyfld, data_src_id_jn_keyfld);
        printf("\n                   DATA_SOURCE_ID AS DATA_SOURCE_ID_B,");
        printf("\n                   RANK() OVER( PARTITION by %s ORDER BY EFFECTIVE_DATE  ) AS D_RNK", data_src_id_jn_keyfld);
        printf("\n              FROM %s.%s A ", tabschema, tabname);
        printf("\n       ) B ");
        printf("\n WHERE RNK = 1 ");
        printf("\n   AND D_RNK = 1 ");
        printf("\n   AND A.%s = B.%s_B ", data_src_id_jn_keyfld, data_src_id_jn_keyfld);
        printf("\n   AND A.DATA_SOURCE_ID = B.DATA_SOURCE_ID_B ");
        printf("\n  WITH UR ");
        printf("\n;\n\n");
    } else {
        printf("\n WHERE RNK = 1 ");
        printf("\n  WITH UR ");
        printf("\n;\n\n");
    }

    if (upddtm_where_needed)
        fprintf(dfp, ",\n                   RANK() OVER( PARTITION by %s ORDER BY EFFECTIVE_DATE, UPDATE_DATETIME DESC) AS RNK ", key_list);
    else
        fprintf(dfp, ",\n                   RANK() OVER( PARTITION by %s ORDER BY EFFECTIVE_DATE ) AS RNK ", key_list);
    fprintf(dfp, "\n              FROM %s.%s A ", tabschema, tabname);
    fprintf(dfp, "\n       )");
    if (data_src_id_jn_reqd) {
        fprintf(dfp, " A,");
        fprintf(dfp, "\n       (");
        fprintf(dfp, "\n            SELECT DISTINCT %s AS %s_B,", data_src_id_jn_keyfld, data_src_id_jn_keyfld);
        fprintf(dfp, "\n                   DATA_SOURCE_ID AS DATA_SOURCE_ID_B,");
        fprintf(dfp, "\n                   RANK() OVER( PARTITION by %s ORDER BY EFFECTIVE_DATE  ) AS D_RNK", data_src_id_jn_keyfld);
        fprintf(dfp, "\n              FROM %s.%s A ", tabschema, tabname);
        fprintf(dfp, "\n       ) B ");
        fprintf(dfp, "\n WHERE RNK = 1 ");
        fprintf(dfp, "\n   AND D_RNK = 1 ");
        fprintf(dfp, "\n   AND A.%s = B.%s_B ", data_src_id_jn_keyfld, data_src_id_jn_keyfld);
        fprintf(dfp, "\n   AND A.DATA_SOURCE_ID = B.DATA_SOURCE_ID_B ");
        fprintf(dfp, "\n  WITH UR ");
        fprintf(dfp, "\n;\n\n");
    }
    else {
        fprintf(dfp, "\n WHERE RNK = 1 ");
        fprintf(dfp, "\n  WITH UR ");
        fprintf(dfp, "\n;\n\n");
    }

    return;
}

void AppHistInitForClient::ProcessColumn(void)
{
    char buf[BUFSIZ * 8], buf2[BUFSIZ * 8], colname_s[BUFSIZ * 8];
    sprintf(colname_s, "%s", colname);

    if (!strcmp(colname_s, "SYS_START") || !strcmp(colname_s, "SYS_END") || !strcmp(colname_s, "TRANS_ID"))
        return;

    if (!strcmp(colname_s, "EFFECTIVE_DATE"))
        sprintf(buf, "'1900-01-01' EFFECTIVE_DATE");
    else if (!strcmp(colname_s, "UPDATE_DATETIME"))
        sprintf(buf, "CURRENT TIMESTAMP UPDATE_DATETIME");
    else if (!strcmp(colname_s, "CADIS_SYSTEM_INSERTED"))
        sprintf(buf, "CURRENT TIMESTAMP CADIS_SYSTEM_INSERTED");
    else if (!strcmp(colname_s, "CADIS_SYSTEM_UPDATED"))
        sprintf(buf, "CURRENT TIMESTAMP CADIS_SYSTEM_UPDATED");
    else if (!strcmp(colname_s, "CADIS_SYSTEM_CHANGEDBY")) // Length = CHAR(50)
        sprintf(buf, "'DEFAULT_FOR_CLIENT_HISTGEN' CADIS_SYSTEM_CHANGEDBY");
    else if (!strcmp(colname_s, "LAST_ACTIVITY_OPERATOR_ID")) // Length = CHAR(32)
        sprintf(buf, "'DEFAULT_FOR_CLIENT_HISTGEN' LAST_ACTIVITY_OPERATOR_ID");
    else if (!strcmp(colname_s, "LAST_ACTIVITY_DATETIME"))
        sprintf(buf, "CURRENT TIMESTAMP LAST_ACTIVITY_DATETIME");
    else
        sprintf(buf, colname_s);

    if (!strcmp(colname_s, "UPDATE_DATETIME"))
        upddtm_where_needed = true;

    if (!colno) { // First row with SELECT
        printf("\n            SELECT %s", buf);
        fprintf(dfp, "\n            SELECT %s", buf);
    }
    else {
        printf(",\n                   %s", buf);
        fprintf(dfp, ",\n                   %s", buf);
    }

    if (keyseq != -1) {
        // if (keyseq == 1) { Not using keseq as driver to facilitate selective column discard
        //                    Instead use boolean flag rank_clause_first
        if (isstring && (!strcmp(tabschema, "MYDBPUB")))
            data_src_id_jn_reqd = true;
        if (!strcmp(colname_s, "CADIS_ID") || !strcmp(colname_s, "UNIVERSAL_BENCHMARK_ID") || !strcmp(colname_s, "APP_PRICE_ID"))
            strcpy(data_src_id_jn_keyfld, colname_s);
        if (rank_clause_first) {
            if (strcmp(colname_s, "UPDATE_DATETIME") && strcmp(colname_s, "EFFECTIVE_DATE")) {
                if (isstring)
                    sprintf(key_list, "UPPER(%s)", colname);
                else
                    sprintf(key_list, "%s", colname);
                rank_clause_first = false;
            }
        }
        else {
            if (strcmp(colname_s, "UPDATE_DATETIME") && strcmp(colname_s, "EFFECTIVE_DATE")) {
                if (isstring)
                    sprintf(buf2, "UPPER(%s)", buf);
                else
                    sprintf(buf2, "%s", buf);
                strcat(key_list, ", ");
                strcat(key_list, buf2);
            }
        }
    }

    return;
}

void AppHistInitForClient::ClearOldWorkSqlFiles(void)
{
    char cmd[BUFSIZ];
    sprintf(cmd, "rm %sINIT_APP_FOR_CLIENT_HIST*.sql", work_folder);
    l->WriteFyi("Clear Old Work Sql Files, Command Return Value: %d", system(cmd));
    return;
}

void AppHistInitForClient::Go(void)
{
    char ddl_file[BUFSIZ];
    for (int i = 0; i < num_tables; i++) {
        sprintf(tabschema, tablist[i]);
        if (strchr(tabschema, '.'))
            *(strchr(tabschema, '.')) = '\0';
        if (strchr(tablist[i], '.'))
            sprintf(tabname, strchr(tablist[i], '.') + 1);
        l->WriteFyi("Processing Table %03d of %03d: [%s].[%s]", i + 1, num_tables, tabschema, tabname);
        *min_where_clause = *max_where_clause = '\0';
        rank_clause_first = true;
        upddtm_where_needed = data_src_id_jn_reqd = false;
        *data_src_id_jn_keyfld = '\0';
        sprintf(ddl_file, "%sINIT_APP_FOR_CLIENT_HIST.%s.%s.sql", work_folder, tabschema, tabname);
        l->WriteFyi("Individual DDL File: %s", ddl_file);
        if (!(dfp = fopen(ddl_file, "wt")))
            ErrExit(FOPEN_DDL_FAILED, "Unable to Open %s", ddl_file);
        GenerateSqlHead(i+1);
        GenerateSqlBody();
        GenerateSqlTail();
        fclose(dfp);
    }

    return;
}


int main(int argc, char **argv)
{
    AppHistInitForClient *ihim = new AppHistInitForClient(argc, argv);

    ihim->Go();

    delete ihim;

    return 0;
}






