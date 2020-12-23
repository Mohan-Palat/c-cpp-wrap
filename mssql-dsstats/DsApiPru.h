// U\
// ***********************************************************************
// *                                                                     *
// * Prudential Wrapper for DataStage C++ API                Mohan Palat *
// * DsApiPru.h                                                 11/26/11 *
// *                                                                     *
// * Methods File: DsApiPru.cpp                                          *
// *                                                                     *
// * Description : Collect DS Job Stats and store it in a DB2 Table      *
// *                                                                     *
// * Dependency  : None. Can be attached as an independent DataStage     *
// *               job to any existing ETL Load Job Sequence             *
// *                                                                     *
// * Main C++    : DSAPI3.cpp compiled to generate DS_LoadStats.exe      *
// *               Binary will be placed in D:\DS_LoadStats\Bin folder   *
// *               on the server.                                        *
// *                                                                     *
// * Collection  : Data is collected at 3 levels                         *
// *               1. Job - The header                                   *
// *               2. Stage - Stage level details like the timestamps    *
// *               3. Link - Stats relating to a link like num rows      *
// *                                                                     *
// *               Given a DS job, the program will collect data as      *
// *               categorized above and will store it in database       *
// *                                                                     *
// * Persistence :                                                       *
// *               1. Job - STGTRSODS.T_DS_LOADSTATS_JOB                 *
// *               2. Stage - STGTRSODS.T_DS_LOADSTATS_STG               *
// *               3. Link - STGTRSODS.T_DS_LOADSTATS_LNK                *
// *               Foreign key with cascade delete ensures that deletes  *
// *               on parent table JOB will delete the children in STG   *
// *               and LNK and no orphans are left behind                *
// *                                                                     *
// * Parameters  : Project Name, Job Name                                *
// *                                                                     *
// * Auxilliary  : Classes Utils, DB2Server, Log                         *
// *               Files Utils.h,Utils.cpp,DB2Server.h,DB2Server.cpp     *
// *               Log.h and Log.cpp                                     *
// *               Note: These were updated in parallel to Vikas's       *
// *               Fatal Error Monitoring System - parse_jlog.cpp and    *
// *               compare_jlog.cpp. If any major functional canges or   *
// *               fixes are made here, it might be a good idea to       *
// *               recompile that under UNIX with FEM Process            *
// *                                                                     *
// * Discipline  : Every method returns a Boolean value true on failure  *
// *               (false otherwise). The calling routine will percolate *
// *               this value all the way back to the main method whih   *
// *               in turn will exit with an error code for the OS.      *
// *               All processes (including the DB2 Server object's      *
// *               methods) takes the Log File Pointer as a parameter    *          
// *               and logs into the same log. The whole program run     *
// *               will only have one log.                               *
// *                                                                     *
// * Caller      : A DS job job executing after a DataStage job,         *
// *               ideally executing in the same sequence.               *
// *                                                                     *
// * Libraries Required in Windows :                                     *
// *       db2cli.lib, asnlib.lib, db2api.lib, db2apie.lib               *
// *                                                                     *
// *                                                                     *
// ***********************************************************************
//    

#ifndef _DAP_HEADER_
#define _DAP_HEADER_
#define _USE_32BIT_TIME_T

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <cstdio>
#include <cstdlib>
#include <cstring>
//extern "C" {
//    #include "dsapi.h"
//}

#include <dsapi.h>
#include "DB2Server.h"
#include "Log.h"
#include "Utils.h"

struct ODBCCounts {
    char rowsIns[BUFSIZ];
    char rowsUpd[BUFSIZ];
    char rowsDel[BUFSIZ];
    char rowsRej[BUFSIZ];
};

class DsApiPru {
    private:
        char **proj;
        char **job;
        char **stage;
        int pcount;
        int jcount;
        int scount;
        ODBCCounts odbcCounts;
        DSPROJECT hProject;
        DSJOB hJob;
        DSPROJECTINFO projInfoRec;
        DSJOBINFO jobInfoRec;
        DSSTAGEINFO stageInfoRec;
        DSLOGEVENT logEventRec;
        int err;
        char errMsg[BUFSIZ*4];
        char *projName;
        char *jobName;
        char *propFile;
        char *logFolder;
        char *logAppendOverwrite;
        char jobNameNoInstance[BUFSIZ];
        char *dsLoadStatsLog;
        char *loadId;
        DB2Server *d1; // Server with Stats Tables
        Log *l1;
        bool LoadProjects();
    public:
        DsApiPru(char *proj_name, char *job_name, char *prop_file, char *log_folder, char *log_append_overwrite);
        ~DsApiPru();
        void ErrExit(int err_cd, const char* format, ...);
        bool Create2DimStrArray(char *str2parse, int &counter, char ***sarray);
        bool Destroy2DimStrArray(int &counter, char ***sarray);
        char * GetLastError(void);
        bool ListProjects();
        int GetProjectCount(void) { return pcount; }
        bool OpenProject(void);
        void CloseProject(void) { DSCloseProject(hProject); }
        bool OpenJob(void);
        void CloseJob(void) { DSCloseJob(hJob); }
        bool GetUniqueLoadId(void);
        bool LoadJobs(void);
        bool ListJobs();
        bool LoadStages(void);
        bool ListStages(void);
        bool ProcessStages(void);
        bool ProcessStageMisc(char *stageName, int seq);
        bool ProcessStageLinkList(char *stageName);
        bool OdbcCountSearchLog(char *ostage);
        bool Db2CountSearchLog(char *ostage);
        bool InsertJobRec(void);
};

#endif // _DAP_HEADER_
