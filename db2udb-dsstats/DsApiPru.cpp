// U\
// **************************************************************************************************************************
// *                                                                                                                        *
// * Prudential Wrapper for DataStage C++ API                                                                   Mohan Palat *
// * DsApiPru.cpp                                                                                                  11/26/11 *
// *                                                                                                                        *
// * Header File : DsApiPru.h                                                                                               *
// *                                                                                                                        *
// * Details     : Refer to DsApiPru.h File                                                                                 *
// *                                                                                                                        *
// * Modification History                                                                                                   *
// *                                                                                                                        *
// * By          Date     Description                                                                                       *
// * ----------- -------- ------------------------------------------------------------------------------------------------- *
// * Mohan Palat 20141209 For ODBC Log Scan included SQL Server and DB2 DSAPI Pru Version - 3.5 to 3.6                      *
// * Mohan Palat 20161207 Created Version 3.6U for Linux Migration                                                          * 
// *                      sprintf(buf, "%s/%s_%s.log", logFolder, projName, jobName); - / for Linux                         *
// * Mohan Palat 20171029 Stage Timestamps - Instead of erroring out collect as 0001-01-01                                  * 
// **************************************************************************************************************************
//

#define _USE_32BIT_TIME_T
#define DSAPIPRU_VER "3.6U"

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "DsApiPru.h"


DsApiPru::DsApiPru(char *proj_name, char *job_name, char *prop_file, char *log_folder, char *log_append_overwrite)
{
    char buf[BUFSIZ];

    pcount = jcount = scount = 0;
    proj = job = stage = NULL; 
    hProject = NULL;
    hJob = NULL;
    projName = jobName = loadId = propFile = logFolder = logAppendOverwrite = NULL;
    odbcCounts.rowsDel[0] = odbcCounts.rowsIns[0] = 
        odbcCounts.rowsRej[0] = odbcCounts.rowsUpd[0] = NULL;
    l1 = NULL;

    if (!(projName=strdup(proj_name))) 
        ErrExit(1, "strdup(Project Name) Failed");

    if (!(jobName=strdup(job_name))) 
        ErrExit(2, "strdup(Job Name) Failed");
    strcpy(jobNameNoInstance, jobName);
    if (strchr(jobNameNoInstance, '.'))
        *(strchr(jobNameNoInstance, '.')) = '\0';

    if (!(propFile=strdup(prop_file))) 
        ErrExit(3, "strdup(DB Property File Name) Failed");
    if (!(logFolder=strdup(log_folder))) 
        ErrExit(4, "strdup(Log Folder) Failed");
    if (!(logAppendOverwrite=strdup(log_append_overwrite))) 
        ErrExit(5, "strdup(Log Append or Overwrite) Failed");

    // Windows: sprintf(buf, "%s\\%s_%s.log", logFolder, projName, jobName);
    sprintf(buf, "%s/%s_%s.log", logFolder, projName, jobName);
    if (!(dsLoadStatsLog=strdup(buf))) 
        ErrExit(6, "strdup(%s) Failed\n", buf);
    
    if (!strcmp(logAppendOverwrite, "LOG_OVERWRITE")) {
        printf("\n[%s] (Mode: Overwrite)\n", dsLoadStatsLog);
        if (!(l1 = new Log(dsLoadStatsLog, "wt"))) 
            ErrExit(7, "Failed creating a Log object using '%s' (Overwrite)", dsLoadStatsLog);
    } else {
        printf("\n[%s] (Mode: Append)\n", dsLoadStatsLog);
        if (!(l1 = new Log(dsLoadStatsLog, "at"))) 
            ErrExit(8, "Failed creating a Log object using '%s' (Append)", dsLoadStatsLog);
    }

    l1->WriteFyi("DS Load Status Log for Version: %s", DSAPIPRU_VER);
    l1->WriteFyi("This File: %s", dsLoadStatsLog);
    l1->WriteFyi("Project Name: %s", projName);
    l1->WriteFyi("Job Name: %s", jobName);
    l1->WriteFyi("DB Control File is: %s", propFile);

    if (!(d1 = new DB2Server(l1->GetFP()))) 
        ErrExit(9, "Server Object Creation Failed");

    if (d1->GetConnProp(propFile)) 
        ErrExit(10, "No connection properties in %s", propFile);

    if (d1->Connect()) 
        ErrExit(11, "db2 Connect using fields in %s failed", propFile);

    if (GetUniqueLoadId()) 
        ErrExit(12, "Unable to get a Unique Timestamp to use as Load Id");

    LoadProjects();
}

DsApiPru::~DsApiPru()
{
    if (job) Destroy2DimStrArray(jcount, &job);
    if (proj) Destroy2DimStrArray(pcount, &proj);
    if (stage) Destroy2DimStrArray(scount, &stage);
    pcount = jcount = scount = 0;
    proj = job = stage = NULL; 
    hProject = NULL;
    hJob = NULL;
    if (d1) delete d1;
    if (l1) delete l1;
    if (projName) free(projName);
    if (jobName) free(jobName);
    if (dsLoadStatsLog) free(dsLoadStatsLog);
    if (loadId) free(loadId);
    if (propFile) free(propFile);
    if (logFolder) free(logFolder);
    if (logAppendOverwrite) free(logAppendOverwrite);
}

void DsApiPru::ErrExit(int err_cd, const char* format, ...)
{
    // Usage:- ErrExit(25, "%d is successful with value %d", 50, 75);
    char buf[BUFSIZ], ebuf[BUFSIZ];    
    va_list args;
    va_start(args, format);
    vsprintf(buf, format, args);
    va_end(args);
    sprintf(ebuf, "ErrorCode: %03d. %s", err_cd, buf);
    if (l1)
        l1->WriteErr(ebuf);
    printf("\n[%s]\n", ebuf);    
    
    exit(err_cd);
}

bool DsApiPru::Create2DimStrArray(char *str2parse, int &counter, char ***sarray)
{
    if (str2parse) {
        if (*str2parse == '\0' && *(str2parse+1) == '\0') {
            l1->WriteErr("\nString to Parse is empty\n");
            return true;
        }
    } else {
        l1->WriteErr("\nString to Parse is NULL\n");
        return true;
    }

    counter=1;
    for (char *p=str2parse; !(*p == '\0' && *(p+1) == '\0'); p++)
        if (*p == '\0')
            counter++;
    //printf("\nString Count = %d\n", counter);
    
    *sarray=((char **) malloc(sizeof(char *) * (counter)));
    if (!(*sarray)) 
        ErrExit(13, "malloc of 2 DIM Array failed");

    char *pt=str2parse;
    int i;
    for (i=0; i<counter; i++) {
        // printf("\n[%03d][%s]", i+1, pt);
        (*sarray)[i] = strdup(pt);
        pt += strlen(pt) + 1;
    }
   
    return false;
}

bool DsApiPru::Destroy2DimStrArray(int &counter, char ***sarray)
{
    for (int i=0; i<counter; i++) {
        free((*sarray)[i]);
        (*sarray)[i] = NULL;
    }
    free(*sarray);
    *sarray = NULL;
    counter = 0;

    return false;
}

bool DsApiPru::LoadProjects(void)
{
    l1->WriteFyi("Loading Project List........");
    char *dsBuf;
    dsBuf = DSGetProjectList();
 
    if (dsBuf) {
        if (*dsBuf == '\0' && *(dsBuf+1) == '\0') {
            l1->WriteErr("No Projects Found");
            return true;
        }
    } else {
        l1->WriteErr("API Call DSGetProjectList Failed");
        return true;
    }

    Create2DimStrArray(dsBuf, pcount, &proj);

    l1->WriteFyi("Project Count = %d", pcount);

    return false;
}

bool DsApiPru::ListProjects(void)
{
    l1->WriteFyi("================================");
    l1->WriteFyi("       List of Projects");
    l1->WriteFyi("================================");
    for(int i=0; i<pcount; i++) 
        l1->WriteFyi("  %03d: %s", i+1, proj[i]);
    l1->Flush();
    
    return false;
}

bool DsApiPru::OpenProject(void)
{
    l1->WriteFyi("Opening Project: %s", projName);

    // Does this project exist ?
    bool proj_exists = false;
    for(int i=0; i<pcount; i++)
        if (!strcmp(proj[i], projName))
            proj_exists = true;
    if (!proj_exists) {
        ListProjects();
        ErrExit(14, "Project [%s] is not in the list of projects on the server", projName);
    }

    hProject = DSOpenProject(projName);
    if (!hProject) {
        l1->WriteErr("API Call DSOpenProject Failed");
        return true;
    }
    
    err = DSGetLastError();
    if (err != DSJE_NOERROR) {
        l1->WriteErr("API Call DSOpenProject Failed: %s", GetLastError());
        return true;
    } else {
        l1->WriteFyi("API Call DSOpenProject(%d) Successful", hProject);
    }

    return false;
}

bool DsApiPru::OpenJob(void)
{
    l1->WriteFyi("Opening Job: %s", jobName);

    // Does this job exits ?
    bool job_exists = false;
    for(int i=0; i<jcount; i++) 
        if (!strcmp(job[i], jobNameNoInstance))
            job_exists = true;
    if (!job_exists) {
        ListJobs();
        ErrExit(15, "Job [%s] is not in the list of jobs on the server", jobName);
    }
 
    if (!hProject) {
        l1->WriteErr("API can not call DSOpenJob without calling DSOpenProject");
        return true;
    }

    hJob = DSOpenJob(hProject, jobName);
    if (!hJob) {
        l1->WriteErr("API Call DSOpenJob() Failed");
        l1->WriteErr("API Error: %s\n", DSGetLastErrorMsg(hProject));
        return true;
    } else {
        l1->WriteFyi("API Call DSOpenJob(%d) Successful", hJob);
    }
    err = DSGetLastError();
    if (err != DSJE_NOERROR) {
        l1->WriteErr("API Call DSOpenJob Failed: %s", GetLastError());
        return true;
    }

    return false;
}

bool DsApiPru::LoadJobs(void)
{
    l1->WriteFyi("Loading Job List........");
    err = DSGetProjectInfo(hProject,DSJ_JOBLIST,&projInfoRec);
    if (err != DSJE_NOERROR) {
        l1->WriteErr("API Call DSGetProjectInfo failed");
        return true;
    }

    char *dsBuf = projInfoRec.info.jobList;

    if (dsBuf) {
        if (*dsBuf == '\0' && *(dsBuf+1) == '\0') {
            l1->WriteErr("No Jobs Found");
            return true;
        }
    } else {
        l1->WriteErr("API Call DSGetProjectInfo(DSJ_JOBLIST) Failed");
        return true;
    }

    Create2DimStrArray(dsBuf, jcount, &job);

    l1->WriteFyi("Job Count = %d", jcount);

    return false;
}

bool DsApiPru::ListJobs(void)
{
    l1->WriteFyi("======================================");
    l1->WriteFyi("           List of Jobs");
    l1->WriteFyi("======================================");
    for(int i=0; i<jcount; i++) 
        l1->WriteFyi("  %04d: %s", i+1, job[i]);
    l1->Flush();

    return false;
}

bool DsApiPru::LoadStages(void)
{
    l1->WriteFyi("Loading Stage List........");

    if (!hProject) {
        l1->WriteErr("Can not call LoadStages before calling OpenProject");
        return true;
    }
    if (!hJob) {
        l1->WriteErr("Can not call LoadStages before calling OpenJob");
        return true;
    }

    err = DSGetJobInfo(hJob,DSJ_STAGELIST,&jobInfoRec);
    if (err != DSJE_NOERROR) {
        l1->WriteErr("API Call DSGetJobInfo(STAGELIST) failed");
        return true;
    }

    err = DSGetLastError();
    if (err != DSJE_NOERROR) {
        l1->WriteErr("API Call DSGetJobInfo(STAGELIST) Failed: %s", GetLastError());
        return true;
    }
    if (!(jobInfoRec.info.stageList)) {
        l1->WriteErr("API DSGetJobInfo(STAGELIST) successful. jobInfoRec.info.stageList is NULL");
        return true;
    }

  
    // printf("\nstageList: [[%s]]", jobInfoRec.info.stageList); printf("\nStep 20");   l1->Flush(); fflush(stdout); 
    // printf("\nstageList: [[%s]]", jobInfoRec.info.stageList);

    char *dsBuf = jobInfoRec.info.stageList;

    if (dsBuf) {
        if (*dsBuf == '\0' && *(dsBuf+1) == '\0') {
            l1->WriteErr("No Stages Found");
            return true;
        }
    } else {
        l1->WriteErr("API Call DSGetJobInfo(STAGELIST) move to dsBuf Failed");
        return true;
    }

    Create2DimStrArray(dsBuf, scount, &stage);

    l1->WriteFyi("Stage Count = %d", scount);

    return false;
}

bool DsApiPru::ListStages(void)
{
    for(int i=0; i<scount; i++) 
        l1->WriteFyi("%04d: %s", i+1, stage[i]);

    return false;
}

bool DsApiPru::ProcessStages(void)
{
    if (InsertJobRec())
        return true;

    for(int i=0; i<scount; i++) {
        l1->WriteFyi("Processing Stage %04d: %s", i+1, stage[i]);
        if (ProcessStageMisc(stage[i], i))
            return true;
        if (ProcessStageLinkList(stage[i]))
            return true;
    }

    return false;
}

    bool DsApiPru::ProcessStageLinkList(char *stageName)
    {
        char *dsBuf;
        int lcount=0;
        char **llist=NULL;
        DSLINKINFO linkInfoRec;

        err = DSGetStageInfo(hJob, stageName, DSJ_LINKLIST, &stageInfoRec);
        if (err != DSJE_NOERROR) {
            l1->WriteErr("API Call DSGetStageInfo(LINKLIST) failed");
            return true;
        }

        err = DSGetLastError();
        if (err != DSJE_NOERROR) {
            l1->WriteErr("API Call DSGetStageInfo(LINKLIST) Failed: %s", GetLastError());
            return true;
        }
        if (!(stageInfoRec.info.linkList)) {
            l1->WriteErr("API DSGetStageInfo(LINKLIST) successful. stageInfoRec.info.linkList is NULL");
            return true;
        }

        dsBuf = stageInfoRec.info.linkList;

        if (dsBuf) {
            if (*dsBuf == '\0' && *(dsBuf+1) == '\0') {
                l1->WriteErr("No Links Found");
                return true;
            }
        } else {
            l1->WriteErr("API Call DSGetStageInfo(LINKLIST) move to dsBuf Failed");
            return true;
        }

        Create2DimStrArray(dsBuf, lcount, &llist);

        // printf("\nLink Count = %d\n", lcount);
    
        char rowCount[BUFSIZ];
        char linkedStage[BUFSIZ];
        for (int i=0; i<lcount; i++) {
            l1->WriteFyi("  Link %04d: %s", i+1, llist[i]);
            err = DSGetLinkInfo(hJob, stageName, llist[i], DSJ_LINKROWCOUNT, &linkInfoRec);
            if (err != DSJE_NOERROR) {
                l1->WriteErr("API Call DSGetLinkInfo(LINKROWCOUNT) failed");
                l1->WriteErr("API Error: %s\n", DSGetLastErrorMsg(hProject));
                return true;
            }
            l1->WriteFyi("    Link Rowcount = %d", linkInfoRec.info.rowCount);
            sprintf(rowCount, "%d", linkInfoRec.info.rowCount); 
            err = DSGetLinkInfo(hJob, stageName, llist[i], DSJ_LINKSTAGE, &linkInfoRec);
            if (err != DSJE_NOERROR) {
                l1->WriteErr("API Call DSGetLinkInfo(LINKSTAGE) failed");
                l1->WriteErr("API Error: %s", DSGetLastErrorMsg(hProject));
                return true;
            }
            l1->WriteFyi("    Linked Stage = '%s'", linkInfoRec.info.linkedStage);
            strcpy(linkedStage, linkInfoRec.info.linkedStage); 
            // Other usable DSLINKINFO Elements
            // SQL State
            // DBMS Code
            // Parallel Jobs - Rowcount per instance, comma separated

            // Insert the stats table 

            /*
                LOAD_ID           BIGINT    NOT NULL,
                LNK_PROJ_NM       CHAR(75)  NOT NULL,
                LNK_JOB_NM        CHAR(75)  NOT NULL,
                LNK_STG_NM        CHAR(75)  NOT NULL,
                LNK_NM            CHAR(75)  NOT NULL,
                LNK_SEQ           INT       NOT NULL,
                LNK_LINKED_STG    CHAR(75),
                LNK_ROWCOUNT      INT,
                LAST_ACTY_OPER_ID CHARACTER(15) NOT NULL DEFAULT ' ',
                LAST_ACTY_DTM     TIMESTAMP     NOT NULL DEFAULT CURRENT TIMESTAMP,
            */

            d1->ClearSql();
            d1->SetSql(" INSERT INTO AUDIT.T_DS_LOADSTATS_LNK ( ");
            d1->SetSql("     LOAD_ID,           ");
            d1->SetSql("     LNK_PROJ_NM,       ");
            d1->SetSql("     LNK_JOB_NM,        ");
            d1->SetSql("     LNK_STG_NM,        ");
            d1->SetSql("     LNK_NM,            ");
            d1->SetSql("     LNK_SEQ,           ");
            d1->SetSql("     LNK_LINKED_STG,    ");
            d1->SetSql("     LNK_ROWCOUNT,      ");
            d1->SetSql("     LAST_ACTY_OPER_ID  ");
            d1->SetSql(" ) VALUES (             ");
            d1->SetSql("     %s,                ", loadId);
            d1->SetSql("     '%s',              ", projName);
            d1->SetSql("     '%s',              ", jobName);
            d1->SetSql("     '%s',              ", stageName);
            d1->SetSql("     '%s',              ", llist[i]);
            d1->SetSql("     %d,                ", i);
            d1->SetSql("     '%s',              ", linkedStage);
            d1->SetSql("     %s,                ", rowCount);
            d1->SetSql("     'DSLSC++%s'        ", DSAPIPRU_VER);
            d1->SetSql(" ) WITH UR              ");

            if (d1->Exec())
                return true;

        }

        if (llist)
            Destroy2DimStrArray(lcount, &llist);

        return false;
    }

    bool DsApiPru::ProcessStageMisc(char *stageName, int seq)
    {
        // Stage Type

        err = DSGetStageInfo(hJob, stageName, DSJ_STAGETYPE, &stageInfoRec);
        if (err != DSJE_NOERROR) {
            l1->WriteErr("API Call DSGetLinkInfo(STAGETYPE) failed");
            l1->WriteErr("API Error: %s\n", DSGetLastErrorMsg(hProject));
            return true;
        }
        char typeName[BUFSIZ];
        sprintf(typeName, "%s", stageInfoRec.info.typeName);
        l1->WriteFyi("  Stage Type:'%s'", typeName);

        // SQL Server or ODBC
        if ( !strcmp(stageInfoRec.info.typeName, "PxOdbc") ||
             !strcmp(stageInfoRec.info.typeName, "Pxsqlsvr") 
           ) {
            if (OdbcCountSearchLog(stageName))
                return true;
            l1->WriteFyi("  Rows Inserted: [%s]", odbcCounts.rowsIns);
            l1->WriteFyi("  Rows Updated : [%s]", odbcCounts.rowsUpd);
            l1->WriteFyi("  Rows Deleted : [%s]", odbcCounts.rowsDel);
            l1->WriteFyi("  Rows Rejected: [%s]", odbcCounts.rowsRej);
        }

        // DB2
        if ( !strcmp(stageInfoRec.info.typeName, "DB2ConnectorPX") 
           ) {
            if (Db2CountSearchLog(stageName))
                return true;
            l1->WriteFyi("  Rows Inserted: [%s]", odbcCounts.rowsIns);
            l1->WriteFyi("  Rows Updated : [%s]", odbcCounts.rowsUpd);
            l1->WriteFyi("  Rows Deleted : [%s]", odbcCounts.rowsDel);
            l1->WriteFyi("  Rows Rejected: [%s]", odbcCounts.rowsRej);
        }

        // Stage TimeStamps

        struct tm *tmval;
        char fts[BUFSIZ], ets[BUFSIZ];

        /* For 3.7U - Instead of erroring out make timestamp 0001-01-01
            err = DSGetStageInfo(hJob, stageName, DSJ_STAGESTARTTIMESTAMP, &stageInfoRec);
            if (err != DSJE_NOERROR) {
                l1->WriteErr("API Call DSGetLinkInfo(STAGESTARTTIMESTAMP) failed");
                l1->WriteErr("API Error: %s", DSGetLastErrorMsg(hProject));
                return true;
            }
            tmval = localtime(&stageInfoRec.info.stageStartTime);
            if (!strftime(fts, BUFSIZ, "%Y-%m-%d %H:%M:%S.000", tmval)) {
                l1->WriteErr("strftime(STAGESTARTTIMESTAMP) Failed");
                return true;
            }
        */

        // 3.7U Default Start Timestamp to 0001-01-01
        err = DSGetStageInfo(hJob, stageName, DSJ_STAGESTARTTIMESTAMP, &stageInfoRec);
        if (err != DSJE_NOERROR) 
            sprintf(fts, "0001-01-01 00:00:00.000");
        else {
            tmval = localtime(&stageInfoRec.info.stageStartTime);
            if (!strftime(fts, BUFSIZ, "%Y-%m-%d %H:%M:%S.000", tmval)) {
                l1->WriteErr("strftime(STAGESTARTTIMESTAMP) Failed");
                return true;
            }
        }

        /* For 3.7U - Instead of erroring out make timestamp 0001-01-01
            err = DSGetStageInfo(hJob, stageName, DSJ_STAGEENDTIMESTAMP, &stageInfoRec);
            if (err != DSJE_NOERROR) {
                l1->WriteErr("API Call DSGetLinkInfo(STAGEENDTIMESTAMP) failed");
                l1->WriteErr("API Error: %s", DSGetLastErrorMsg(hProject));
                return true;
            }
            tmval = localtime(&stageInfoRec.info.stageEndTime);
            if (!strftime(ets, BUFSIZ, "%Y-%m-%d %H:%M:%S.000", tmval)) {
                l1->WriteErr("strftime(STAGEENDTIMESTAMP) Failed");
                return true;
            }
        */

        err = DSGetStageInfo(hJob, stageName, DSJ_STAGEENDTIMESTAMP, &stageInfoRec);
        // 3.7U Default Start Timestamp to 0001-01-01
        if (err != DSJE_NOERROR) 
            sprintf(ets, "0001-01-01 00:00:00.000");
        else {
            tmval = localtime(&stageInfoRec.info.stageEndTime);
            if (!strftime(ets, BUFSIZ, "%Y-%m-%d %H:%M:%S.000", tmval)) {
                l1->WriteErr("strftime(STAGEENDTIMESTAMP) Failed");
                return true;
            }
        }
        
        l1->WriteFyi("  Time:'%s' to '%s'", fts, ets);

        // Stage Status

        err = DSGetStageInfo(hJob, stageName, DSJ_STAGESTATUS, &stageInfoRec);
        if (err != DSJE_NOERROR) {
            l1->WriteErr("API Call DSGetLinkInfo(STAGESTATUS) failed");
            l1->WriteErr("API Error: %s", DSGetLastErrorMsg(hProject));
            return true;
        }

        char stageStatus[BUFSIZ];
        sprintf(stageStatus, "%d", stageInfoRec.info.stageStatus);
        l1->WriteFyi("  Status:[%s]", stageStatus);

        // Other usable DSSTAGEINFO Elements
        // Last Error Message from any link of the stage
        // Primary Link Input Row Number
        // List of Stage Variables
        // Parallel: Comma Separated instance IDs
        // Parallel: Comma Separated instance PIDs
        // Parallel: Comma Separated instance CPU seconds
        // Parallel: Comma Separated Link Types
        // Stage Description
        // Stage Elapsed Time - Seconds

        // Insert the stats table 

        /*
            LOAD_ID           BIGINT    NOT NULL,
            STG_PROJ_NM       CHAR(75)  NOT NULL,
            STG_JOB_NM        CHAR(75)  NOT NULL,
            STG_NM            CHAR(75)  NOT NULL,
            STG_SEQ           INT       NOT NULL,
            STG_START_DTM     TIMESTAMP NOT NULL,
            STG_END_DTM       TIMESTAMP,
            STG_TYPE          CHAR(75)  NOT NULL,
            STG_STATUS        INT,
            STG_ODBC_INS      INT,
            STG_ODBC_UPD      INT,
            STG_ODBC_DEL      INT,
            STG_ODBC_REJ      INT,
            LAST_ACTY_OPER_ID CHARACTER(15) NOT NULL DEFAULT ' ',
            LAST_ACTY_DTM     TIMESTAMP     NOT NULL DEFAULT CURRENT TIMESTAMP,
        */

        d1->ClearSql();
        d1->SetSql(" INSERT INTO AUDIT.T_DS_LOADSTATS_STG ( ");
        d1->SetSql("     LOAD_ID,           ");
        d1->SetSql("     STG_PROJ_NM,       ");
        d1->SetSql("     STG_JOB_NM,        ");
        d1->SetSql("     STG_NM,            ");
        d1->SetSql("     STG_SEQ,           ");
        d1->SetSql("     STG_START_DTM,     ");
        d1->SetSql("     STG_END_DTM,       ");
        d1->SetSql("     STG_TYPE,          ");
        d1->SetSql("     STG_STATUS,        ");
        d1->SetSql("     STG_ODBC_INS,      ");
        d1->SetSql("     STG_ODBC_UPD,      ");
        d1->SetSql("     STG_ODBC_DEL,      ");
        d1->SetSql("     STG_ODBC_REJ,      ");
        d1->SetSql("     LAST_ACTY_OPER_ID  ");
        d1->SetSql(" ) VALUES (             ");
        d1->SetSql("     %s,                ", loadId);
        d1->SetSql("     '%s',              ", projName);
        d1->SetSql("     '%s',              ", jobName);
        d1->SetSql("     '%s',              ", stageName);
        d1->SetSql("     %d,                ", seq);
        d1->SetSql("     '%s',              ", fts);
        d1->SetSql("     '%s',              ", ets); 
        d1->SetSql("     '%s',              ", typeName);
        d1->SetSql("     %s,                ", stageStatus);
        d1->SetSql("     %s,                ", strlen(odbcCounts.rowsIns) ? odbcCounts.rowsIns : "0");
        d1->SetSql("     %s,                ", strlen(odbcCounts.rowsUpd) ? odbcCounts.rowsUpd : "0");
        d1->SetSql("     %s,                ", strlen(odbcCounts.rowsDel) ? odbcCounts.rowsDel : "0");
        d1->SetSql("     %s,                ", strlen(odbcCounts.rowsRej) ? odbcCounts.rowsRej : "0");
        d1->SetSql("     'DSLSC++%s'        ", DSAPIPRU_VER);
        d1->SetSql(" ) WITH UR              ");

        // d1->ShowSql();

        if (d1->Exec())
            return true;

        return false;
    }

char * DsApiPru::GetLastError(void)
{
    switch (err) {
        // Open Project
        case DSJE_BAD_VERSION:
            strcpy(errMsg, "Bad Version");
            break;
        case DSJE_INCOMPATIBLE_SERVER:
            strcpy(errMsg, "Incompatible server");
            break;
        case DSJE_SERVER_ERROR:
            strcpy(errMsg, "Server error");
            break;
        case DSJE_BADPROJECT:
            strcpy(errMsg, "Bad project");
            break;
        case DSJE_NO_DATASTAGE:
            strcpy(errMsg, "No DataStage");
            break;
        // Open Job
        case DSJE_OPENFAIL:
            strcpy(errMsg, "Enjine failed to open job");
            break;
        case DSJE_NO_MEMORY:
            strcpy(errMsg, "Memory allocation failure");
            break;
        case DSJE_NOT_AVAILABLE:
            strcpy(errMsg, "There are no instances of the requested information in the job");
            break;
        case DSJE_BADHANDLE:
            strcpy(errMsg, "Invalid Job Handle");
            break;
        case DSJE_BADTYPE:
            strcpy(errMsg, "Invalid InfoType");
            break;
        default:
            strcpy(errMsg, "Unknown error");
            break;
    }

    return errMsg;
}

bool DsApiPru::OdbcCountSearchLog(char *ostage)
{
    // Parse the DS log for upsert stats from ODBC stage

    // Log:[odbc_napv_fact,0: ROWS INSERTED :1,873   ] = 0
    // Log:[odbc_napv_fact,0: ROWS UPDATED  :546   ]   = 1
    // Log:[odbc_napv_fact,0: ROWS DELETED  :0   ]     = 2
    // Log:[odbc_napv_fact,0: ROWS REJECTED :0   ]     = 3

    odbcCounts.rowsDel[0] = odbcCounts.rowsIns[0] = 
        odbcCounts.rowsRej[0] = odbcCounts.rowsUpd[0] = '\0';

    char rows_affected[4][BUFSIZ];
    char odbc_stage[4][BUFSIZ];
    char db_op[4][BUFSIZ];

    strcpy(db_op[0], "ROWS INSERTED :"); 
    strcpy(db_op[1], "ROWS UPDATED  :"); 
    strcpy(db_op[2], "ROWS DELETED  :"); 
    strcpy(db_op[3], "ROWS REJECTED :"); 

    for (int i=0; i<4; i++) 
        *rows_affected[i] = *odbc_stage[i] = '\0';

    int idx;

    err = DSFindFirstLogEntry(hJob, DSJ_LOGANY, 0, 0, 5000, &logEventRec);
    if (err != DSJE_NOERROR) {
        if (err == DSJE_NOMORE)
            return false;
        l1->WriteErr("API Call DSFindFirstLogEntry(DSJ_LOGANY) failed");
        l1->WriteErr("API Error: %s", DSGetLastErrorMsg(hProject));
        return true;
    }
    // printf("\nLog0:[%s]",logEventRec.message);    

    while(true) {
        err = DSFindNextLogEntry(hJob, &logEventRec);
        if (err != DSJE_NOERROR) {
            if (err == DSJE_NOMORE)
                break;
            l1->WriteErr("API Call DSFindNextLogEntry(DSJ_LOGANY) failed");
            l1->WriteErr("API Error: %s", DSGetLastErrorMsg(hProject));
            return true;
        }
        // printf("\nLog1:[%s]",logEventRec.message);    
        if (strstr(logEventRec.message, db_op[0])) 
            idx = 0;
        else if (strstr(logEventRec.message, db_op[1]))
            idx = 1;
        else if (strstr(logEventRec.message, db_op[2])) 
            idx = 2;
        else if (strstr(logEventRec.message, db_op[3])) 
            idx = 3;
        else
            idx = -1;

        if (idx >= 0) {
            strcpy(odbc_stage[idx], logEventRec.message);
            if (strchr(odbc_stage[idx], ','))
                *(strchr(odbc_stage[idx], ',')) = '\0';
            else {
                l1->WriteErr("Log Parse Error for a Database Connector Stage");
                ErrExit(16, "Expected comma in line [%s]", logEventRec.message);
            }
            int colons=0;
            char *ptr;
            for (ptr=logEventRec.message; *ptr; ptr++) {
                if (*ptr == ':')
                    colons++;
                if (colons == 2)
                    break;
            }
            if (colons == 2) {
                strcpy(rows_affected[idx], ptr+1);
                while (strchr(rows_affected[idx], ','))
                    strcpy(strchr(rows_affected[idx], ','), strchr(rows_affected[idx], ',')+1);
            } else {
                l1->WriteErr("Log Parse Error for a Database Connector Stage");
                ErrExit(17, "Expected 2 colons in line [%s]", logEventRec.message);
            }
            if (!strcmp(odbc_stage[idx], ostage)) {
                switch (idx) {
                    case 0:    strcpy(odbcCounts.rowsIns, rows_affected[idx]); break;
                    case 1:    strcpy(odbcCounts.rowsUpd, rows_affected[idx]); break;
                    case 2:    strcpy(odbcCounts.rowsDel, rows_affected[idx]); break;
                    case 3:    strcpy(odbcCounts.rowsRej, rows_affected[idx]); break;
                }
            }
        } 
    }

    // for (i=0; i<4; i++) 
    //     l1->WriteFyi("  Stage: [%s], %s [%s]", odbc_stage[i], db_op[i], rows_affected[i]);

    return false;
    
}

bool DsApiPru::Db2CountSearchLog(char *ostage)
{
    // Parse the DS log for upsert stats from ODBC stage

    // Log:[DB2_Connector_3,0: Number of rows updated: 1,873   ] = 0
    // Log:[DB2_Connector_3,0: Number of rows inserted: 546   ]   = 1
    // Log:[DB2_Connector_3,0: Number of rows rejected: 0   ]     = 2
    // Log:[DB2_Connector_3,0: Number of rows deleted: 0   ]     = 3

    odbcCounts.rowsDel[0] = odbcCounts.rowsIns[0] = 
        odbcCounts.rowsRej[0] = odbcCounts.rowsUpd[0] = '\0';

    char rows_affected[4][BUFSIZ];
    char odbc_stage[4][BUFSIZ];
    char db_op[4][BUFSIZ];

    strcpy(db_op[0], "Number of rows inserted: "); 
    strcpy(db_op[1], "Number of rows updated: "); 
    strcpy(db_op[2], "Number of rows rejected: "); 
    strcpy(db_op[3], "Number of rows deleted: "); 

    for (int i=0; i<4; i++) 
        *rows_affected[i] = *odbc_stage[i] = '\0';

    int idx;

    err = DSFindFirstLogEntry(hJob, DSJ_LOGANY, 0, 0, 5000, &logEventRec);
    if (err != DSJE_NOERROR) {
        if (err == DSJE_NOMORE)
            return false;
        l1->WriteErr("API Call DSFindFirstLogEntry(DSJ_LOGANY) failed");
        l1->WriteErr("API Error: %s", DSGetLastErrorMsg(hProject));
        return true;
    }
    // printf("\nLog0:[%s]",logEventRec.message);    

    while(true) {
        err = DSFindNextLogEntry(hJob, &logEventRec);
        if (err != DSJE_NOERROR) {
            if (err == DSJE_NOMORE)
                break;
            l1->WriteErr("API Call DSFindNextLogEntry(DSJ_LOGANY) failed");
            l1->WriteErr("API Error: %s", DSGetLastErrorMsg(hProject));
            return true;
        }
        // printf("\nLog1:[%s]",logEventRec.message);    
        if (strstr(logEventRec.message, db_op[0])) 
            idx = 0;
        else if (strstr(logEventRec.message, db_op[1]))
            idx = 1;
        else if (strstr(logEventRec.message, db_op[2])) 
            idx = 2;
        else if (strstr(logEventRec.message, db_op[3])) 
            idx = 3;
        else
            idx = -1;

        if (idx >= 0) {
            strcpy(odbc_stage[idx], logEventRec.message);
            if (strchr(odbc_stage[idx], ','))
                *(strchr(odbc_stage[idx], ',')) = '\0';
            else {
                l1->WriteErr("Log Parse Error for a Database Connector Stage");
                ErrExit(16, "Expected comma in line [%s]", logEventRec.message);
            }
            int colons=0;
            char *ptr;
            for (ptr=logEventRec.message; *ptr; ptr++) {
                if (*ptr == ':')
                    colons++;
                if (colons == 2)
                    break;
            }
            if (colons == 2) {
                strcpy(rows_affected[idx], ptr+1);
                while (strchr(rows_affected[idx], ','))
                    strcpy(strchr(rows_affected[idx], ','), strchr(rows_affected[idx], ',')+1);
            } else {
                l1->WriteErr("Log Parse Error for a Database Connector Stage");
                ErrExit(17, "Expected 2 colons in line [%s]", logEventRec.message);
            }
            if (!strcmp(odbc_stage[idx], ostage)) {
                switch (idx) {
                    case 0:    strcpy(odbcCounts.rowsIns, rows_affected[idx]); break;
                    case 1:    strcpy(odbcCounts.rowsUpd, rows_affected[idx]); break;
                    case 2:    strcpy(odbcCounts.rowsDel, rows_affected[idx]); break;
                    case 3:    strcpy(odbcCounts.rowsRej, rows_affected[idx]); break;
                }
            }
        } 
    }

    // for (i=0; i<4; i++) 
    //     l1->WriteFyi("  Stage: [%s], %s [%s]", odbc_stage[i], db_op[i], rows_affected[i]);

    return false;
    
}

bool DsApiPru::GetUniqueLoadId(void)
{
    static SQLCHAR uniqts[BUFSIZ];
    char buf[BUFSIZ];

    d1->ClearSql();
    d1->SetSql(" SELECT RTRIM(CHAR(BIGINT(SUBSTR(REPLACE(REPLACE(REPLACE(CHAR(");
    d1->SetSql("                CURRENT TIMESTAMP),':',''),'.',''),'-',''),1,19))))");
    d1->SetSql("   FROM SYSIBM.SYSDUMMY1");

    if (d1->Exec())
        return true;

    if (d1->Bind(1, &uniqts, sizeof(uniqts)))
        return true;

    int num_rows=0;
    SQLRETURN RetCode;
    while ((RetCode = d1->Fetch()) != SQL_NO_DATA) {
        if (RetCode == SQL_ERROR)
            return true;
    }

    if (d1->CloseCursor())
        return true;

    sprintf(buf, "%s", uniqts);
    if (!(loadId=strdup(buf))) {
        l1->WriteErr("strdup(loadId) Failed");
        return true;
    }
    
    l1->WriteFyi("Load ID (BIGINT) for current run: %s", loadId);
            
    return false;
}

bool DsApiPru::InsertJobRec(void)
{
    /*
        LOAD_ID           BIGINT    NOT NULL,
        JOB_PROJ_NM       CHAR(75)  NOT NULL,
        JOB_NM            CHAR(75)  NOT NULL,
        LAST_ACTY_OPER_ID CHARACTER(15) NOT NULL DEFAULT ' ',
        LAST_ACTY_DTM     TIMESTAMP     NOT NULL DEFAULT CURRENT TIMESTAMP,
    */

    d1->ClearSql();
    d1->SetSql(" INSERT INTO AUDIT.T_DS_LOADSTATS_JOB ( ");
    d1->SetSql("     LOAD_ID,           ");
    d1->SetSql("     JOB_PROJ_NM,       ");
    d1->SetSql("     JOB_NM,            ");
    d1->SetSql("     LAST_ACTY_OPER_ID  ");
    d1->SetSql(" ) VALUES (             ");
    d1->SetSql("     %s,                ", loadId);
    d1->SetSql("     '%s',              ", projName);
    d1->SetSql("     '%s',              ", jobName);
    d1->SetSql("     'DSLSC++%s'        ", DSAPIPRU_VER);
    d1->SetSql(" ) WITH UR              ");

    if (d1->Exec())
        return true;

    return false;
}

