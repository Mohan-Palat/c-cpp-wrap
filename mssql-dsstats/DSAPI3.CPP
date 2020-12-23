// U\
// ***********************************************************************
// *                                                                     *
// * DataStage Load Stats collection using C++ API           Mohan Palat *
// * DsApiPru.h                                                 11/26/11 *
// *                                                                     *
// * Methods Head: DsApiPru.h                                            *
// * Methods File: DsApiPru.cpp                                          *
// *                                                                     *
// * Description : Refer to the comments frrom DsApiPru.h                *
// *                                                                     *
// *     By        Date                 Description                      *
// * ----------- ---------- -------------------------------------------- *
// * Mohan       2014-04-17 First working copy for                       * 
//                             a) Windows 7                              *
//                             b) DB2 Verison 9 CLI Libraries            *
//                             c) Multiple DataStage Instances           *
//                             d) ErrExit() Function Added for ease      *
// * Mohan       2017-07-21 Linux Recompile with ODBC Library            * 
// ***********************************************************************
//    

// Linux Recompile Changes
// SQLUINTEGER cs; 2017-07-21 Compiling with odbc library and include from folder /usr 
// SQLINTEGER  *s_or_i; 2017-07-21 Compiling with odbc library and include from folder /usr 
// SQLINTEGER num_rows; 2017-07-21 Compiling with odbc library and include from folder /usr 

#define _USE_32BIT_TIME_T

#ifdef _MSC_VER 
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "DsApiPru.h" 

int initialize(DsApiPru *dap);
int process(DsApiPru *dap);
void terminate(DsApiPru *dap);

int main(int argc, char **argv)
{
    if (argc != 6) {
        fprintf(stderr, "\nUsage: %s <1. DS Project Name> <2. DS Job Name> <3. DB Connection Property File> <4. Log Folder> <5. Switch: LOG_OVERWRITE | LOG_APPEND>\n\n", argv[0]);
        return 100;
    }

    DsApiPru *dap = new DsApiPru(argv[1], argv[2], argv[3], argv[4], argv[5]);

    int rc=0;
    if (!(rc = initialize(dap)))
        rc = process(dap); 

    terminate(dap);

    delete dap;

    return rc;
}

int initialize(DsApiPru *dap)
{
    if (dap->OpenProject()) 
        return 101;

    if (dap->LoadJobs()) 
        return 102;

    if (dap->OpenJob()) 
        return 103;

    return 0;
}


int process(DsApiPru *dap)
{
    if (dap->LoadStages()) 
        return 104;
    
    if (dap->ListStages()) 
        return 105;
    
    if (dap->ProcessStages()) 
        return 106;

    // if (dap->ListProjects()) 
    //     return 107;
    
    // if (dap->ListJobs()) 
    //     return 108;

    return 0;
}

void terminate(DsApiPru *dap)
{
    dap->CloseJob();
    dap->CloseProject();

    return;
}
