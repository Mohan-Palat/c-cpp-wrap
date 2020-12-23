// U\
// *********************************************************************** 
// *                                                                     *
// * Log Class                                               Mohan Palat *
// *                                                            10/20/06 *
// *                                                                     *
// * Description : Create a common Log object and once opened            *
// *               file pointer can be shared by all objects             *
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
// * xlC $EXTRA_CFLAG -I$DB2PATH/include -c Log.cpp                      *
// *                                                                     *
// *********************************************************************** 

#include "Log.h"

Log::Log(char *fn, char *mode) 
{ 
    logfp=fopen(fn, mode); 
    strcpy(logfname,fn); 
    if (logfp)
        fprintf(logfp, "\n<BOL>");
    fflush(logfp);
} 

Log::~Log() 
{ 
    if (logfp)
        fprintf(logfp, "\n<EOL>\n");
    fclose(logfp); 
} 

void Log::Write(char *err_tag, char *msg)
{
    char log[BUFSIZ];
    time_t ltime;
    time(&ltime);
    struct tm *today;
    today = localtime(&ltime);
    memset(log, '\0', BUFSIZ);
    strftime(log, 27, "\n[%Y-%m-%d %H:%M:%S]", today);
    strcat(log, err_tag);
    strcat(log, msg);
    fprintf(logfp, "%s", log);
    return;
}

void Log::WriteFyi(const char* format, ...)
{
    char log[BUFSIZ];
    time_t ltime;
    time(&ltime);
    struct tm *today;
    today = localtime(&ltime);
    memset(log, '\0', BUFSIZ);
    strftime(log, 27, "\n[%Y-%m-%d %H:%M:%S]", today);
    strcat(log, "[FYI] ");
    fprintf(logfp, "%s", log);
    va_list args;
    va_start(args, format);
    vfprintf(logfp, format, args);
    va_end(args);
    return;
}

void Log::WriteErr(const char* format, ...)
{
    char log[BUFSIZ];
    time_t ltime;
    time(&ltime);
    struct tm *today;
    today = localtime(&ltime);
    memset(log, '\0', BUFSIZ);
    strftime(log, 27, "\n[%Y-%m-%d %H:%M:%S]", today);
    strcat(log, "[ERR] ");
    fprintf(logfp, "%s", log);
    va_list args;
    va_start(args, format);
    vfprintf(logfp, format, args);
    va_end(args);
    return;
}


