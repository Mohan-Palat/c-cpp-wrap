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

#ifndef _LOG_HEADER_
#define _LOG_HEADER_
#define _USE_32BIT_TIME_T

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstdarg>
#include <cstring>
#include <cctype>
#include <ctime>

class Log {
    public: 
        FILE *logfp;
        char logfname[BUFSIZ];
    public:
        Log(char *fn, char *mode="at"); // Contruct
        ~Log(); // Destruct
        FILE * GetFP(void) { return logfp; }
        void GetName(char *nm) { strcpy(nm, logfname); return; } 
        void Write(char *err_tag, char *msg);
        void WriteFyi(const char* format, ...);
        void WriteErr(const char* format, ...);
        void Flush(void) { fflush(logfp); }
};

#endif // _LOG_HEADER_

