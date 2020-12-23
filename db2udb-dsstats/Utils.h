
// U\
// ***********************************************************************
// *                                                                     *
// * Generic Utilities Class                                 Mohan Palat *
// *                                                            10/27/11 *
// *                                                                     *
// * Description : Generic Routines Categorized as                       *
// *               1. General Methods like trim(), replace() etc         *
// *               2. 2 Dimensional Array Operation Methods              *
// *               3. Property File Manipulation Methods                 *
// *               4. Time Related Methods                               *
// *                                                                     *
// * Program File : Utils.cpp                                            *
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
// * xlC -I$DB2PATH/include -c Utils.cpp                                 *
// * # Link the Utils.o                                                  *
// *                                                                     *
// ***********************************************************************
//

/* 
=========================
Generic Utility Routines.
=========================
*/

#ifndef _UTILS_HEADER_
#define _UTILS_HEADER_
#define _USE_32BIT_TIME_T

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#define DSSTYLEBUFSIZ BUFSIZ*15

class Utils {
    private:
        char dsstylebuf[DSSTYLEBUFSIZ];
        int elemcount;
        char **arr;
        time_t t1, t2;
        struct tm tm0, *tm1, *tm2;
        char weekday1[BUFSIZ], weekday2[BUFSIZ];
        char ts1[BUFSIZ], ts2[BUFSIZ];
        char yymmdd1[BUFSIZ], yymmdd2[BUFSIZ];
        char mmddyy1[BUFSIZ], mmddyy2[BUFSIZ];
        char free1[BUFSIZ], free2[BUFSIZ];
    public:
        Utils();
        ~Utils();
        // General Methods
        void Rtrim(char *str);
        void Trim(char *str);
        void Ltrim(char *str);
        char * Upper(char *str);        
        char * Lower(char *str);        
        bool Replace(char *str, char *from_pat, char *to_pat);
        // 2 Dimensional Array Operation Methods
        bool CreateArray(char *str2parse, int &counter, char ***sarray);
        bool CreateArray(void); 
        bool DestroyArray(int &counter, char ***sarray);
        bool DestroyArray(void); 
        char *ArrayElement(int idx) { return arr[idx]; }
        int  ArrayElementCount(void) { return elemcount; }
        // Property File Manipulation Methods
        bool ParamVal(char *pfile, char *par, char *val, char *separator=NULL);
        void AddString(char *str);
        void ListString(void);
        void EmptyString(void) {dsstylebuf[0]=dsstylebuf[1]='\0'; return;}        
        // Time Related Methods
        void TimeSet(int tnum=0,char *Y=NULL,char *m=NULL,char *d=NULL,
                                       char *H=NULL,char *M=NULL,char *S=NULL);
        bool TimeSetYymmdd(int tnum=0, char *tstr=NULL);
        void TimeSetNow(int tnum=0);
        char *TimeGet(int tnum=0, char *fmt="RAW");
        void TimeAdd(int tnum, int units, char *timepart);
        time_t TimeDiff(char *timepart);
        void TimeFormat(int tnum);
        int LastDayOfMonth(int year, int month);        
        int TimePart(int tnum=0, char *timepart="YEARDAY");

};

#endif // _UTILS_HEADER_
        
