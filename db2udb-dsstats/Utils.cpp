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
// * Header File : Utils.h                                               *
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

#define _USE_32BIT_TIME_T

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstdarg>
#include <cstring>
#include <cctype>
#include <ctime>
#include "Utils.h"

/* 
=========================
Generic Utility Routines.
=========================
*/

Utils::Utils()
{
    EmptyString();
    elemcount = 0;
    arr = NULL;
    TimeSet(0);
    TimeSet(1);
}


Utils::~Utils()
{
    if (elemcount)
        DestroyArray();            
}

/*
+-----------------------------------------------------------------------------+
|                                                                             |
| Function : R Trim                                                           |
|            ------                                                           |
|  Inputs  : String to be right trimmed                                       |
|                                                                             |
|  Outputs : None                                                             |
|                                                                             |
|  Process :                                                                  |
|  -------                                                                    |
|      Right Trim white spaces                                                |
|      Entirely reusable code                                                 |
|                                                                             |
+-----------------------------------------------------------------------------+
*/
void Utils::Rtrim(char *str)
{
    while(strlen(str))
        if (isspace(*(str+strlen(str)-1)))
            *(str+strlen(str)-1) = '\0';
        else
            break;
    return;
}

/*
+-----------------------------------------------------------------------------+
|                                                                             |
| Function : Trim                                                             |
|            ----                                                             |
|  Inputs  : String to be stripped of whitespaces                             |
|                                                                             |
|  Outputs : None                                                             |
|                                                                             |
|  Process :                                                                  |
|  -------                                                                    |
|      Remove all white spaces from the given string                          |
|                                                                             |
------------------------------------------------------------------------------+
*/

void Utils::Trim(char *str)
{
    while(*str)
        if (isspace(*str))
            strcpy(str, str+1);
        else
            str++;
    return;
}

/*
+-----------------------------------------------------------------------------+
|                                                                             |
| Function : L Trim                                                           |
|            ------                                                           |
|  Inputs  : String to be left trimmed of whitespaces                         |
|                                                                             |
|  Outputs : None                                                             |
|                                                                             |
|  Process :                                                                  |
|  -------                                                                    |
|      Remove left padded white spaces from the given string                  |
|                                                                             |
------------------------------------------------------------------------------+
*/

void Utils::Ltrim(char *str)
{
    while(*str)
        if (isspace(*str))
            strcpy(str, str+1);
        else
            break;
    return;
}


/*
+-----------------------------------------------------------------------------+
|                                                                             |
| Function : Replace                                                          |
|            -------                                                          |
|  Inputs  : String which contains the pattern to be replaced                 |
|            Pattern to be replaced (from)                                    |
|            Replace pattern (to)                                             |
|                                                                             |
|  Outputs : True if the string misses the pattern to be replaced             |
|                                                                             |
|  Process :                                                                  |
|  -------                                                                    |
|      Replace the first occurence of the from_pattern with to_pattern        |
|                                                                             |
+-----------------------------------------------------------------------------+
*/

bool Utils::Replace(char *str, char *from_pat, char *to_pat)
{
    char *ptr = str;
    char buf[BUFSIZ];

    if (!strstr(ptr, from_pat))
        return true; // Pattern not found to replace

    while(true) {
        if (strstr(ptr, from_pat)) {
            strcpy(buf, ptr);
            *(strstr(buf, from_pat)) = '\0';
            strcat(buf, to_pat);
            strcat(buf, strstr(ptr, from_pat)+strlen(from_pat));
            strcpy(ptr, buf);
        } else
            break;
        ptr = strstr(ptr, to_pat) + strlen(to_pat);
    }
        
    return false; // Replaced
}

/*
+-----------------------------------------------------------------------------+
|                                                                             |
| Function : Create 2 Dimensional String Array                                |
|            ---------------------------------                                |
|  Inputs  : String which has DataStage API Style buffer of the form          |
|             <String1>\0<String2>\0...<Stringn>\0\0                          |
|            Ref of the integer counter (Will populate the # strings found)   |
|            Address of the 2 dim array which will be malloced                |
|                                                                             |
|  Outputs : Boolean (True on failure Fales on success)                       |
|                                                                             |
|  Process :                                                                  |
|  -------                                                                    |
|      Create a 2 dimensional array of Strings                                |
|                                                                             |
+-----------------------------------------------------------------------------+
*/

bool Utils::CreateArray(char *str2parse, int &counter, char ***sarray)
{
    if (str2parse) {
        if (*str2parse == '\0' && *(str2parse+1) == '\0') {
            printf("\nString to Parse is empty\n");
            return true;
        }
    } else {
        printf("\nString to Parse is NULL\n");
        return true;
    }

    counter=1;
    for (char *p=str2parse; !(*p == '\0' && *(p+1) == '\0'); p++)
        if (*p == '\0')
            counter++;
    //printf("\nString Count = %d\n", counter);
    
    *sarray=((char **) malloc(sizeof(char *) * (counter)));
    if (!(*sarray)) {
        fprintf(stderr, "malloc of 2 DIM Array failed");
        exit(1);
    }

    char *p=str2parse;
    int i;
    for (i=0; i<counter; i++) {
        //printf("\n[%02d][%s]", i+1, p);
        (*sarray)[i] = strdup(p);
        p += strlen(p) + 1;
    }
   
    return false;
}


/*
+-----------------------------------------------------------------------------+
|                                                                             |
| Function : Create 2 Dimensional String Array                                |
|            ---------------------------------                                |
|  Inputs  : None                                                             |
|                                                                             |
|  Outputs : Boolean (True on failure Fales on success)                       |
|                                                                             |
|  Process :                                                                  |
|  -------                                                                    |
|      Create a 2 dimensional array of Strings using the DataStage Style Buf  |
|                                                                             |
+-----------------------------------------------------------------------------+
*/

bool Utils::CreateArray(void)
{
    if (dsstylebuf[0]=='\0' && dsstylebuf[1]=='\0') {
        printf("\nDS Style String to Parse is NULL\n");
        return true;
    } 

    if (elemcount)
        DestroyArray();            

    elemcount=1;
    for (char *p=dsstylebuf; !(*p == '\0' && *(p+1) == '\0'); p++)
        if (*p == '\0')
            elemcount++;
    //printf("\nString Count = %d\n", elemcount);
    
    arr=((char **) malloc(sizeof(char *) * (elemcount)));
    if (!arr) {
        fprintf(stderr, "malloc of 2 DIM Array failed");
        exit(1);
    }

    char *p=dsstylebuf;
    int i;
    for (i=0; i<elemcount; i++) {
        //printf("\n[%02d][%s]", i+1, p);
        arr[i] = strdup(p);
        p += strlen(p) + 1;
    }
   
    return false;
}


/*
+-----------------------------------------------------------------------------+
|                                                                             |
| Function : Destroy 2 Dimensional String Array                               |
|            ----------------------------------                               |
|  Inputs  : Ref of the integer counter (Will populate the # strings found)   |
|            Address of the 2 dim array which will be malloced                |
|                                                                             |
|  Outputs : False (Success)                                                  |
|                                                                             |
|  Process :                                                                  |
|  -------                                                                    |
|      Destroy the user created 2 dimensional string array                    |
|                                                                             |
+-----------------------------------------------------------------------------+
*/

bool Utils::DestroyArray(int &counter, char ***sarray)
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

/*
+-----------------------------------------------------------------------------+
|                                                                             |
| Function : Destroy 2 Dimensional String Array                               |
|            ----------------------------------                               |
|  Inputs  : None                                                             |
|                                                                             |
|  Outputs : False (Success)                                                  |
|                                                                             |
|  Process :                                                                  |
|  -------                                                                    |
|      Destroy the internal 2 dimensional string array                        |
|                                                                             |
+-----------------------------------------------------------------------------+
*/

bool Utils::DestroyArray(void)
{
    for (int i=0; i<elemcount; i++) {
        free(arr[i]);
        arr[i] = NULL;
    }
    free(arr);
    arr = NULL;
    elemcount = 0;

    return false;
}


/*
+-----------------------------------------------------------------------------+
|                                                                             |
| Function : Parameter's Value                                                |
|            -----------------                                                |
|  Inputs  : Parameter File with Param Value pairs (Input)                    |
|            Parameter string (Input)                                         |
|            Value string (Output)                                            |
|                                                                             |
|  Outputs : False (Success) True (Failed fetching value)                     |
|                                                                             |
|  Process :                                                                  |
|  -------                                                                    |
|      If Param and corresponding Value is found, return the value.           |
|                                                                             |
+-----------------------------------------------------------------------------+
*/

bool Utils::ParamVal(char *pfile, char *par, char *val, char *separator)
{
    *val = '\0';

    FILE *fp = fopen(pfile, "rt");
    if (!fp) {
        fprintf(stderr, "\nUnable to open Parameter Value File [%s]\n", pfile);
        return true;
    }
   
    char buf[BUFSIZ*8], sval[BUFSIZ];
    while(fgets(buf, BUFSIZ*8, fp)) {
        Ltrim(buf);
        if (!strstr(buf, par))
            continue;
        if (separator)
            if (!strstr(buf, separator))
                continue;
        if (strchr(buf, '\n'))
            *(strchr(buf, '\n')) = '\0';
        char *p = buf;
        if (separator) {
            while(*p != *separator) {
                if (*p == '\0') 
                    continue;
                else
                    p++;
            }
        } else {
            while(!(isspace(*p))) {
                if (*p == '\0') 
                    continue;
                else
                    p++;
            }
        }
        if (*p) {
            strcpy(sval, p+1);
            Ltrim(sval);
            *p = '\0';
            Rtrim(p);
            if (!strcmp(par, buf)) {
                strcpy(val, sval);
                fclose(fp);
                return false;
            }
        }
    }

    fprintf(stderr, "\nUnable to get Value for Param [%s] in Param File [%s]\n", par, pfile);    
    fclose(fp);
    return true;
}

void Utils::AddString(char *str)
{
    char *p1=dsstylebuf;
    char *p2=dsstylebuf+1;

    if (!(*p1) && !(*p2)) { // First String
        strcpy(dsstylebuf, str);
        *(dsstylebuf+strlen(dsstylebuf)+1) = '\0';
        return;
    }

    int i=0;
    while (*p2 || *p1) {
        i++;
        if (i >= (DSSTYLEBUFSIZ-3)) {
            fprintf(stderr, "\nDataStage Style Buffer Operation Error\n");
            exit(1);
        }
        p1++;
        p2++;
    }
    for (char *p=str; *p; p++) {
        *p2 = *p;
        p2++;
    }
    *p2 = '\0';
    *(p2+1) = '\0';
        
    return;
}

void Utils::ListString(void)
{
    char *p1=dsstylebuf;
    char *p2=dsstylebuf+1;
    int i=0;
    while (*p2 || *p1) {
        i++;
        if (i >= (DSSTYLEBUFSIZ-3)) {
            fprintf(stderr, "\nDataStage Style Buffer Operation Error\n");
            exit(1);
        }
        if (*p1)
            printf("[%c]", *p1);
        else
            printf("<\\0>");
        p1++;
        p2++;
    }

    printf("<\\0><\\0>\n");

    return;
}

bool Utils::TimeSetYymmdd(int tnum, char *tstr)
{
    char buf[6][BUFSIZ];

    // CCYY-MM-DD HH:MM:SS

    if ( *(tstr+4)=='-' && *(tstr+7)=='-' &&
             *(tstr+13)==':' && *(tstr+16)==':' && strlen(tstr)==19) {
        // 0000000000111111111
        // 0123456789012345678
        // CCYY-MM-DD HH:MM:SS (19 Characters)
        // printf("\n[%c|%c||%c|%c][%d]\n",
        //      *(tstr+4),*(tstr+7),*(tstr+13),*(tstr+16), strlen(tstr));
        int pos[]={0,5,8,11,14,17};
        int len[]={4,2,2,2,2,2};
        for(int i=0; i<6; i++) {
            strcpy(buf[i], tstr+pos[i]); 
            *(buf[i]+len[i]) = '\0';
            // printf("\n[%s]", buf[i]);
        }
        TimeSet(tnum, buf[0], buf[1], buf[2], buf[3], buf[4], buf[5]);
    return false;
    } 

    // MM/DD/CCYY HH:MM:SS
    
    if ( *(tstr+2)=='/' && *(tstr+5)=='/' &&
             *(tstr+13)==':' && *(tstr+16)==':' && strlen(tstr)==19) {
        // 0000000000111111111
        // 0123456789012345678
        // 11/14/2011 20:00:00 (19 Characters)
        // printf("\n[%c|%c||%c|%c][%d]\n",
        //      *(tstr+2),*(tstr+5),*(tstr+13),*(tstr+16), strlen(tstr));
    char ntstr[BUFSIZ];
    strcpy(ntstr, tstr+6);
    *(ntstr+4)='\0';
    strcat(ntstr, "-");
    strcat(ntstr, tstr);
    *(ntstr+7)='\0';
    strcat(ntstr, "-");
    strcat(ntstr, tstr+3);
    *(ntstr+10)='\0';
    strcat(ntstr, tstr+10);
        // printf("\n[%s] Converted to [%s]", tstr, ntstr);
        int pos[]={0,5,8,11,14,17};
        int len[]={4,2,2,2,2,2};
        for(int i=0; i<6; i++) {
            strcpy(buf[i], ntstr+pos[i]); 
            *(buf[i]+len[i]) = '\0';
            // printf("\n[%s]", buf[i]);
        }
        TimeSet(tnum, buf[0], buf[1], buf[2], buf[3], buf[4], buf[5]);
    return false;
    } 

    return true;
}       

void Utils::TimeSet(int tnum, char *Y, char *m, char *d, char *H, char *M, char *S)
{
    // tm_sec seconds after the minute 0-61* 
    // tm_min minutes after the hour 0-59 
    // tm_hour hours since midnight 0-23 
    // tm_mday day of the month 1-31 
    // tm_mon months since January 0-11 
    // tm_year years since 1900  
    // tm_wday days since Sunday 0-6 
    // tm_yday days since January 1 0-365 
    // tm_isdst Daylight Saving Time flag (1 When DST is on, 0 otherwise)

    // printf("\nY=%s,m=%s,d=%s, H=%s,M=%s,S=%s",Y,m,d,H,M,S);

    if (!tnum) {
        if (!Y && !m && !d && !H && !M && !S) {
            *ts1 = '\0';
            tm1 = NULL;
            t1 = 0; 
            *weekday1 = '\0';
        } else {
            tm0.tm_isdst = -1; // Please calc daylight savings
            if (S) sscanf(S, "%d", &tm0.tm_sec);
            if (M) sscanf(M, "%d", &tm0.tm_min);
            if (H) sscanf(H, "%d", &tm0.tm_hour);
            if (d) sscanf(d, "%d", &tm0.tm_mday);
            if (m) sscanf(m, "%d", &tm0.tm_mon);
            tm0.tm_mon--;
            if (Y) sscanf(Y, "%d", &tm0.tm_year);
            tm0.tm_year -= 1900;
            t1 = mktime(&tm0);
            tm1 = localtime(&t1);
            TimeFormat(0);
        }
    } else {
        if (!Y && !m && !d && !H && !M && !S) {
            *ts2 = '\0'; 
            tm2 = NULL;
            t2 = 0; 
            *weekday2 = '\0';
        } else {
            tm0.tm_isdst = -1; // Please calc daylight savings
            if (S) sscanf(S, "%d", &tm0.tm_sec);
            if (M) sscanf(M, "%d", &tm0.tm_min);
            if (H) sscanf(H, "%d", &tm0.tm_hour);
            if (d) sscanf(d, "%d", &tm0.tm_mday);
            if (m) sscanf(m, "%d", &tm0.tm_mon);
            tm0.tm_mon--;
            if (Y) sscanf(Y, "%d", &tm0.tm_year);
            tm0.tm_year -= 1900;
            t2 = mktime(&tm0);
            tm2 = localtime(&t2);
            TimeFormat(1);
        }
    }
    
    return;
} 

void Utils::TimeSetNow(int tnum)
{
    if (!tnum) {
        time(&t1);
        tm1 = localtime(&t1);
        TimeFormat(0);
    } else {
        time(&t2);
        tm2 = localtime(&t2);
        TimeFormat(1);
    }
    
    return;
} 
        
char *Utils::TimeGet(int tnum, char *fmt) 
{
    if (!strcmp(fmt,"YYMMDD")) {
        if (!tnum)
            return yymmdd1;
        else
            return yymmdd2;
    } else if (!strcmp(fmt,"MMDDYY")) {
        if (!tnum)
            return mmddyy1;
        else
            return mmddyy2;
    } else if (!strcmp(fmt,"RAW")) {
        if (!tnum)
            return ts1;
        else
            return ts2;
    } else if (!strcmp(fmt,"FREE")) {
        if (!tnum)
            return free1;
        else
            return free2;
    } else {
        fprintf(stderr, "\n'%s' not YYMMDD,MMDDYY,RAW or FREE\n", fmt); 
        exit(1);    
    }
    
    return "0000-00-00";
}

void Utils::TimeAdd(int tnum, int units, char *timepart)
{
    // 
    // Must fix for Daylight Savings
    // Following is 0.5 hour increments TimeAdd(tnum, 30, Min)
    // When tm_isdst switches from 1 - 0 in Nov...
    // ----------------------------------------
    // 2011-11-06 00:38:38.310.1.Sunday
    // 2011-11-06 01:08:38.310.1.Sunday
    // 2011-11-06 01:38:38.310.1.Sunday
    // 2011-11-06 01:08:38.310.0.Sunday Fall back an hour
    // 2011-11-06 01:38:38.310.0.Sunday Fall back hour
    // 2011-11-06 02:08:38.310.0.Sunday and so on
    // When tm_isdst switches from 0 - 1 in Mar...
    // ----------------------------------------
    // 2012-03-11 00:38:38.071.0.Sunday
    // 2012-03-11 01:08:38.071.0.Sunday
    // 2012-03-11 01:38:38.071.0.Sunday
    // 2012-03-11 03:08:38.071.1.Sunday Spring an hour
    // 2012-03-11 03:38:38.071.1.Sunday and so on
    // ----------------------------------------
    // /home/sctldev/visioncfe_driverfiles/src/timetest.cpp
    //

    int mul, prev_tm_isdst;
    switch (toupper(*timepart)) {
        case 'D' : mul = units * 60 * 60 * 24; break;
        case 'H' : mul = units * 60 * 60; break;
        case 'M' : mul = units * 60; break;
        case 'S' : mul = units * 1; break;
        default: fprintf(stderr, "\n'%s' is not DAY/DAT,HOUR,MINUTE or SECOND\n",
                                                                timepart); 
                 exit(1);
    } 
    if (!tnum) {
        prev_tm_isdst = tm1->tm_isdst;
        t1 += mul;
        tm1 = localtime(&t1);
        TimeFormat(0);
    } else {
        prev_tm_isdst = tm1->tm_isdst;
        t2 += mul;
        tm2 = localtime(&t2);
        TimeFormat(1);
    }
    return;
}

time_t Utils::TimeDiff(char *timepart)
{
    int div;
    switch (toupper(*timepart)) {
        case 'D' : div = 60 * 60 * 24; break;
        case 'H' : div = 60 * 60; break;
        case 'M' : div = 60; break;
        case 'S' : div = 1; break;
        default: fprintf(stderr, "\n'%s' is not DAY/DAT,HOUR,MINUTE or SECOND\n",
                                                                timepart); 
                 exit(1);
    } 

    return (t2 - t1) / div;
}

char * Utils::Upper(char *str) 
{
    strcpy(dsstylebuf, str);
    for (char *p=dsstylebuf; *p; p++) 
        *p = toupper(*p);
    return dsstylebuf; 
}

char * Utils::Lower(char *str) 
{
    strcpy(dsstylebuf, str);
    for (char *p=dsstylebuf; *p; p++) 
        *p = tolower(*p);
    return dsstylebuf; 
}

int Utils::LastDayOfMonth(int year, int month)
{
    char y[BUFSIZ], m[BUFSIZ];
    sprintf(y, "%04d", year);
    sprintf(m, "%02d", month);
    TimeSet(0, y, m, "28", "10", "25", "45");
    while (true) {
        TimeAdd(0, 1, "D");
        if (TimePart(0, "MONTH") != month) {
            TimeAdd(0, -1, "D");
            break;
        }    
    }
    return TimePart(0, "DATE");
}

int Utils::TimePart(int tnum, char *timepart)
{
    // tm_sec seconds after the minute 0-61* 
    // tm_min minutes after the hour 0-59 
    // tm_hour hours since midnight 0-23 
    // tm_mday day of the month 1-31 
    // tm_mon months since January 0-11 
    // tm_year years since 1900  
    // tm_wday days since Sunday 0-6 
    // tm_yday days since January 1 0-365 
    // tm_isdst Daylight Saving Time flag (1 When DST is on, 0 otherwise)
    char *tp = strdup(Upper(timepart));
    if (!tnum) {
        if (!strcmp(tp, "YEAR"))    return tm1->tm_year+1900;
        if (!strcmp(tp, "MONTH"))   return tm1->tm_mon+1;
        if (!strcmp(tp, "DATE"))    return tm1->tm_mday;
        if (!strcmp(tp, "HOUR"))    return tm1->tm_hour;
        if (!strcmp(tp, "MINUTE"))  return tm1->tm_min;
        if (!strcmp(tp, "SECOND"))  return tm1->tm_sec;
        if (!strcmp(tp, "WEEKDAY")) return tm1->tm_wday+1;
        if (!strcmp(tp, "YEARDAY")) return tm1->tm_yday+1;
        if (!strcmp(tp, "ISDST"))   return tm1->tm_isdst;
        fprintf(stderr, "\n'%s' Not YEAR,MONTH,DATE,HOUR,MINUTE,", tp); 
        fprintf(stderr, "SECOND,WEEKDAY,YEARDAY or ISDT\n\n"); 
        exit(1);
    } else {
        if (!strcmp(tp, "YEAR"))    return tm2->tm_year+1900;
        if (!strcmp(tp, "MONTH"))   return tm2->tm_mon+1;
        if (!strcmp(tp, "DATE"))    return tm2->tm_mday;
        if (!strcmp(tp, "HOUR"))    return tm2->tm_hour;
        if (!strcmp(tp, "MINUTE"))  return tm2->tm_min;
        if (!strcmp(tp, "SECOND"))  return tm2->tm_sec;
        if (!strcmp(tp, "WEEKDAY")) return tm2->tm_wday+1;
        if (!strcmp(tp, "YEARDAY")) return tm2->tm_yday+1;
        if (!strcmp(tp, "ISDST"))   return tm2->tm_isdst;
        fprintf(stderr, "\n'%s' Not YEAR,MONTH,DATE,HOUR,MINUTE,", tp); 
        fprintf(stderr, "SECOND,WEEKDAY,YEARDAY or ISDT\n\n"); 
        exit(1);
    }
    free (tp);
    return -1; // Control never reaches this point
}

void Utils::TimeFormat(int tnum)
{
    // tm_yday days since January 1 0-365 
    // tm_isdst Daylight Saving Time flag 
    char * weekday[] = { "Sunday", "Monday", "Tuesday", "Wednesday",
                                                  "Thursday", "Friday", "Saturday"};    
    if (!tnum) {
        sprintf(weekday1, "%s", weekday[tm1->tm_wday]);
        sprintf(ts1, "%04d%02d%02d%02d%02d%02d%03d%d%s", 
            tm1->tm_year+1900,tm1->tm_mon+1,tm1->tm_mday,tm1->tm_hour,
                  tm1->tm_min,tm1->tm_sec,tm1->tm_yday+1,tm1->tm_isdst,weekday1);
        sprintf(yymmdd1, "%04d-%02d-%02d %02d:%02d:%02d.%03d.%d.%s", 
            tm1->tm_year+1900,tm1->tm_mon+1,tm1->tm_mday,tm1->tm_hour,
                  tm1->tm_min,tm1->tm_sec,tm1->tm_yday+1,tm1->tm_isdst,weekday1);
        sprintf(mmddyy1, "%02d/%02d/%04d %02d:%02d:%02d:%03d.%d.%s", 
            tm1->tm_mon+1,tm1->tm_mday,tm1->tm_year+1900,tm1->tm_hour,
                  tm1->tm_min,tm1->tm_sec,tm1->tm_yday+1,tm1->tm_isdst,weekday1);
        sprintf(free1, "%s", asctime(tm1));
        if (strchr(free1, '\n'))
            *(strchr(free1, '\n')) = '\0';
    } else {
        sprintf(weekday2, "%s", weekday[tm2->tm_wday]);
        sprintf(ts2, "%04d%02d%02d%02d%02d%02d%03d%d%s", 
            tm2->tm_year+1900,tm2->tm_mon+1,tm2->tm_mday,tm2->tm_hour,
                  tm2->tm_min,tm2->tm_sec,tm2->tm_yday+1,tm2->tm_isdst,weekday2);
        sprintf(yymmdd2, "%04d-%02d-%02d %02d:%02d:%02d.%03d.%d.%s", 
            tm2->tm_year+1900,tm2->tm_mon+1,tm2->tm_mday,tm2->tm_hour,
                  tm2->tm_min,tm2->tm_sec,tm2->tm_yday+1,tm2->tm_isdst,weekday2);
        sprintf(mmddyy2, "%02d/%02d/%04d %02d:%02d:%02d:%03d.%d.%s", 
            tm2->tm_mon+1,tm2->tm_mday,tm2->tm_year+1900,tm2->tm_hour,
                  tm2->tm_min,tm2->tm_sec,tm2->tm_yday+1,tm2->tm_isdst,weekday2);
        sprintf(free2, "%s", asctime(tm2));
        if (strchr(free2, '\n'))
            *(strchr(free2, '\n')) = '\0';
    }

    return;
}



