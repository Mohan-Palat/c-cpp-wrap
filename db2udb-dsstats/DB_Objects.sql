db2 describe table AUDIT.T_DS_JOB_DESC                                                                                                                   
db2 describe table AUDIT.T_DS_LNK_DESC                                                                                                                   
db2 describe table AUDIT.T_DS_LOADSTATS_JOB                                                                                                              
db2 describe table AUDIT.T_DS_LOADSTATS_LNK                                                                                                              
db2 describe table AUDIT.T_DS_LOADSTATS_STG                                                                                                              
db2 describe table AUDIT.T_DS_STG_DESC          


                                Data type                     Column
Column name                     schema    Data type name      Length     Scale Nulls
------------------------------- --------- ------------------- ---------- ----- ------
PROJ_NM                         SYSIBM    CHARACTER                   75     0 No    
JOB_NM                          SYSIBM    CHARACTER                   75     0 No    
JOB_DESC                        SYSIBM    VARCHAR                    350     0 No    
LAST_ACTY_OPER_ID               SYSIBM    CHARACTER                   15     0 No    
LAST_ACTY_DTM                   SYSIBM    TIMESTAMP                   10     6 No    

  5 record(s) selected.

clretlqa@p1ehowld403:/home/clretlqa>                                                                                  <

                                Data type                     Column
Column name                     schema    Data type name      Length     Scale Nulls
------------------------------- --------- ------------------- ---------- ----- ------
PROJ_NM                         SYSIBM    CHARACTER                   75     0 No    
JOB_NM                          SYSIBM    CHARACTER                   75     0 No    
STG_NM                          SYSIBM    CHARACTER                   75     0 No    
LNK_NM                          SYSIBM    CHARACTER                   75     0 No    
LNK_DESC                        SYSIBM    VARCHAR                    350     0 No    
LAST_ACTY_OPER_ID               SYSIBM    CHARACTER                   15     0 No    
LAST_ACTY_DTM                   SYSIBM    TIMESTAMP                   10     6 No    

  7 record(s) selected.

clretlqa@p1ehowld403:/home/clretlqa>                                                                                  <

                                Data type                     Column
Column name                     schema    Data type name      Length     Scale Nulls
------------------------------- --------- ------------------- ---------- ----- ------
LOAD_ID                         SYSIBM    BIGINT                       8     0 No    
JOB_PROJ_NM                     SYSIBM    CHARACTER                   75     0 No    
JOB_NM                          SYSIBM    CHARACTER                   75     0 No    
LAST_ACTY_OPER_ID               SYSIBM    CHARACTER                   15     0 No    
LAST_ACTY_DTM                   SYSIBM    TIMESTAMP                   10     6 No    

  5 record(s) selected.

clretlqa@p1ehowld403:/home/clretlqa>                                                                                  <

                                Data type                     Column
Column name                     schema    Data type name      Length     Scale Nulls
------------------------------- --------- ------------------- ---------- ----- ------
LOAD_ID                         SYSIBM    BIGINT                       8     0 No    
LNK_PROJ_NM                     SYSIBM    CHARACTER                   75     0 No    
LNK_JOB_NM                      SYSIBM    CHARACTER                   75     0 No    
LNK_STG_NM                      SYSIBM    CHARACTER                   75     0 No    
LNK_NM                          SYSIBM    CHARACTER                   75     0 No    
LNK_SEQ                         SYSIBM    INTEGER                      4     0 No    
LNK_LINKED_STG                  SYSIBM    CHARACTER                   75     0 Yes   
LNK_ROWCOUNT                    SYSIBM    INTEGER                      4     0 Yes   
LAST_ACTY_OPER_ID               SYSIBM    CHARACTER                   15     0 No    
LAST_ACTY_DTM                   SYSIBM    TIMESTAMP                   10     6 No    

  10 record(s) selected.

clretlqa@p1ehowld403:/home/clretlqa>                                                                                  <

                                Data type                     Column
Column name                     schema    Data type name      Length     Scale Nulls
------------------------------- --------- ------------------- ---------- ----- ------
LOAD_ID                         SYSIBM    BIGINT                       8     0 No    
STG_PROJ_NM                     SYSIBM    CHARACTER                   75     0 No    
STG_JOB_NM                      SYSIBM    CHARACTER                   75     0 No    
STG_NM                          SYSIBM    CHARACTER                   75     0 No    
STG_SEQ                         SYSIBM    INTEGER                      4     0 No    
STG_START_DTM                   SYSIBM    TIMESTAMP                   10     6 No    
STG_END_DTM                     SYSIBM    TIMESTAMP                   10     6 Yes   
STG_TYPE                        SYSIBM    CHARACTER                   75     0 No    
STG_STATUS                      SYSIBM    INTEGER                      4     0 Yes   
STG_ODBC_INS                    SYSIBM    INTEGER                      4     0 Yes   
STG_ODBC_UPD                    SYSIBM    INTEGER                      4     0 Yes   
STG_ODBC_DEL                    SYSIBM    INTEGER                      4     0 Yes   
STG_ODBC_REJ                    SYSIBM    INTEGER                      4     0 Yes   
LAST_ACTY_OPER_ID               SYSIBM    CHARACTER                   15     0 No    
LAST_ACTY_DTM                   SYSIBM    TIMESTAMP                   10     6 No    

  15 record(s) selected.

clretlqa@p1ehowld403:/home/clretlqa>db2 describe table AUDIT.T_DS_STG_DESC 

                                Data type                     Column
Column name                     schema    Data type name      Length     Scale Nulls
------------------------------- --------- ------------------- ---------- ----- ------
PROJ_NM                         SYSIBM    CHARACTER                   75     0 No    
JOB_NM                          SYSIBM    CHARACTER                   75     0 No    
STG_NM                          SYSIBM    CHARACTER                   75     0 No    
STG_DESC                        SYSIBM    VARCHAR                    350     0 No    
LAST_ACTY_OPER_ID               SYSIBM    CHARACTER                   15     0 No    
LAST_ACTY_DTM                   SYSIBM    TIMESTAMP                   10     6 No    

  6 record(s) selected.

CREATE VIEW AUDIT.V_DS_LOADSTATS AS
    SELECT J.LOAD_ID,
           J.JOB_PROJ_NM,
           J.JOB_NM,
           JD.JOB_DESC,
           S.STG_NM,
           SD.STG_DESC,
           S.STG_SEQ,
           S.STG_START_DTM,
           S.STG_END_DTM,
           S.STG_TYPE,
           S.STG_STATUS,
           S.STG_ODBC_INS,
           S.STG_ODBC_UPD,
           S.STG_ODBC_DEL,
           S.STG_ODBC_REJ,
           L.LNK_NM,
           LD.LNK_DESC,
           L.LNK_SEQ,
           L.LNK_LINKED_STG,
           L.LNK_ROWCOUNT,
           J.LAST_ACTY_OPER_ID,
           J.LAST_ACTY_DTM
      FROM AUDIT.T_DS_LOADSTATS_JOB J
      LEFT OUTER JOIN AUDIT.T_DS_LOADSTATS_STG S
        ON S.LOAD_ID = J.LOAD_ID
       AND S.STG_PROJ_NM = J.JOB_PROJ_NM
       AND S.STG_JOB_NM = J.JOB_NM
      LEFT OUTER JOIN AUDIT.T_DS_LOADSTATS_LNK L
        ON L.LOAD_ID = S.LOAD_ID
       AND L.LNK_PROJ_NM = S.STG_PROJ_NM
       AND L.LNK_JOB_NM = S.STG_JOB_NM
       AND L.LNK_STG_NM = S.STG_NM
      LEFT OUTER JOIN AUDIT.T_DS_JOB_DESC JD
        ON JD.PROJ_NM = J.JOB_PROJ_NM
       AND JD.JOB_NM = J.JOB_NM
      LEFT OUTER JOIN AUDIT.T_DS_STG_DESC SD
        ON SD.PROJ_NM = S.STG_PROJ_NM
       AND SD.JOB_NM = S.STG_JOB_NM
       AND SD.STG_NM = S.STG_NM
      LEFT OUTER JOIN AUDIT.T_DS_LNK_DESC LD
        ON LD.PROJ_NM = L.LNK_PROJ_NM
       AND LD.JOB_NM = L.LNK_JOB_NM
       AND LD.STG_NM = L.LNK_STG_NM
       AND LD.LNK_NM = L.LNK_NM
 

