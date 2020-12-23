# @REM Usage: DS_LoadStats.exe <1. DS Project Name> <2. DS Job Name> <3. DB Connection Property File> <4. Log Folder> <5. Switch: LOG_OVERWRITE>
# # @REM E:\DS_LoadStats\Bin\DS_LoadStats.exe EDW EDW_10100_OMNI_PSTG_IVR_PLN1 E:\DS_LoadStats\Prop\DS_LoadStats.dbctrl E:\DS_LoadStats\Log LOG_OVERWRITE
# # @REM E:\DS_LoadStats\Bin\DS_LoadStatsMs.exe EDW EDW_10100_OMNI_PSTG_IVR_PLN1 E:\DS_LoadStats\Prop\DS_LoadStatsMs.dbctrl E:\DS_LoadStats\Log LOG_OVERWRITE
# # @REM DS_LoadStats.exe EDW ITEST
# # @REM E:\DS_LoadStats\Bin\DS_LoadStats.exe EDW ITEST E:\DS_LoadStats\Prop\DS_LoadStats.dbctrl E:\DS_LoadStats\Log LOG_APPEND
# # @REM E:\DS_LoadStats\Bin\DS_LoadStatsMs.exe CRM DSAPI_UPSERT_LOG E:\DS_LoadStats\Prop\DS_LoadStatsMs.dbctrl E:\DS_LoadStats\Log LOG_OVERWRITE
# # 
# # E:\DS_LoadStats\Bin\DS_LoadStatsMs.exe EDW DSAPI_UPSERT_LOG E:\DS_LoadStats\Prop\DS_LoadStatsMs.dbctrl E:\DS_LoadStats\Log LOG_OVERWRITE
# # E:\DS_LoadStats\Bin\DS_LoadStatsMs.exe CRM DSAPI_UPSERT_LOG E:\DS_LoadStats\Prop\DS_LoadStatsMs.dbctrl E:\DS_LoadStats\Log LOG_OVERWRITE
# # 
# # E:\DS_LoadStats\Bin\DS_LoadStats.exe EDW DSAPI_UPSERT_LOG_DB2 E:\DS_LoadStats\Prop\DS_LoadStats.dbctrl E:\DS_LoadStats\Log LOG_APPEND
# # E:\DS_LoadStats\Bin\DS_LoadStats.exe CRM DSAPI_UPSERT_LOG E:\DS_LoadStats\Prop\DS_LoadStats.dbctrl E:\DS_LoadStats\Log LOG_APPEND
#
# # ../DS_Job_Statistics EDW EDW_10100_OMNI_PSTG_IVR_PLN1 ../Prop/DS_LoadStats.dbctrl ../Log LOG_OVERWRITE
#
#
# # ../DS_Job_Statistics INVODS IODS_40100_IMDM_MASTER_INV_KEY_MAPPINGS ../Prop/DS_LoadStats.dbctrl ../Log LOG_OVERWRITE
# # ../DS_Job_Statistics INVODS INVODS_20200_IODS_PASSPORT_SEQ_FILE     ../Prop/DS_LoadStats.dbctrl ../Log LOG_OVERWRITE
#
# # ../DS_Job_Statistics INVODS MO_IODS_10200_PACRS_DB_PLAN_FUND_LD ../Prop/DS_LoadStats.dbctrl ../Log LOG_APPEND
#
# # [?8/?13/?2018 10:23 AM]  John Palatianos:  
# # /usr/local/dstage/EDW/Batch/DS_LoadStats/DS_Job_Statistics EDW EDW_10100_OMNI_PSTG_IVR_PLN1.VIS /usr/local/dstage/EDW/Batch/DS_LoadStats/Prop/DS_LoadStats.dbctrl /usr/local/dstage/EDW/Batch/DS_LoadStats/Log LOG_APPEND 

# ../DS_Job_Statistics EDW EDW_10100_OMNI_PSTG_IVR_PLN1.VIS ../Prop/DS_LoadStats.dbctrl ../Log LOG_APPEND
../DS_Job_Statistics INVODS MO_SPL ../Prop/DS_LoadStats.dbctrl ../Log LOG_APPEND

# ../DS_Job_Statistics CRM Extract_Security_Object ../Prop/DS_LoadStats.dbctrl ../Log LOG_APPEND


