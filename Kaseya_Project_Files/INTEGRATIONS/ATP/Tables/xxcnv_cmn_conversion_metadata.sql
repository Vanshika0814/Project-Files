create table xxcnv.xxcnv_cmn_conversion_metadata
(
ID                        VARCHAR2(30)  
,TYPE                      VARCHAR2(20)  
,DESCRIPTION               VARCHAR2(256) 
,PROCESS_AREA              VARCHAR2(20)  
,MODULE                    VARCHAR2(20)  
,BOUNDARY_SYSTEM           VARCHAR2(256) 
,DATA_OBJECT               VARCHAR2(50)  
,IT_DL_NAME                VARCHAR2(256) 
,BIZ_DL_NAME               VARCHAR2(256) 
,PRIORITY                  VARCHAR2(20)  
,FREQUENCY_MODE            VARCHAR2(20)  
,TRANSFORMER_TYPE          VARCHAR2(20)  
,TRANSFORMER_ROUTINE       VARCHAR2(256) 
,STOP_PROCESSING           VARCHAR2(20)  
,FILE_SERVER_PATH          VARCHAR2(256) 
,OBJ_STORAGE_PATH          VARCHAR2(256) 
,LAST_UPDATE_DATE          DATE          
,LAST_UPDATED_BY           VARCHAR2(30)  
,CREATION_DATE             DATE          
,CREATED_BY                VARCHAR2(30)  
,SOURCE_COA                VARCHAR2(100) 
,TARGET_COA                VARCHAR2(100)  
);