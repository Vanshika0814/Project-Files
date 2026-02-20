create table xxcnv.xxcnv_cmn_conversion_execution_details
(
EXECUTION_ID           VARCHAR2(100)  
,EXECUTION_STEP         VARCHAR2(100)  
,START_TIMESTAMP        TIMESTAMP(9)   
,END_TIMESTAMP          TIMESTAMP(9)   
,PROCESS_REF            VARCHAR2(256)  
,FILE_PATH              VARCHAR2(256)  
,FILE_NAME              VARCHAR2(2000) 
,FILE_TIMESTAMP         VARCHAR2(20)   
,LAST_UPDATE_DATE       TIMESTAMP(9)   
,LAST_UPDATED_BY        VARCHAR2(50)   
,CREATION_DATE          TIMESTAMP(9)   
,CREATED_BY             VARCHAR2(50)   
,ATTRIBUTE1             VARCHAR2(30)   
,ATTRIBUTE2             VARCHAR2(500)  
);