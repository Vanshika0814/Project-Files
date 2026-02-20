create table xxcnv.xxcnv_cmn_conversion_execution
(
EXECUTION_ID           VARCHAR2(100)  
,CONVERSION_ID          VARCHAR2(20)   
,BOUNDARY_SYSTEM        VARCHAR2(256)  
,FILE_PATH              VARCHAR2(256)  
,FILE_NAME              VARCHAR2(2000) 
,FILE_TIMESTAMP         VARCHAR2(20)   
,START_TIMESTAMP        TIMESTAMP(6)   
,END_TIMESTAMP          TIMESTAMP(6)   
,STATUS                 VARCHAR2(50)   
,LAST_UPDATE_DATE       TIMESTAMP(9)   
,LAST_UPDATED_BY        VARCHAR2(50)   
,CREATION_DATE          TIMESTAMP(9)   
,CREATED_BY             VARCHAR2(50) 
);