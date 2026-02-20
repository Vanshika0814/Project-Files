CREATE TABLE xxcnv.xxcnv_pdh_c027_egp_item_categories_stg
(
TRANSACTION_TYPE                VARCHAR2(10)   
,BATCH_ID                        NUMBER(18)     
,BATCH_NUMBER                    VARCHAR2(40)   
,ITEM_NUMBER                     VARCHAR2(300)  
,ORGANIZATION_CODE               VARCHAR2(18)   
,CATEGORY_SET_NAME               VARCHAR2(30)   
,CATEGORY_NAME                   VARCHAR2(250)  
,CATEGORY_CODE                   VARCHAR2(820)  
,OLD_CATEGORY_NAME               VARCHAR2(250)  
,OLD_CATEGORY_CODE               VARCHAR2(820)  
,SOURCE_SYSTEM_CODE              VARCHAR2(30)   
,SOURCE_SYSTEM_REFERENCE         VARCHAR2(255)  
,START_DATE                      DATE           
,END_DATE                        DATE           
,FILE_NAME                       VARCHAR2(4000) 
,IMPORT_STATUS                   VARCHAR2(4000) 
,ERROR_MESSAGE                   VARCHAR2(4000) 
,FILE_REFERENCE_IDENTIFIER       VARCHAR2(4000) 
,SOURCE_SYSTEM                   VARCHAR2(4000) 
,EXECUTION_ID                    VARCHAR2(4000)
);