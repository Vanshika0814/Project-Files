 /******************************************************************************************
    PURPOSE : Table created for loading all the CVR errors
    Change History																	    
    Developer        Date         Version     Comments and changes made				    
    -------------   ------       ----------  --------------------------------------------
    Satya Pavani       25-06-2025      1.0         Initial Development
********************************************************************************************/

Create table xxcnv.xxcnv_gl_cvr_violation_tbl
(
SEGMENT3              VARCHAR2(25)   
,SEGMENT4              VARCHAR2(25)   
,TARGET_SEGMENT3       VARCHAR2(25)   
,TARGET_SEGMENT4       VARCHAR2(25)   
,ERROR_MESSAGE         VARCHAR2(1000) 
,SEGMENT1              VARCHAR2(25)   
,PERIOD_NAME           VARCHAR2(15)   
,FILE_NAME             VARCHAR2(4000) 
,STATUS                VARCHAR2(50)   
,ERROR_DATE            DATE           
,SEGMENT5              VARCHAR2(25)   
,TARGET_SEGMENT5       VARCHAR2(25)   
);
