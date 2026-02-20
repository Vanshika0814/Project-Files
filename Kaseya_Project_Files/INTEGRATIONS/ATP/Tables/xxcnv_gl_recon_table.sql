DROP TABLE xxcnv.xxcnv_gl_recon_table;

CREATE TABLE xxcnv.xxcnv_gl_recon_table
(
TARGET_SEGMENT1          VARCHAR2(50)  
,FILE_TYPE                VARCHAR2(50)  
,LEDGER_NAME              VARCHAR2(100) 
,PERIOD_NAME              VARCHAR2(50)  
,FILE_COUNT               NUMBER        
,TOTAL_ENTERED_DR         NUMBER        
,TOTAL_ENTERED_CR         NUMBER        
,TOTAL_ACCOUNTED_DR       NUMBER        
,TOTAL_ACCOUNTED_CR       NUMBER        
,PROCESSED_DATE           VARCHAR2(50)  
,JOURNAL_NAME             VARCHAR2(100)
);

GRANT select,update,delete on xxcnv.xxcnv_gl_recon_table to xxcnv_run;
