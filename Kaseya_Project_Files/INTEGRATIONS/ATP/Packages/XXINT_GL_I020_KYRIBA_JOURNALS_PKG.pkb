create or replace PACKAGE BODY XXINT.XXINT_GL_I020_KYRIBA_JOURNALS_PKG IS
/********************************************************************************************
OBJECT NAME: Kyriba GL Journals Package
DESCRIPTION: Package specification for GL_I020
Version 	Name              	Date           		Version-Description
---------------------------------------------------------------------------
<1.0>		Kunjesh Kumar Singh  			05-JUNE-2025 	    1.0-Initial Draft
**********************************************************************************************/


/************************************************************************************************|
	OBJECT NAME: Bulk Insert Procedure                                                           |
	DESCRIPTION: Procedure to Bulk insert into ATP Table XXINT.XXINT_GL_I020_KYRIBA_JOURNALS_STG | 
	Version 	Name              	Date           		Version-Description                      |
-------------------------------------------------------------------------------------------------|
	<1.0>		Kunjesh Kumar Singh     			05-JUNE-2025  	    1.0- Initial Draft       |
*************************************************************************************************|
*/
PROCEDURE bulk_insert_stg_prc (
p_current_instance_id  IN   VARCHAR2,
p_parent_instance_id   IN   VARCHAR2,
p_interface_rice_id    IN   VARCHAR2,
p_interface_rice_name  IN   VARCHAR2,
p_log_flag             IN   VARCHAR2,
p_db_object_name       IN   VARCHAR2,
p_message              IN   VARCHAR2,
p_gl_interface_rec     IN   gl_interface_tbl_type,
p_status               OUT  VARCHAR2,
p_error_msg            OUT  VARCHAR2
)

IS 

 BEGIN
--logging started
XXINT_XX_I010_COMMON_LOGGING_PKG.DB_LOGGING_DETAILS_PRC (     
	    p_log_flag   ,        
        p_interface_rice_id,  
        p_interface_rice_name,   
        p_parent_instance_id,  
        p_current_instance_id, 
        'xxint_gl_i020_kyriba_journals_pkg.bulk_insert_stg_prc',     
        'Procedure Execution Started at'||systimestamp ,           
		null,         
		null,           
		null,           
		null,           
		null         

    );


--Inserting Kyriba Journals to STG
        FORALL i IN p_gl_interface_rec.first..p_gl_interface_rec.last
            INSERT INTO XXINT.XXINT_GL_I020_KYRIBA_JOURNALS_STG (
LEDGER_ID                                   ,
STATUS_CODE                                 ,
ACCOUNTING_DATE                             ,
USER_JE_SOURCE_NAME                         ,
USER_JE_CATEGORY_NAME                       ,
CURRENCY_CODE                               ,
DATE_CREATED                                ,
ACTUAL_FLAG                                 ,
SEGMENT1                                    ,
SEGMENT2                                    ,
SEGMENT3                                    ,
SEGMENT4                                    ,
SEGMENT5                                    ,
SEGMENT6                                    ,
SEGMENT7                                    ,
SEGMENT8                                    ,
SEGMENT9                                    ,
SEGMENT10                                   ,
SEGMENT11                                   ,
SEGMENT12                                   ,
SEGMENT13                                   ,
SEGMENT14                                   ,
SEGMENT15                                   ,
SEGMENT16                                   ,
SEGMENT17                                   ,
SEGMENT18                                   ,
SEGMENT19                                   ,
SEGMENT20                                   ,
SEGMENT21                                   ,
SEGMENT22                                   ,
SEGMENT23                                   ,
SEGMENT24                                   ,
SEGMENT25                                   ,
SEGMENT26                                   ,
SEGMENT27                                   ,
SEGMENT28                                   ,
SEGMENT29                                   ,
SEGMENT30                                   ,
ENTERED_DR                                  ,
ENTERED_CR                                  ,
ACCOUNTED_DR                                ,
ACCOUNTED_CR                                ,
REFERENCE1                                  ,
REFERENCE2                                  ,
REFERENCE3                                  ,
REFERENCE4                                  ,
REFERENCE5                                  ,
REFERENCE6                                  ,
REFERENCE7                                  ,
REFERENCE8                                  ,
REFERENCE9                                  ,
REFERENCE10                                 ,
REFERENCE21                                 ,
REFERENCE22                                 ,
REFERENCE23                                 ,
REFERENCE24                                 ,
REFERENCE25                                 ,
REFERENCE26                                 ,
REFERENCE27                                 ,
REFERENCE28                                 ,
REFERENCE29                                 ,
REFERENCE30                                 ,
STAT_AMOUNT                                 ,
USER_CURRENCY_CONVERSION_TYPE               ,
CURRENCY_CONVERSION_DATE                    ,
CURRENCY_CONVERSION_RATE                    ,
GROUP_ID                                    ,
ATTRIBUTE_CATEGORY                          ,
ATTRIBUTE1                                  ,
ATTRIBUTE2                                  ,
ATTRIBUTE3                                  ,
ATTRIBUTE4                                  ,
ATTRIBUTE5                                  ,
ATTRIBUTE6                                  ,
ATTRIBUTE7                                  ,
ATTRIBUTE8                                  ,
ATTRIBUTE9                                  ,
ATTRIBUTE10                                 ,
ATTRIBUTE11                                 ,
ATTRIBUTE12                                 ,
ATTRIBUTE13                                 ,
ATTRIBUTE14                                 ,
ATTRIBUTE15                                 ,
ATTRIBUTE16                                 ,
ATTRIBUTE17                                 ,
ATTRIBUTE18                                 ,
ATTRIBUTE19                                 ,
ATTRIBUTE20                                 ,
ATTRIBUTE_CATEGORY3                         ,
AVERAGE_JOURNAL_FLAG                        ,
ORIGINATING_BAL_SEG_VALUE                   ,
LEDGER_NAME                                 ,
ENCUMBRANCE_TYPE_ID                         ,
JGZZ_RECON_REF                              ,
PERIOD_NAME                                 ,
REFERENCE18                                 ,
REFERENCE19                                 ,
REFERENCE20                                 ,
ATTRIBUTE_DATE1                             ,
ATTRIBUTE_DATE2                             ,
ATTRIBUTE_DATE3                             ,
ATTRIBUTE_DATE4                             ,
ATTRIBUTE_DATE5                             ,
ATTRIBUTE_DATE6                             ,
ATTRIBUTE_DATE7                             ,
ATTRIBUTE_DATE8                             ,
ATTRIBUTE_DATE9                             ,
ATTRIBUTE_DATE10                            ,
ATTRIBUTE_NUMBER1                           ,
ATTRIBUTE_NUMBER2                           ,
ATTRIBUTE_NUMBER3                           ,
ATTRIBUTE_NUMBER4                           ,
ATTRIBUTE_NUMBER5                           ,
ATTRIBUTE_NUMBER6                           ,
ATTRIBUTE_NUMBER7                           ,
ATTRIBUTE_NUMBER8                           ,
ATTRIBUTE_NUMBER9                           ,
ATTRIBUTE_NUMBER10                          ,
GLOBAL_ATTRIBUTE_CATEGORY                   ,
GLOBAL_ATTRIBUTE1                           ,
GLOBAL_ATTRIBUTE2                           ,
GLOBAL_ATTRIBUTE3                           ,
GLOBAL_ATTRIBUTE4                           ,
GLOBAL_ATTRIBUTE5                           ,
GLOBAL_ATTRIBUTE6                           ,
GLOBAL_ATTRIBUTE7                           ,
GLOBAL_ATTRIBUTE8                           ,
GLOBAL_ATTRIBUTE9                           ,
GLOBAL_ATTRIBUTE10                          ,
GLOBAL_ATTRIBUTE11                          ,
GLOBAL_ATTRIBUTE12                          ,
GLOBAL_ATTRIBUTE13                          ,
GLOBAL_ATTRIBUTE14                          ,
GLOBAL_ATTRIBUTE15                          ,
GLOBAL_ATTRIBUTE16                          ,
GLOBAL_ATTRIBUTE17                          ,
GLOBAL_ATTRIBUTE18                          ,
GLOBAL_ATTRIBUTE19                          ,
GLOBAL_ATTRIBUTE20                          ,
GLOBAL_ATTRIBUTE_DATE1                      ,
GLOBAL_ATTRIBUTE_DATE2                      ,
GLOBAL_ATTRIBUTE_DATE3                      ,
GLOBAL_ATTRIBUTE_DATE4                      ,
GLOBAL_ATTRIBUTE_DATE5                      ,
GLOBAL_ATTRIBUTE_NUMBER1                    ,
GLOBAL_ATTRIBUTE_NUMBER2                    ,
GLOBAL_ATTRIBUTE_NUMBER3                    ,
GLOBAL_ATTRIBUTE_NUMBER4                    ,
GLOBAL_ATTRIBUTE_NUMBER5                    ,
SOURCE_FILE_NAME                            ,
FILE_REFERENCE_IDENTIFIER                   ,
SOURCE_SYSTEM                               ,
LEDGER_CURRENCY                             ,
CREATED_BY                                  ,
CREATION_DATE                               ,
LAST_UPDATED_BY                             ,
LAST_UPDATED_DATE                           ,
CURRENT_INSTANCE_ID                         ,
PARENT_INSTANCE_ID                          ,
INTERFACE_RICE_ID                           ,
INTERFACE_RICE_NAME                         ,
STATUS
)

VALUES

(
p_gl_interface_rec(i).LEDGER_ID                                   ,                            
p_gl_interface_rec(i).STATUS_CODE                                 ,                            
p_gl_interface_rec(i).EFFECTIVE_TRSN_DATE                         ,                  
p_gl_interface_rec(i).JOURNAL_SOURCE                              ,                       
p_gl_interface_rec(i).JOURNAL_CATEGORY                            ,                     
p_gl_interface_rec(i).CURRENCY_CODE                               ,                        
p_gl_interface_rec(i).JOURNAL_EN_CRN_DATE                         ,                  
p_gl_interface_rec(i).ACTUAL_FLAG                                 ,                          
p_gl_interface_rec(i).SEGMENT1                                    ,  
NVL(p_gl_interface_rec(i).SEGMENT2,'999')                       ,  
NVL(p_gl_interface_rec(i).SEGMENT3,'99999')                       ,                          
p_gl_interface_rec(i).SEGMENT4                                    ,                          
NVL(p_gl_interface_rec(i).SEGMENT5,'9999')                        ,                          
NVL(p_gl_interface_rec(i).SEGMENT6,'999999')                        ,   
NVL(p_gl_interface_rec(i).SEGMENT7,'9999')                        ,
NVL(p_gl_interface_rec(i).SEGMENT8,'9999')                        ,                          
NVL(p_gl_interface_rec(i).SEGMENT9,'9999')                        ,                          
NVL(p_gl_interface_rec(i).SEGMENT10,'999999')                        ,                          
p_gl_interface_rec(i).SEGMENT11                                   ,                          
p_gl_interface_rec(i).SEGMENT12                                   ,                          
p_gl_interface_rec(i).SEGMENT13                                   ,                          
p_gl_interface_rec(i).SEGMENT14                                   ,                          
p_gl_interface_rec(i).SEGMENT15                                   ,                          
p_gl_interface_rec(i).SEGMENT16                                   ,                          
p_gl_interface_rec(i).SEGMENT17                                   ,                          
p_gl_interface_rec(i).SEGMENT18                                   ,                          
p_gl_interface_rec(i).SEGMENT19                                   ,                          
p_gl_interface_rec(i).SEGMENT20                                   ,                          
p_gl_interface_rec(i).SEGMENT21                                   ,                          
p_gl_interface_rec(i).SEGMENT22                                   ,                          
p_gl_interface_rec(i).SEGMENT23                                   ,                          
p_gl_interface_rec(i).SEGMENT24                                   ,                          
p_gl_interface_rec(i).SEGMENT25                                   ,                          
p_gl_interface_rec(i).SEGMENT26                                   ,                          
p_gl_interface_rec(i).SEGMENT27                                   ,                          
p_gl_interface_rec(i).SEGMENT28                                   ,                          
p_gl_interface_rec(i).SEGMENT29                                   ,                          
p_gl_interface_rec(i).SEGMENT30                                   ,                          
p_gl_interface_rec(i).ENTERED_DEBIT_AMOUNT                        ,                 
p_gl_interface_rec(i).ENTERED_CREDIT_AMOUNT                       ,                
p_gl_interface_rec(i).CONVERTED_DEBIT_AMOUNT                      ,               
p_gl_interface_rec(i).CONVERTED_CREDIT_AMOUNT                     ,              
p_gl_interface_rec(i).REFERENCE1                                  ,                         
p_gl_interface_rec(i).REFERENCE2                                  ,                         
p_gl_interface_rec(i).REFERENCE3                                  ,                         
p_gl_interface_rec(i).REFERENCE4                                  ,                         
p_gl_interface_rec(i).REFERENCE5                                  ,                         
p_gl_interface_rec(i).REFERENCE6                                  ,                         
p_gl_interface_rec(i).REFERENCE7                                  ,                         
p_gl_interface_rec(i).REFERENCE8                                  ,                         
p_gl_interface_rec(i).REFERENCE9                                  ,                         
SUBSTR((p_gl_interface_rec(i).REFERENCE10), 1, 240)               ,  --truncating REFERENCE10 upto 240                       
p_gl_interface_rec(i).REFERENCE_COLUMN_1                          ,       
p_gl_interface_rec(i).REFERENCE_COLUMN_2                          ,                   
p_gl_interface_rec(i).REFERENCE_COLUMN_3                          ,                   
p_gl_interface_rec(i).REFERENCE_COLUMN_4                          ,                   
p_gl_interface_rec(i).REFERENCE_COLUMN_5                          ,                   
p_gl_interface_rec(i).REFERENCE_COLUMN_6                          ,                   
p_gl_interface_rec(i).REFERENCE_COLUMN_7                          ,                   
p_gl_interface_rec(i).REFERENCE_COLUMN_8                          ,                   
p_gl_interface_rec(i).REFERENCE_COLUMN_9                          ,                   
p_gl_interface_rec(i).REFERENCE_COLUMN_10                         ,                  
p_gl_interface_rec(i).STATISTICAL_AMOUNT                          ,                   
p_gl_interface_rec(i).Currency_Conversion_Type                    ,             
p_gl_interface_rec(i).CURRENCY_CONVERSION_DATE                    ,      
p_gl_interface_rec(i).CURRENCY_CONVERSION_RATE                    ,      
p_gl_interface_rec(i).Interface_Group_Identifier                  , 
p_gl_interface_rec(i).CONTEXT_FENTRY_LINE_DFF                     , 
p_gl_interface_rec(i).ATTRIBUTE1                                  ,      
p_gl_interface_rec(i).ATTRIBUTE2                                  ,      
p_gl_interface_rec(i).ATTRIBUTE3                                  ,      
p_gl_interface_rec(i).ATTRIBUTE4                                  ,      
p_gl_interface_rec(i).ATTRIBUTE5                                  ,      
p_gl_interface_rec(i).ATTRIBUTE6                                  ,      
p_gl_interface_rec(i).ATTRIBUTE7                                  ,      
p_gl_interface_rec(i).ATTRIBUTE8                                  ,      
p_gl_interface_rec(i).ATTRIBUTE9                                  ,      
p_gl_interface_rec(i).ATTRIBUTE10                                 ,      
p_gl_interface_rec(i).ATTRIBUTE11                                 ,      
p_gl_interface_rec(i).ATTRIBUTE12                                 ,      
p_gl_interface_rec(i).ATTRIBUTE13                                 ,      
p_gl_interface_rec(i).ATTRIBUTE14                                 ,      
p_gl_interface_rec(i).ATTRIBUTE15                                 ,      
p_gl_interface_rec(i).ATTRIBUTE16                                 ,      
p_gl_interface_rec(i).ATTRIBUTE17                                 ,      
p_gl_interface_rec(i).ATTRIBUTE18                                 ,      
p_gl_interface_rec(i).ATTRIBUTE19                                 ,      
p_gl_interface_rec(i).ATTRIBUTE20                                 ,      
p_gl_interface_rec(i).CONTEXTFLD                                  ,         
p_gl_interface_rec(i).AVERAGE_JOURNAL_FLAG                        ,      
p_gl_interface_rec(i).CLEARING_COMPANY                            ,         
p_gl_interface_rec(i).LEDGER_NAME                                 ,     
p_gl_interface_rec(i).ENCUMBRANCE_TYPE_ID                         ,      
p_gl_interface_rec(i).RECONCILIATION_REFERENCE                    ,             
to_char(TO_DATE(nvl(p_gl_interface_rec(i).Effective_Trsn_Date,sysdate), 'YYYY-MM-DD'),'MON-YY'), --transforming period_name as MON-YY format.
p_gl_interface_rec(i).REFERENCE_18                                 ,                         
p_gl_interface_rec(i).REFERENCE_19                                 ,                         
p_gl_interface_rec(i).REFERENCE_20                                 ,                         
p_gl_interface_rec(i).ATTRIBUTE_DATE_1                             ,            
p_gl_interface_rec(i).ATTRIBUTE_DATE_2                             ,            
p_gl_interface_rec(i).ATTRIBUTE_DATE_3                             ,            
p_gl_interface_rec(i).ATTRIBUTE_DATE_4                             ,            
p_gl_interface_rec(i).ATTRIBUTE_DATE_5                             ,            
p_gl_interface_rec(i).ATTRIBUTE_DATE_6                             ,            
p_gl_interface_rec(i).ATTRIBUTE_DATE_7                             ,            
p_gl_interface_rec(i).ATTRIBUTE_DATE_8                             ,            
p_gl_interface_rec(i).ATTRIBUTE_DATE_9                             ,            
p_gl_interface_rec(i).ATTRIBUTE_DATE_10                            ,            
p_gl_interface_rec(i).ATTRIBUTE_NUMBER_1                           ,            
p_gl_interface_rec(i).ATTRIBUTE_NUMBER_2                           ,            
p_gl_interface_rec(i).ATTRIBUTE_NUMBER_3                           ,            
p_gl_interface_rec(i).ATTRIBUTE_NUMBER_4                           ,            
p_gl_interface_rec(i).ATTRIBUTE_NUMBER_5                           ,            
p_gl_interface_rec(i).ATTRIBUTE_NUMBER_6                           ,            
p_gl_interface_rec(i).ATTRIBUTE_NUMBER_7                           ,            
p_gl_interface_rec(i).ATTRIBUTE_NUMBER_8                           ,            
p_gl_interface_rec(i).ATTRIBUTE_NUMBER_9                           ,            
p_gl_interface_rec(i).ATTRIBUTE_NUMBER_10                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_CATEGORY                    ,          
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_1                           ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_2                           ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_3                           ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_4                           ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_5                           ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_6                           ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_7                           ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_8                           ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_9                           ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_10                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_11                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_12                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_13                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_14                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_15                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_16                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_17                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_18                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_19                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_20                          ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_DATE_1                      ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_DATE_2                      ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_DATE_3                      ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_DATE_4                      ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_DATE_5                      ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_NUMBER_1                    ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_NUMBER_2                    ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_NUMBER_3                    ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_NUMBER_4                    ,            
p_gl_interface_rec(i).GLOBAL_ATTRIBUTE_NUMBER_5                    ,            
p_current_instance_id||'.zip'                                      ,
p_current_instance_id                                              ,
'KYRIBA'                                                           ,
p_gl_interface_rec(i).CURRENCY_CODE                                ,
'OIC-ATP'                                                          ,
SYSDATE                                                            ,
'OIC-ATP'                                                          ,
SYSDATE                                                            ,
p_current_instance_id                                              ,
p_parent_instance_id                                               ,
p_interface_rice_id                                                ,
p_interface_rice_name                                              ,
'NEW'
);

COMMIT;

--validation section

--segment1 validation

UPDATE XXINT.XXINT_GL_I020_KYRIBA_JOURNALS_STG 
SET STATUS='ERROR',MESSAGE=MESSAGE||'**SEGMENT1 is Null.'
where SEGMENT1 is null and CURRENT_INSTANCE_ID =p_current_instance_id;
COMMIT;
XXINT_XX_I010_COMMON_LOGGING_PKG.DB_LOGGING_DETAILS_PRC (     
	    p_log_flag   ,        
        p_interface_rice_id,  
        p_interface_rice_name,   
        p_parent_instance_id,  
        p_current_instance_id, 
        'xxint_gl_i020_kyriba_journals_pkg.bulk_insert_stg_prc',     
        'Segment1 validation completed at'||systimestamp ,           
		null,         
		null,           
		null,           
		null,           
		null         

    ); 

--segment4 validation

UPDATE XXINT.XXINT_GL_I020_KYRIBA_JOURNALS_STG 
SET STATUS='ERROR',MESSAGE=MESSAGE||'**SEGMENT4 is Null.'
where SEGMENT4 is null and CURRENT_INSTANCE_ID =p_current_instance_id;
COMMIT;
XXINT_XX_I010_COMMON_LOGGING_PKG.DB_LOGGING_DETAILS_PRC (     
	    p_log_flag   ,        
        p_interface_rice_id,  
        p_interface_rice_name,   
        p_parent_instance_id,  
        p_current_instance_id, 
        'xxint_gl_i020_kyriba_journals_pkg.bulk_insert_stg_prc',     
        'Segment4 validation completed at'||systimestamp ,           
		null,         
		null,           
		null,           
		null,           
		null         

    ); 	

--LedgerName validation

UPDATE XXINT.XXINT_GL_I020_KYRIBA_JOURNALS_STG 
SET STATUS='ERROR',MESSAGE=MESSAGE||'**LedgerName is Null.'
where LEDGER_NAME is null and CURRENT_INSTANCE_ID =p_current_instance_id;
COMMIT;
XXINT_XX_I010_COMMON_LOGGING_PKG.DB_LOGGING_DETAILS_PRC (     
	    p_log_flag   ,        
        p_interface_rice_id,  
        p_interface_rice_name,   
        p_parent_instance_id,  
        p_current_instance_id, 
        'xxint_gl_i020_kyriba_journals_pkg.bulk_insert_stg_prc',     
        'LedgerName validation completed at'||systimestamp ,           
		null,         
		null,           
		null,           
		null,           
		null         

    ); 	

        p_status := 'SUCCESS';
        --logging success for bulk insert
XXINT_XX_I010_COMMON_LOGGING_PKG.DB_LOGGING_DETAILS_PRC (     
	    p_log_flag   ,        
        p_interface_rice_id,  
        p_interface_rice_name,   
        p_parent_instance_id,  
        p_current_instance_id, 
        'xxint_gl_i020_kyriba_journals_pkg.bulk_insert_stg_prc',     
        'Procedure Execution Successfully completed at'||systimestamp ,           
		null,         
		null,           
		null,           
		null,           
		null         

    );

EXCEPTION

	   WHEN OTHERS THEN
            p_status := 'ERROR';
            p_error_msg := sqlerrm;
              --logging error
XXINT_XX_I010_COMMON_LOGGING_PKG.DB_LOGGING_DETAILS_PRC (     
	    p_log_flag   ,        
        p_interface_rice_id,  
        p_interface_rice_name,   
        p_parent_instance_id,  
        p_current_instance_id, 
        'xxint_gl_i020_kyriba_journals_pkg.bulk_insert_stg_prc',     
        'Procedure Execution Completed with Error at '||systimestamp||' and ErrorCode '||sqlcode||
        'and error message '||p_error_msg,           
		null,         
		null,           
		null,           
		null,           
		null         

    );
END;

END XXINT_GL_I020_KYRIBA_JOURNALS_PKG;