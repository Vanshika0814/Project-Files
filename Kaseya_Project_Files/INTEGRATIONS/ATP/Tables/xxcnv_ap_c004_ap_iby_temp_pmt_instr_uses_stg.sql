create table  xxcnv.xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
(
                   FEEDER_IMPORT_BATCH_ID              NUMBER(18),
	               TEMP_EXT_PAYEE_ID               	   NUMBER(18), 
	               TEMP_EXT_BANK_ACCT_ID               NUMBER(18),
	               TEMP_PMT_INSTR_USE_ID               NUMBER(18),
	               PRIMARY_FLAG                        VARCHAR2(1),
	               START_DATE                          DATE,  
	               END_DATE                            DATE,
				    file_name							VARCHAR2(100),	
					 import_status						VARCHAR2(100),
                     error_message						VARCHAR2(4000),
                     file_reference_identifier          VARCHAR2(4000),
                     execution_id						VARCHAR2(4000),
				     source_system						VARCHAR2(4000)
            );