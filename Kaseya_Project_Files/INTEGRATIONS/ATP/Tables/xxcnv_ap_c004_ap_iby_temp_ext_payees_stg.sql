 /*************************************************************************************
    NAME              :     XXCNV.xxcnv_ap_c004_iby_temp_ext_payees_stg
    PURPOSE           :     This package is the detailed body of all the procedures.
    -- Modification History
    -- Developer          Date         Version     Comments and changes made
    -- -------------   ------       ----------  -----------------------------------------
    --  Priyanka.K   25-Mar-2025       1.0         Initial Development
    --  Bhargavi.K   26-Jul-2025       1.1         Added taget segment column - OC_VENDOR_NUM
    ****************************************************************************************/

Create table xxcnv.xxcnv_ap_c004_iby_temp_ext_payees_stg(

                 FEEDER_IMPORT_BATCH_ID             NUMBER ,
	             TEMP_EXT_PAYEE_ID					NUMBER , 
	             BUSINESS_UNIT    					VARCHAR2(240) , 
	             VENDOR_NUM          				VARCHAR2(30), 
	             VENDOR_SITE_CODE     				VARCHAR2(240), 
	             EXCLUSIVE_PAYMENT_FLAG  			VARCHAR2(1), 
	             DEFAULT_PAYMENT_METHOD_CODE 		VARCHAR2(30) ,  
	             DELIVERY_CHANNEL_CODE				VARCHAR2(30), 
	             SETTLEMENT_PRIORITY                VARCHAR2(30),
	             REMIT_ADVICE_DELIVERY_METHOD		VARCHAR2(30), 
	             REMIT_ADVICE_EMAIL			        VARCHAR2(255), 
	             REMIT_ADVICE_FAX 					VARCHAR2(100), 
	             BANK_INSTRUCTION1_CODE 			VARCHAR2(30), 
	             BANK_INSTRUCTION2_CODE 			VARCHAR2(30), 
	             BANK_INSTRUCTION_DETAILS			VARCHAR2(255), 
	             PAYMENT_REASON_CODE 				VARCHAR2(30), 
	             PAYMENT_REASON_COMMENTS			VARCHAR2(240), 
	             PAYMENT_TEXT_MESSAGE1 			    VARCHAR2(150), 
	             PAYMENT_TEXT_MESSAGE2 			    VARCHAR2(150), 
	             PAYMENT_TEXT_MESSAGE3 			    VARCHAR2(150), 
	             BANK_CHARGE_BEARER				    VARCHAR2(30),
OC_VENDOR_NUM          				VARCHAR2(30),
				 file_name							VARCHAR2(100),	
				 import_status						VARCHAR2(100),
                 error_message						VARCHAR2(4000),
                 file_reference_identifier          VARCHAR2(4000),
                 execution_id						VARCHAR2(4000),
				source_system						VARCHAR2(4000)
				);