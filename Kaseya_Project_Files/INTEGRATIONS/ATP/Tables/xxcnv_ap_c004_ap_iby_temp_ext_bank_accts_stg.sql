 /*************************************************************************************
    NAME              :     XXCNV.xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
    PURPOSE           :     This package is the detailed body of all the procedures.
    -- Modification History
    -- Developer          Date         Version     Comments and changes made
    -- -------------   ------       ----------  -----------------------------------------
    --  Priyanka.K   25-Mar-2025       1.0         Initial Development
    --  Bhargavi.K   26-Jul-2025       1.1         Added taget segment column - OC_BRANCH_NAME
    ****************************************************************************************/


Create Table xxcnv.xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
 (
					 FEEDER_IMPORT_BATCH_ID              NUMBER,
	                 TEMP_EXT_PAYEE_ID               	 NUMBER, 
	                 TEMP_EXT_BANK_ACCT_ID               NUMBER,
	                 BANK_NAME                           VARCHAR2(80),
	                 BRANCH_NAME                         VARCHAR2(80),
	                 COUNTRY_CODE                        VARCHAR2(2),
	                 BANK_ACCOUNT_NAME                   VARCHAR2(80),
	                 BANK_ACCOUNT_NUMBER                 VARCHAR2(100),
	                 CURRENCY_CODE                       VARCHAR2(15),
	                 FOREIGN_PAYMENT_USE_FLAG            VARCHAR2(1),
	                 START_DATE                          DATE, 
	                 END_DATE                            DATE, 
	                 IBAN                                VARCHAR2(50),
	                 CHECK_DIGITS                        VARCHAR2(30),
	                 BANK_ACCOUNT_NAME_ALT               VARCHAR2(320),
	                 BANK_ACCOUNT_TYPE                   VARCHAR2(25), 
	                 ACCOUNT_SUFFIX                      VARCHAR2(30), 
	                 DESCRIPTION                         VARCHAR2(240), 
	                 AGENCY_LOCATION_CODE                VARCHAR2(30), 
	                 EXCHANGE_RATE_AGREEMENT_NUM         VARCHAR2(80), 
	                 EXCHANGE_RATE_AGREEMENT_TYPE        VARCHAR2(80),
	                 EXCHANGE_RATE                       NUMBER,
	                 SECONDARY_ACCOUNT_REFERENCE         VARCHAR2(30),
	                 ATTRIBUTE_CATEGORY                  VARCHAR2(150 CHAR),
	                 ATTRIBUTE1                          VARCHAR2(150 CHAR),
	                 ATTRIBUTE2                          VARCHAR2(150 CHAR),
	                 ATTRIBUTE3                          VARCHAR2(150 CHAR),
	                 ATTRIBUTE4                          VARCHAR2(150 CHAR),
	                 ATTRIBUTE5                          VARCHAR2(150 CHAR),
	                 ATTRIBUTE6                          VARCHAR2(150 CHAR),
	                 ATTRIBUTE7                          VARCHAR2(150 CHAR),
	                 ATTRIBUTE8                          VARCHAR2(150 CHAR),
	                 ATTRIBUTE9                          VARCHAR2(150 CHAR),
	                 ATTRIBUTE10                         VARCHAR2(150 CHAR),
	                 ATTRIBUTE11                         VARCHAR2(150 CHAR),
	                 ATTRIBUTE12                         VARCHAR2(150 CHAR),
	                 ATTRIBUTE13                         VARCHAR2(150 CHAR),
	                 ATTRIBUTE14                         VARCHAR2(150 CHAR),
	                 ATTRIBUTE15                         VARCHAR2(150 CHAR),
OC_BRANCH_NAME                         VARCHAR2(80),

					 file_name							VARCHAR2(100),	
					 import_status						VARCHAR2(100),
                     error_message						VARCHAR2(4000),
                     file_reference_identifier          VARCHAR2(4000),
                     execution_id						VARCHAR2(4000),
				     source_system						VARCHAR2(4000)
					 );