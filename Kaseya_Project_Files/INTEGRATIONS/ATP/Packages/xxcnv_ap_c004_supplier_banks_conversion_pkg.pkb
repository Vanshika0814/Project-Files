create or replace PACKAGE BODY       xxcnv.xxcnv_ap_c004_supplier_banks_conversion_pkg IS
    /*************************************************************************************
    NAME              :     SUPPLIER_BANK_CONVERSION_PKG BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
    -- Modification History
    -- Developer          Date         Version     Comments and changes made
    -- -------------   ------       ----------  -----------------------------------------
    -- Priyanka Kadam   27-Mar-2025        1.0         Initial Development    
	-- Satya Pavani     03-Aug-2025        1.1         Changes added for v1.1 #Jira LTCI-6590
    ****************************************************************************************/

    -- Declaring global Variables
    gv_import_status            VARCHAR2(256) := NULL;
    gv_error_message            VARCHAR2(500) := NULL;
    gv_file_name                VARCHAR2(256) := NULL;
    gv_oci_file_path            VARCHAR2(256) := NULL;
    gv_oci_file_name            VARCHAR2(4000) := NULL;
    gv_oci_file_name_payee      VARCHAR2(100) := NULL;
    gv_oci_file_name_bank_accts VARCHAR2(100) := NULL;
    gv_oci_file_name_pmt_instr  VARCHAR2(100) := NULL;
    gv_execution_id             VARCHAR2(100) := NULL;
    gv_batch_id                 NUMBER(38) := NULL;
    gv_credential_name          CONSTANT VARCHAR2(100) := 'OCI$RESOURCE_PRINCIPAL';
    gv_status_success           CONSTANT VARCHAR2(100) := 'Success';
    gv_status_failure           CONSTANT VARCHAR2(100) := 'Failure';
    gv_conversion_id            VARCHAR2(100) := NULL;
    gv_boundary_system          VARCHAR2(100) := NULL;
    gv_status_picked            CONSTANT VARCHAR2(100) := 'File_Picked_From_OCI_AND_Loaded_To_Stg';
    gv_status_picked_for_tr     CONSTANT VARCHAR2(100) := 'Transformed_Data_From_Ext_To_Stg';
    gv_status_validated         CONSTANT VARCHAR2(100) := 'Validated';
    gv_status_failed            CONSTANT VARCHAR2(100) := 'Failed_At_Validation';
    gv_status_failed_validation CONSTANT VARCHAR2(100) := 'Not_Validated';
    gv_fbdi_export_status       CONSTANT VARCHAR2(100) := 'Exported_To_Fbdi';
    gv_status_staged            CONSTANT VARCHAR2(100) := 'Staged_For_Import';
    gv_transformed_folder       CONSTANT VARCHAR2(100) := 'Transformed_FBDI_Files';
    gv_source_folder            CONSTANT VARCHAR2(100) := 'Source_FBDI_Files';
    gv_properties               CONSTANT VARCHAR2(100) := 'properties';
    gv_file_picked              VARCHAR2(100) := 'File_Picked_From_OCI_Server';
    gv_recon_folder             CONSTANT VARCHAR2(100) := 'ATP_Validation_Error_Files';
    gv_recon_report             CONSTANT VARCHAR2(100) := 'Recon_Report_Created';
    gv_file_not_found           CONSTANT VARCHAR2(100) := 'File_not_found';

    /*===========================================================================================================
    -- PROCEDURE : main_prc
    -- PARAMETERS:
    -- COMMENT   : This procedure is used to call all the procedures under a single procedure
    ==============================================================================================================*/
    PROCEDURE main_prc (
        p_rice_id         IN VARCHAR2,
        p_execution_id    IN VARCHAR2,
        p_boundary_system IN VARCHAR2,
        p_file_name       IN VARCHAR2
    ) AS

        p_loading_status VARCHAR2(30) := NULL;
        lv_start_pos     NUMBER := 1;
        lv_end_pos       NUMBER;
        lv_file_name     VARCHAR2(4000);
    BEGIN
        gv_conversion_id := p_rice_id;
        gv_execution_id := p_execution_id;
        gv_boundary_system := p_boundary_system;
        dbms_output.put_line('conversion_id: ' || gv_conversion_id);
        dbms_output.put_line('execution_id: ' || gv_execution_id);
        dbms_output.put_line('boundary_system: ' || gv_boundary_system);

        -- Fetch execution details

        BEGIN
            SELECT
                ce.execution_id,
                ce.file_path,
                ce.file_name
            INTO
                gv_execution_id,
                gv_oci_file_path,
                gv_oci_file_name
            FROM
                xxcnv_cmn_conversion_execution ce
            WHERE
                    ce.conversion_id = gv_conversion_id
                AND ce.status = gv_file_picked
                AND ce.last_update_date = (
                    SELECT
                        MAX(ce1.last_update_date)
                    FROM
                        xxcnv_cmn_conversion_execution ce1
                    WHERE
                            ce1.conversion_id = gv_conversion_id
                        AND ce1.status = gv_file_picked
                )
                AND ROWNUM = 1;


				-- Debugging output
            dbms_output.put_line('Fetched execution details:');
            dbms_output.put_line('Execution ID: ' || gv_execution_id);
            dbms_output.put_line('File Path: ' || gv_oci_file_path);
            dbms_output.put_line('File Name: ' || gv_oci_file_name);

				-- Initialize loop variables
            lv_start_pos := 1;

				-- Split the concatenated file names and assign to global variables
            LOOP
                lv_end_pos := instr(gv_oci_file_name, '.csv', lv_start_pos) + 3;
                EXIT WHEN lv_end_pos = 3; -- Exit loop if no more '.csv' found

                lv_file_name := substr(gv_oci_file_name, lv_start_pos, lv_end_pos - lv_start_pos + 1);
                dbms_output.put_line('Processing file name: ' || lv_file_name); -- Debugging output

                CASE
                    WHEN lv_file_name LIKE '%IbyTempExtPayees%.csv' THEN
                        gv_oci_file_name_payee := lv_file_name;
                    WHEN lv_file_name LIKE '%IbyTempExtBankAccts%.csv' THEN
                        gv_oci_file_name_bank_accts := lv_file_name;
                    WHEN lv_file_name LIKE '%IbyTempPmtInstrUses%.csv' THEN
                        gv_oci_file_name_pmt_instr := lv_file_name;
                    ELSE
                        dbms_output.put_line('No match found for file name: ' || lv_file_name); -- Debugging output
                END CASE;

                lv_start_pos := lv_end_pos + 1;
            END LOOP;

				-- Output the results for debugging
            dbms_output.put_line('lv_File Name: ' || lv_file_name);
            dbms_output.put_line('Payee File Name: ' || gv_oci_file_name_payee);
            dbms_output.put_line('Bank Accounts File Name: ' || gv_oci_file_name_bank_accts);
            dbms_output.put_line('Pmt Instr File Name: ' || gv_oci_file_name_pmt_instr);
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error fetching execution details: ' || sqlerrm);
        END;

			-- Call to import data from OCI to external table
        BEGIN
            import_data_from_oci_to_stg_prc(p_loading_status);
            IF p_loading_status = gv_status_failure THEN
                dbms_output.put_line('Error in import_data_from_oci_to_stg_prc ');
                RETURN;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling import_data_from_oci_to_stg_prc : ' || sqlerrm);
        END;


    -- Call to perform data and business validations in interface table
        BEGIN
            data_validations_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling data_validations_prc: ' || sqlerrm);
        END;

    -- Call to create a CSV file from supplier_banks_interface after all validations
        BEGIN
            create_fbdi_file_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_fbdi_file_prc: ' || sqlerrm);
        END;

    --CREATE RECON REPORT 

        BEGIN
            create_atp_validation_recon_report_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_atp_validation_recon_report_prc: ' || sqlerrm);
        END;

    END main_prc;

/*=================================================================================================================
-- PROCEDURE : import_data_from_oci_to_stg_prc 
-- PARAMETERS: p_loading_status
-- COMMENT   : This procedure is used to create an external table and transfer that data from external to stg table.
===================================================================================================================*/
    PROCEDURE import_data_from_oci_to_stg_prc (
        p_loading_status OUT VARCHAR2
    ) IS
        lv_table_count NUMBER := 0;
        lv_row_count   NUMBER := 0;
    BEGIN
        BEGIN
            BEGIN
                lv_table_count := 0;
                SELECT
                    COUNT(*)
                INTO lv_table_count
                FROM
                    all_objects
                WHERE
                        upper(object_name) = 'XXCNV_AP_C004_IBY_TEMP_EXT_PAYEES_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c004_iby_temp_ext_payees_ext';
                    dbms_output.put_line('Table xxcnv_ap_c004_iby_temp_ext_payees_ext dropped');
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c004_iby_temp_ext_payees_stg';
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_ap_c004_iby_temp_ext_payees_ext: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '|->'
                                         || dbms_utility.format_error_backtrace);

                    p_loading_status := gv_status_failure;
            END;

            BEGIN
                lv_table_count := 0;
                SELECT
                    COUNT(*)
                INTO lv_table_count
                FROM
                    all_objects
                WHERE
                        upper(object_name) = 'XXCNV_AP_C004_IBY_TEMP_EXT_BANK_ACCTS_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c004_iby_temp_ext_bank_accts_ext';
                    dbms_output.put_line('Table xxcnv_ap_c004_iby_temp_ext_bank_accts_ext dropped');
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg';
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_ap_c004_iby_temp_ext_bank_accts_ext  : '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '|->'
                                         || dbms_utility.format_error_backtrace);

                    p_loading_status := gv_status_failure;
			--RETURN;
            END;

            BEGIN
                lv_table_count := 0;
                SELECT
                    COUNT(*)
                INTO lv_table_count
                FROM
                    all_objects
                WHERE
                        upper(object_name) = 'XXCNV_AP_C004_IBY_TEMP_PMT_INSTR_USES_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c004_iby_temp_pmt_instr_uses_ext';
                    dbms_output.put_line('Table xxcnv_ap_c004_iby_temp_pmt_instr_uses_ext dropped');
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg';
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_ap_c004_iby_temp_pmt_instr_uses_ext : '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '|->'
                                         || dbms_utility.format_error_backtrace);

                    p_loading_status := gv_status_failure;
            END;

        END;

    -- Create the external table
        BEGIN
            IF gv_oci_file_name_payee LIKE '%IbyTempExtPayees%' THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c004_iby_temp_ext_payees_ext');
                dbms_output.put_line('xxcnv_ap_c004_iby_temp_ext_payees_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_payee);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c004_iby_temp_ext_payees_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_payee,
                    format          =>
                            JSON_OBJECT(
                                'skipheaders' VALUE '1',
                                'type' VALUE 'csv',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'ignoremissingcolumns' VALUE 'true',
                                'blankasnull' VALUE 'true'
                            ),
                    column_list     => 'FEEDER_IMPORT_BATCH_ID             NUMBER ,
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
	             BANK_CHARGE_BEARER				    VARCHAR2(30)'
                );

                dbms_output.put_line(' External table xxcnv_ap_c004_iby_temp_ext_payees_ext is created');
                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c004_iby_temp_ext_payees_stg (
					             FEEDER_IMPORT_BATCH_ID,          
                                 TEMP_EXT_PAYEE_ID,				
                                 BUSINESS_UNIT,    				
                                 VENDOR_NUM,          			
                                 VENDOR_SITE_CODE,     			
                                 EXCLUSIVE_PAYMENT_FLAG,  		
                                 DEFAULT_PAYMENT_METHOD_CODE, 	
                                 DELIVERY_CHANNEL_CODE,			
                                 SETTLEMENT_PRIORITY,             
                                 REMIT_ADVICE_DELIVERY_METHOD,	
                                 REMIT_ADVICE_EMAIL,				
                                 REMIT_ADVICE_FAX, 				
                                 BANK_INSTRUCTION1_CODE, 			
                                 BANK_INSTRUCTION2_CODE, 			
                                 BANK_INSTRUCTION_DETAILS,		
                                 PAYMENT_REASON_CODE, 			
                                 PAYMENT_REASON_COMMENTS,			
                                 PAYMENT_TEXT_MESSAGE1, 			
                                 PAYMENT_TEXT_MESSAGE2, 			
                                 PAYMENT_TEXT_MESSAGE3, 			
                                 BANK_CHARGE_BEARER,
								 file_name,
								 import_status,
                                 error_message,
                                 file_reference_identifier,
								 --execution_id,
								 source_system
							) SELECT
								 FEEDER_IMPORT_BATCH_ID,          
                                 TEMP_EXT_PAYEE_ID,				
                                 BUSINESS_UNIT,    				
                                 VENDOR_NUM,          			
                                 VENDOR_SITE_CODE,     			
                                 EXCLUSIVE_PAYMENT_FLAG,  		
                                 DEFAULT_PAYMENT_METHOD_CODE, 	
                                 DELIVERY_CHANNEL_CODE,			
                                 SETTLEMENT_PRIORITY,             
                                 REMIT_ADVICE_DELIVERY_METHOD,	
                                 REMIT_ADVICE_EMAIL,				
                                 REMIT_ADVICE_FAX, 				
                                 BANK_INSTRUCTION1_CODE, 			
                                 BANK_INSTRUCTION2_CODE, 			
                                 BANK_INSTRUCTION_DETAILS,		
                                 PAYMENT_REASON_CODE, 			
                                 PAYMENT_REASON_COMMENTS,			
                                 PAYMENT_TEXT_MESSAGE1, 			
                                 PAYMENT_TEXT_MESSAGE2, 			
                                 PAYMENT_TEXT_MESSAGE3, 			
                                 BANK_CHARGE_BEARER,
								 null,
                                 null,
                                 null,
								 null,
					             --'
                                  || chr(39)
                                  || gv_execution_id
                                  || chr(39)
                                  || ',
								 null FROM xxcnv_ap_c004_iby_temp_ext_payees_ext';

                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c004_iby_temp_ext_payees_stg: ' || SQL%rowcount);
				--commit;
            END IF;

            IF gv_oci_file_name_bank_accts LIKE '%IbyTempExtBankAccts%' THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c004_iby_temp_ext_bank_accts_ext');
                dbms_output.put_line(' xxcnv_ap_c004_iby_temp_ext_bank_accts_ext   : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_bank_accts);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c004_iby_temp_ext_bank_accts_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_bank_accts,
                    format          =>
                            JSON_OBJECT(
                                'skipheaders' VALUE '1',
                                'type' VALUE 'csv',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'dateformat' VALUE 'yyyy/mm/dd',
                                'ignoremissingcolumns' VALUE 'true',
                                        'blankasnull' VALUE 'true'
                            ),
                    column_list     => 'FEEDER_IMPORT_BATCH_ID              NUMBER,
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
	                 ATTRIBUTE15                         VARCHAR2(150 CHAR)'
                );

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c004_iby_temp_ext_bank_accts_stg (
			            FEEDER_IMPORT_BATCH_ID,      
						TEMP_EXT_PAYEE_ID,          
						TEMP_EXT_BANK_ACCT_ID,       
						BANK_NAME,                   
						BRANCH_NAME,                 
						COUNTRY_CODE,                
						BANK_ACCOUNT_NAME,           
						BANK_ACCOUNT_NUMBER,         
						CURRENCY_CODE,               
						FOREIGN_PAYMENT_USE_FLAG,    
						START_DATE,                  
						END_DATE,                    
						IBAN,                        
						CHECK_DIGITS,                
						BANK_ACCOUNT_NAME_ALT,       
						BANK_ACCOUNT_TYPE,           
						ACCOUNT_SUFFIX,              
						DESCRIPTION,                 
						AGENCY_LOCATION_CODE,        
						EXCHANGE_RATE_AGREEMENT_NUM, 
						EXCHANGE_RATE_AGREEMENT_TYPE,
						EXCHANGE_RATE,               
						SECONDARY_ACCOUNT_REFERENCE, 
						ATTRIBUTE_CATEGORY,          
						ATTRIBUTE1,                  
						ATTRIBUTE2,                  
						ATTRIBUTE3,                  
						ATTRIBUTE4,                  
						ATTRIBUTE5,                  
						ATTRIBUTE6,                  
						ATTRIBUTE7,                  
						ATTRIBUTE8,                  
						ATTRIBUTE9,                  
						ATTRIBUTE10,                 
						ATTRIBUTE11,                 
						ATTRIBUTE12,                 
						ATTRIBUTE13,                 
						ATTRIBUTE14,                 
						ATTRIBUTE15, 
						file_name,
						import_status,
						error_message,
						file_reference_identifier,
						--execution_id,
						source_system) 
					SELECT 
						FEEDER_IMPORT_BATCH_ID,      
						TEMP_EXT_PAYEE_ID,          
						TEMP_EXT_BANK_ACCT_ID,       
						BANK_NAME,                   
						BRANCH_NAME,                 
						COUNTRY_CODE,                
						BANK_ACCOUNT_NAME,           
						BANK_ACCOUNT_NUMBER,         
						CURRENCY_CODE,               
						FOREIGN_PAYMENT_USE_FLAG,    
						START_DATE,                  
						END_DATE,                    
						IBAN,                        
						CHECK_DIGITS,                
						BANK_ACCOUNT_NAME_ALT,       
						BANK_ACCOUNT_TYPE,           
						ACCOUNT_SUFFIX,              
						DESCRIPTION,                 
						AGENCY_LOCATION_CODE,        
						EXCHANGE_RATE_AGREEMENT_NUM, 
						EXCHANGE_RATE_AGREEMENT_TYPE,
						EXCHANGE_RATE,               
						SECONDARY_ACCOUNT_REFERENCE, 
						ATTRIBUTE_CATEGORY,          
						ATTRIBUTE1,                  
						ATTRIBUTE2,                  
						ATTRIBUTE3,                  
						ATTRIBUTE4,                  
						ATTRIBUTE5,                  
						ATTRIBUTE6,                  
						ATTRIBUTE7,                  
						ATTRIBUTE8,                  
						ATTRIBUTE9,                  
						ATTRIBUTE10,                 
						ATTRIBUTE11,                 
						ATTRIBUTE12,                 
						ATTRIBUTE13,                 
						ATTRIBUTE14,                 
						ATTRIBUTE15,
						null,
						null,
						null,
						null,
						--'
                                  || chr(39)
                                  || gv_execution_id
                                  || chr(39)
                                  || ',
						null
						FROM xxcnv_ap_c004_iby_temp_ext_bank_accts_ext';

                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c004_iby_temp_ext_bank_accts_stg: ' || SQL%rowcount);
				--commit;
            END IF;

            IF gv_oci_file_name_pmt_instr LIKE '%IbyTempPmtInstrUses%' THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c004_iby_temp_pmt_instr_uses_ext');
                dbms_output.put_line(' xxcnv_ap_c004_iby_temp_pmt_instr_uses_ext  : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_pmt_instr);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c004_iby_temp_pmt_instr_uses_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_pmt_instr,
                    format          =>
                            JSON_OBJECT(
                                'skipheaders' VALUE '1',
                                'type' VALUE 'csv',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'dateformat' VALUE 'yyyy/mm/dd',
                                'ignoremissingcolumns' VALUE 'true',
                                        'blankasnull' VALUE 'true'
                            ),
                    column_list     => 'FEEDER_IMPORT_BATCH_ID              NUMBER(18),
	               TEMP_EXT_PAYEE_ID               	   NUMBER(18), 
	               TEMP_EXT_BANK_ACCT_ID               NUMBER(18),
	               TEMP_PMT_INSTR_USE_ID               NUMBER(18),
	               PRIMARY_FLAG                        VARCHAR2(1),
	               START_DATE                          DATE,  
	               END_DATE                            DATE'
                );

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg (

					FEEDER_IMPORT_BATCH_ID,  
                    TEMP_EXT_PAYEE_ID,        
                    TEMP_EXT_BANK_ACCT_ID,   
                    TEMP_PMT_INSTR_USE_ID,   
                    PRIMARY_FLAG,            
                    START_DATE,              
                    END_DATE, 
					file_name,
					import_status,
					error_message,
					file_reference_identifier,
					--execution_id,
					source_system) 
				SELECT 
					FEEDER_IMPORT_BATCH_ID,  
                    TEMP_EXT_PAYEE_ID,        
                    TEMP_EXT_BANK_ACCT_ID,   
                    TEMP_PMT_INSTR_USE_ID,   
                    PRIMARY_FLAG,            
                    START_DATE,              
                    END_DATE,
					null,
					null,
					null,
					null,
					--'
                                  || chr(39)
                                  || gv_execution_id
                                  || chr(39)
                                  || ',
                    null					
					FROM xxcnv_ap_c004_iby_temp_pmt_instr_uses_ext';

                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg: ' || SQL%rowcount);
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error creating external table: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

    -- Count the number of rows in the external table
        BEGIN
            IF gv_oci_file_name = '%IbyTempExtPayees%' THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c004_iby_temp_ext_payees_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c004_iby_temp_ext_payees_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;

            IF gv_oci_file_name = '%IbyTempExtBankAccts%' THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c004_iby_temp_ext_bank_accts_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c004_iby_temp_ext_bank_accts_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;

            IF gv_oci_file_name = '%IbyTempPmtInstrUses%' THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in the external table: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

    -- Select FEEDER_IMPORT_BATCH_ID from the external table
        BEGIN
        -- Count the number of rows in the external table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c004_iby_temp_ext_payees_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c004_iby_temp_ext_payees_stg from OCI Source Folder: ' || lv_row_count
            );

        -- Use an implicit cursor in the FOR LOOP to iterate over distinct batch_ids
            FOR rec IN (
                SELECT DISTINCT
                    feeder_import_batch_id
                FROM
                    xxcnv_ap_c004_iby_temp_ext_payees_stg
            ) LOOP
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_picked,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name,
                    p_attribute1        => rec.feeder_import_batch_id,
                    p_attribute2        => lv_row_count,
                    p_process_reference => NULL
                );
            END LOOP;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_ap_c004_iby_temp_ext_payees_stg: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

    END import_data_from_oci_to_stg_prc;
/*=================================================================================================================
-- PROCEDURE : data_validations_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for the validating the mandatory columns and business validations as per lean spec
===================================================================================================================*/
    PROCEDURE data_validations_prc IS

  -- Declaring Local Variables for validation.     
        lv_row_count   NUMBER;
        lv_error_count NUMBER;
    BEGIN
        BEGIN

     -- Initializing batch_id to current time stamp --

            SELECT
                to_char(sysdate, 'YYYYMMDDHHMMSS')
            INTO gv_batch_id
            FROM
                dual;

            BEGIN
                UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                SET
                    execution_id = gv_execution_id,
                    feeder_import_batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END;
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c004_iby_temp_ext_payees_stg
            WHERE
                execution_id = gv_execution_id;

            IF lv_row_count <> 0 THEN 

		 -- Initialize ERROR_MESSAGE to an empty string if it is NULL
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        error_message = ''
                    WHERE
                        error_message IS NULL;

                END;


         -- Validate Payee Identifier
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        error_message = error_message || '|Payee Identifier should not be null'
                    WHERE
                        temp_ext_payee_id IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Payee Identifier is validated');
                END;

          -- Validate Supplier Number
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        error_message = error_message || '|Supplier Number should not be null'
                    WHERE
                        vendor_num IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Supplier Number is validated');
                END;


          -- Update Supplier Number
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg 
                    SET
                        oc_vendor_num = (
                            SELECT
                                oc_vendor_num
                            FROM
                                xxcnv_ap_c008_contract_supplier_mapping
                            WHERE
                                    ns_vendor_num = vendor_num
                                AND prc_bu_name = business_unit
                        )
                    WHERE
                        vendor_num IS NOT NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Supplier Number is updated');
                END;

          -- Validate Supplier Number
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        error_message = error_message || '|Supplier is not present in Oracle'
                    WHERE
                        oc_vendor_num IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Supplier Number is validated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        default_payment_method_code = (
                            SELECT
                                oc_value
                            FROM
                                xxcnv_ap_payment_method_mapping
                            WHERE
                                upper(ns_value) = upper(default_payment_method_code)
                        )
                    WHERE
                        default_payment_method_code IS NOT NULL;

                    dbms_output.put_line('Payment Method is updated');
                END;

          -- Validate Payment Reason Code  
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        error_message = error_message || '|Payment Reason Code is mandatory for this country'
                    WHERE
                        --payment_reason_code IS NULL     -- Commented as per v1.1
						payment_reason_comments IS NULL -- Added as per v1.1
                        AND temp_ext_payee_id IN (
                            SELECT
                                temp_ext_payee_id
                            FROM
                                xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                            WHERE
                                country_code IN ( 'AE', 'BH', 'CN', 'IN', 'MY',
                                                  'OM', 'PH', 'QA', 'RU', 'TH' )
                        )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Supplier Number is validated');
                END;

          -- Update Supplier Site
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        vendor_site_code = (
                            SELECT
                                oc_vendor_site
                            FROM
                                xxcnv_ap_c008_contract_supplier_mapping
                            WHERE
                                    ns_vendor_num = vendor_num
                                AND prc_bu_name = business_unit
                        )
                    WHERE
                        oc_vendor_num IS NOT NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Supplier Site is updated');
                END;

          -- Validate Supplier Site
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        error_message = error_message || '|Supplier Site is not present in Oracle'
                    WHERE
                        vendor_site_code IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Supplier Site is validated');
                END;

		   --Update Supplier Site with commas
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        vendor_site_code = '"'
                                           || vendor_site_code
                                           || '|"'
                    WHERE
                        vendor_site_code LIKE '%,%'
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Supplier Site with Comma is validated');
                END;



	      -- Check for uniqueness of the concatenation of Import Batch Identifier and Payee Identifier
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        error_message = error_message || '|Payee Identifier value should be unique'
                    WHERE
                        ( temp_ext_payee_id ) IN (
                            SELECT
                                temp_ext_payee_id
                            FROM
                                xxcnv_ap_c004_iby_temp_ext_payees_stg
                            GROUP BY
                                temp_ext_payee_id
                            HAVING
                                COUNT(*) > 1
                        )
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Uniqueness is validated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        error_message = error_message || '|Payee Identifier is not present in the child file IBY_TEMP_EXT_BANK_ACCTS.csv'
                    WHERE
                        ( temp_ext_payee_id ) NOT IN (
                            SELECT
                                temp_ext_payee_id
                            FROM
                                xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                        )
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Child 1 is validated');
                END;




           -- Update import_status based on error_message
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        import_status =
                            CASE
                                WHEN error_message IS NOT NULL THEN
                                    'ERROR'
                                ELSE
                                    'PROCESSED'
                            END
                    WHERE
                        execution_id = gv_execution_id;

                    dbms_output.put_line('import_status is validated');
                END;


          -- Final update to set error_message and import_status
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        error_message = ltrim(error_message, ','),
                        import_status =
                            CASE
                                WHEN error_message IS NOT NULL THEN
                                    'ERROR'
                                ELSE
                                    'PROCESSED'
                            END
                    WHERE
                        execution_id = gv_execution_id;

                    dbms_output.put_line('import_status column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        source_system = gv_conversion_id,
                        exclusive_payment_flag = 'N'
                    WHERE
                        file_reference_identifier IS NULL;

                    dbms_output.put_line('source_system is updated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        file_name = gv_oci_file_name_payee
                    WHERE
                        file_reference_identifier IS NULL;

                    dbms_output.put_line('file_name column is updated');
                END;


	  -- Check if there are any error messages
                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_ap_c004_iby_temp_ext_payees_stg
                WHERE
                    error_message IS NOT NULL
                    AND file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    error_message IS NOT NULL
                    AND file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_reference_identifier column is updated');
                UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success
                WHERE
                    error_message IS NULL
                    AND file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_reference_identifier column is updated');
                IF lv_error_count > 0 THEN
			 -- Logging the message If data is not validated
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_failed,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_payee,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

                IF
                    lv_error_count = 0
                    AND gv_oci_file_name_payee IS NOT NULL
                THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_validated,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_payee,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;
	  --COMMIT;
                IF gv_oci_file_name_payee IS NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_file_not_found,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_payee,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

            ELSE
                dbms_output.put_line('No Data is found in interface tables. Data is not loaded from ext to stg ');
            END IF;

        END;

  -- 2 TABLE BANK ACCOUNTS 

  -- bank accounts validations
        BEGIN
            BEGIN
                UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                SET
                    execution_id = gv_execution_id,
                    feeder_import_batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END;
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
            WHERE
                execution_id = gv_execution_id;

            IF lv_row_count <> 0 THEN

		  -- Initialize ERROR_MESSAGE to an empty string if it is NULL
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = ''
                    WHERE
                        error_message IS NULL;

                END;

          -- Validate Payee Identifier
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Payee Identifier should not be null'
                    WHERE
                        temp_ext_payee_id IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Payee Identifier is validated');
                END;

          -- Validate Payee Bank Account Identifier
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Payee Bank Account Identifier should not be null'
                    WHERE
                        temp_ext_bank_acct_id IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Payee Bank Account Identifier is validated');
                END;


          -- Validate Account Country Code
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Account Country Code should not be null'
                    WHERE
                        country_code IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Account Country Code is validated');
                END;



		   --Validate Bank Account Name with comma's
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        bank_account_name = '"'
                                            || bank_account_name
                                            || '"'
                    WHERE
                        bank_account_name LIKE '%,%'
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Bank Account Name With Comma is validated');
                END;


	-- United States --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( branch_name IS NULL
                            OR bank_account_number IS NULL )
                          OR ( branch_name IS NULL
                               AND bank_account_number IS NULL ) )
                        AND country_code = 'US'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- INDIA --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( bank_name IS NULL
                            OR branch_name IS NULL
                            OR bank_account_number IS NULL
                            OR bank_account_name IS NULL )
                          OR ( bank_name IS NULL
                               AND branch_name IS NULL
                               AND bank_account_number IS NULL
                               AND bank_account_name IS NULL ) )
                        AND country_code = 'IN'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- IRELAND --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( bank_name IS NULL
                            OR iban IS NULL
                            OR bank_account_name IS NULL )
                          OR ( bank_name IS NULL
                               AND iban IS NULL
                               AND bank_account_name IS NULL ) )
                        AND country_code = 'IE'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Philippines --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( bank_name IS NULL
                            OR branch_name IS NULL
                            OR bank_account_name IS NULL )
                          OR ( bank_name IS NULL
                               AND branch_name IS NULL
                               AND bank_account_name IS NULL ) )
                        AND country_code = 'PH'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- CHINA --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( bank_name IS NULL
                            OR branch_name IS NULL
                            OR bank_account_number IS NULL
                            OR bank_account_name IS NULL )
                          OR ( bank_name IS NULL
                               AND branch_name IS NULL
                               AND bank_account_number IS NULL
                               AND bank_account_name IS NULL ) )
                        AND country_code = 'CN'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- SWEDEN --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                        WHERE  bank_account_number IS NULL
                        AND iban IS NULL
                        AND country_code = 'SE'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- POLAND --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE iban IS NULL 
                        AND country_code = 'PL'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Malaysia --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( bank_name IS NULL
                            OR branch_name IS NULL )
                          OR ( bank_name IS NULL
                               AND branch_name IS NULL ) )
                        AND country_code = 'MY'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Lebanon --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( bank_name IS NULL
                            OR iban IS NULL )
                          OR ( bank_name IS NULL
                               AND iban IS NULL ) )
                        AND country_code = 'LB'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Morocco --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( bank_name IS NULL
                            OR bank_account_number IS NULL )
                          OR ( bank_name IS NULL
                               AND bank_account_number IS NULL ) )
                        AND country_code = 'MA'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Costa Rica --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE ( bank_account_number IS NULL
                               AND iban IS NULL )
                        AND country_code = 'CR'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- New Zealand --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( branch_name IS NULL
                            OR bank_account_number IS NULL
                            OR bank_account_name IS NULL
                            OR account_suffix IS NULL )
                          OR ( branch_name IS NULL
                               AND bank_account_number IS NULL
                               AND bank_account_name IS NULL
                               AND account_suffix IS NULL ) )
                        AND country_code = 'NZ'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Italy --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE      branch_name IS NULL
                               AND iban IS NULL 
                        AND country_code = 'IT'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Canada --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( branch_name IS NULL
                            OR bank_account_number IS NULL )
                          OR ( branch_name IS NULL
                               AND bank_account_number IS NULL ) )
                        AND country_code = 'CA'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Australia --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( branch_name IS NULL
                            OR bank_account_number IS NULL )
                          OR ( branch_name IS NULL
                               AND bank_account_number IS NULL ) )
                        AND country_code = 'AU'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Germany --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE iban IS NULL
                        AND country_code = 'DE'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Netherlands --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE iban IS NULL 
                        AND country_code = 'NL'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Switzerland --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE iban IS NULL 
                        AND country_code = 'CH'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Romania --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE  iban IS NULL 
                        AND country_code = 'RO'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Slovakia --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE iban IS NULL 
                        AND country_code = 'SK'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Lithuania --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE iban IS NULL 
                        AND country_code = 'LT'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Columbia --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE iban IS NULL 
                        AND country_code = 'CO'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Finland --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE iban IS NULL 
                        AND country_code = 'FI'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Belgium --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE iban IS NULL
                        AND country_code = 'BE'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Spain --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE		branch_name IS NULL
                                AND iban IS NULL 
                        AND country_code = 'ES'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- France --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE      branch_name IS NULL
                               AND iban IS NULL 
                        AND country_code = 'FR'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Singapore --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( branch_name IS NULL
                            OR bank_account_number IS NULL
                            OR bank_account_name IS NULL )
                          OR ( branch_name IS NULL
                               AND bank_account_number IS NULL
                               AND bank_account_name IS NULL ) )
                        AND country_code = 'SG'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- South Africa --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( branch_name IS NULL
                            OR bank_account_number IS NULL
                            OR bank_account_name IS NULL )
                          OR ( branch_name IS NULL
                               AND bank_account_number IS NULL
                               AND bank_account_name IS NULL ) )
                        AND country_code = 'ZA'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Israel --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE branch_name IS NULL 
                        AND country_code = 'IL'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Jersey --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        ( ( branch_name IS NULL
                            OR bank_account_number IS NULL
                            OR bank_account_name IS NULL )
                          OR ( branch_name IS NULL
                               AND bank_account_number IS NULL
                               AND bank_account_name IS NULL ) )
                        AND country_code = 'JE'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Indonesia --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        bank_account_number IS NULL
                        AND country_code = 'ID'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Eswatini --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        bank_account_number IS NULL
                        AND country_code = 'SZ'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Cayman Islands --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        bank_account_number IS NULL
                        AND country_code = 'KY'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Puerto Rico --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        bank_account_number IS NULL
                        AND country_code = 'PR'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Panama --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        bank_account_number IS NULL
                        AND country_code = 'PA'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Honduras --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        bank_account_number IS NULL
                        AND country_code = 'HN'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Taiwan --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        bank_account_number IS NULL
                        AND country_code = 'TW'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Mexico --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        bank_account_number IS NULL
                        AND country_code = 'MX'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Algeria --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE
                        bank_account_number IS NULL
                        AND country_code = 'DZ'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Iceland --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE     bank_account_number IS NULL
                               AND iban IS NULL 
                        AND country_code = 'IS'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Georgia --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE bank_account_number IS NULL
                               AND iban IS NULL 
                        AND country_code = 'GE'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Belarus --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE     bank_account_number IS NULL
                               AND iban IS NULL
                        AND country_code = 'BY'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Serbia --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE     bank_account_number IS NULL
                               AND iban IS NULL 
                        AND country_code = 'RS'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;		  
	-- Dominican Republic --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE    bank_account_number IS NULL
                               AND iban IS NULL
                        AND country_code = 'DO'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;		  
	-- Croatia --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE     bank_account_number IS NULL
                               AND iban IS NULL
                        AND country_code = 'HR'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;		  
	-- Moldova --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE   bank_account_number IS NULL
                               AND iban IS NULL 
                        AND country_code = 'MD'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;		  
	-- Norway --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE      bank_account_number IS NULL
                               AND iban IS NULL 
                        AND country_code = 'NO'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;		  
	-- Turkey --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE      bank_account_number IS NULL
                               AND iban IS NULL 
                        AND country_code = 'TR'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- United Kingdom --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Mandatory columns should not be NULL'
                    WHERE      bank_account_number IS NULL
                               AND iban IS NULL 
                        AND country_code = 'GB'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	-- Branch Number/BIC Code --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Atleast Branch Number OR BIC Code should not be NULL'
                    WHERE
                        ( branch_name IS NULL
                          AND attribute2 IS NULL )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Branch Number is validated');
                END;

	 -- Branch Name update 4 combination--
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg stg
                    SET
                        stg.oc_branch_name = (
                            SELECT DISTINCT
                                bm.branch_name
                            FROM
                                xxcnv_supplier_branch_mapping bm
                            WHERE
                                ( bm.bank_name IS NOT NULL
                                  AND bm.bank_city IS NOT NULL
                                  AND bm.bic_code IS NOT NULL
                                  AND bm.routing_number IS NOT NULL )
                                AND ( upper(stg.bank_name)
                                      || upper(stg.attribute1)
                                      || upper(stg.attribute2)
                                      || upper(stg.branch_name) ) = ( upper(bm.bank_name)
                                                                      || upper(bm.bank_city)
                                                                      || upper(bm.bic_code)
                                                                      || upper(bm.routing_number) )
                        )
                    WHERE
                        ( stg.bank_name IS NOT NULL
                          AND stg.attribute1 IS NOT NULL
                          AND stg.attribute2 IS NOT NULL
                          AND stg.branch_name IS NOT NULL );

                    dbms_output.put_line('Branch Name 4 combo is validated');
                END;

	--  Branch Name update 3 combination--
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg stg
                    SET
                        stg.oc_branch_name = (
                            SELECT
                                bm.branch_name
                            FROM
                                xxcnv_supplier_branch_mapping bm
                            WHERE
                                ( bm.bank_name IS NOT NULL
                                  AND bm.bank_city IS NOT NULL
                                  AND bm.bic_code IS NOT NULL
                                  AND stg.branch_name IS NULL )
                                AND ( upper(stg.bank_name)
                                      || upper(stg.attribute1)
                                      || upper(stg.attribute2) ) = ( upper(bm.bank_name)
                                                                     || upper(bm.bank_city)
                                                                     || upper(bm.bic_code) )
                                AND ROWNUM = 1
                        )
                    WHERE
                        ( stg.bank_name IS NOT NULL
                          AND stg.attribute1 IS NOT NULL
                          AND stg.attribute2 IS NOT NULL )
                        AND stg.oc_branch_name IS NULL
                        AND stg.iban IS NOT NULL;

                    dbms_output.put_line('Branch Name 3 combo case 1 is validated');
                END;

	--  Branch Name update 3 combination--
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg stg
                    SET
                        stg.oc_branch_name = (
                            SELECT
                                bm.branch_name
                            FROM
                                xxcnv_supplier_branch_mapping bm
                            WHERE
                                ( bm.bank_name IS NOT NULL
                                  AND bm.bank_city IS NOT NULL
                                  AND bm.routing_number IS NOT NULL
                                  AND stg.attribute2 IS NULL
                                  AND stg.country_code = 'US' )
                                AND ( upper(stg.bank_name)
                                      || upper(stg.attribute1)
                                      || upper(stg.branch_name) ) = ( upper(bm.bank_name)
                                                                      || upper(bm.bank_city)
                                                                      || upper(bm.routing_number) )
                                AND ROWNUM = 1
                        )
                    WHERE
                        ( stg.bank_name IS NOT NULL
                          AND stg.attribute1 IS NOT NULL
                          AND stg.branch_name IS NOT NULL )
                        AND oc_branch_name IS NULL;

                    dbms_output.put_line('Branch Name 3 combo case 2 is validated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg stg
                    SET
                        stg.oc_branch_name = (
                            SELECT
                                bm.branch_name
                            FROM
                                xxcnv_supplier_branch_mapping bm
                            WHERE
                                ( bm.bank_name IS NOT NULL
                                  AND bm.bic_code IS NOT NULL
                                  AND bm.routing_number IS NOT NULL )
                                AND ( upper(stg.bank_name)
                                      || upper(stg.attribute2)
                                      || upper(stg.branch_name) ) = ( upper(bm.bank_name)
                                                                      || upper(bm.bic_code)
                                                                      || upper(bm.routing_number) )
                                AND ROWNUM = 1
                        )
                    WHERE
                        ( stg.bank_name IS NOT NULL
                          AND stg.attribute2 IS NOT NULL
                          AND stg.branch_name IS NOT NULL )
                        AND oc_branch_name IS NULL;

                    dbms_output.put_line('Branch Name 3 combo case 3 is validated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg stg
                    SET
                        stg.oc_branch_name = (
                            SELECT
                                bm.branch_name
                            FROM
                                xxcnv_supplier_branch_mapping bm
                            WHERE
                                ( bm.bank_name IS NOT NULL
                                  AND bm.bic_code IS NOT NULL
                                  AND stg.branch_name IS NULL )
                                AND ( upper(stg.bank_name)
                                      || upper(stg.attribute2) ) = ( upper(bm.bank_name)
                                                                     || upper(bm.bic_code) )
                                AND ROWNUM = 1
                        )
                    WHERE
                        ( stg.bank_name IS NOT NULL
                          AND stg.attribute2 IS NOT NULL )
                        AND stg.oc_branch_name IS NULL
                        AND stg.iban IS NOT NULL;

                    dbms_output.put_line('Branch Name 2 combo case 1 is validated');
                END;
				
		-- start added code to remove bank_city from the US country branch derivation v1.1 --
		
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg stg
                    SET
                        stg.oc_branch_name = (
                            SELECT
                                bm.branch_name
                            FROM
                                xxcnv_supplier_branch_mapping bm
                            WHERE
                                ( bm.bank_name IS NOT NULL
                                  AND bm.routing_number IS NOT NULL
                                  AND stg.attribute2 IS NULL
                                  AND stg.country_code = 'US' )
                                AND ( upper(stg.bank_name)
                                      || upper(stg.branch_name) ) = ( upper(bm.bank_name)
                                                                      || upper(bm.routing_number) )
                                AND ROWNUM = 1
                        )
                    WHERE
                        ( stg.bank_name IS NOT NULL
                          AND stg.branch_name IS NOT NULL )
                        AND oc_branch_name IS NULL;

                    dbms_output.put_line('Branch Name 3 combo case 4 is validated');
                END;
				
		-- end added code to remove bank_city from the US country branch derivation v1.1 --


	-- Branch Number --
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Branch name could not be derived as no matching combination found'
                    WHERE
                        oc_branch_name IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Branch Name is validated after the transformation');
                END;


          -- Check for uniqueness of the concatenation of Import Batch Identifier, Payee Identifier, and Payee Bank Account Identifier
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Combination of Payee Identifier and Payee Bank Account Identifier values are not unique'
                    WHERE
                        ( ( to_char(temp_ext_payee_id)
                            || to_char(temp_ext_bank_acct_id) ) IN (
                            SELECT
                                to_char(temp_ext_payee_id)
                                || to_char(temp_ext_bank_acct_id)
                            FROM
                                xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                            GROUP BY
                                to_char(temp_ext_payee_id)
                                || to_char(temp_ext_bank_acct_id)
                            HAVING
                                COUNT(*) > 1
                        ) )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Uniqueness is validated');
                END;

		  -- check in child --

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Combination of Payee Identifier and Payee Bank Account Identifier values is not present in the child IBY_TEMP_PMT_INSTR_USES.csv file'
                    WHERE
                        ( to_char(temp_ext_payee_id)
                          || to_char(temp_ext_bank_acct_id) ) NOT IN (
                            SELECT
                                ( to_char(temp_ext_payee_id)
                                  || to_char(temp_ext_bank_acct_id) )
                            FROM
                                xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                        )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Child is validated');
                END;

		  -- check in parent --

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Payee Identifier is not present in the parent IBY_TEMP_EXT_PAYEES.csv file'
                    WHERE
                        to_char(temp_ext_payee_id) NOT IN (
                            SELECT
                                to_char(temp_ext_payee_id)
                            FROM
                                xxcnv_ap_c004_iby_temp_ext_payees_stg
                        )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Parent is validated');
                END;

		  --Erroring out the record in child table as it errored out in parent table
                BEGIN
              -- Update the import_status in xxcnv_ap_c004_iby_temp_ext_bank_accts_stg to 'ERROR' where the concatenated feeder_import_batch_id and temp_ext_payee_id in xxcnv_ap_c004_iby_temp_ext_payees_stg has import_status 'ERROR'
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Parent Record failed in validation'
                    WHERE
                        to_char(temp_ext_payee_id) IN (
                            SELECT
                                to_char(temp_ext_payee_id)
                            FROM
                                xxcnv_ap_c004_iby_temp_ext_payees_stg
                            WHERE
                                import_status = 'ERROR'
                        )
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Parent failed check');
                END;


		  --Validate Bank Name with comma's
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        bank_name = '"'
                                    || bank_name
                                    || '"'
                    WHERE
                        bank_name LIKE '%,%'
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Bank Name With Comma is validated');
                END;

		  -- Update import_status based on error_message
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        import_status =
                            CASE
                                WHEN error_message IS NOT NULL THEN
                                    'ERROR'
                                ELSE
                                    'PROCESSED'
                            END
                    WHERE
                        execution_id = gv_execution_id;

                    dbms_output.put_line('import_status is validated');
                END;

		  -- Final update to set error_message and import_status
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = ltrim(error_message, ','),
                        import_status =
                            CASE
                                WHEN error_message IS NOT NULL THEN
                                    'ERROR'
                                ELSE
                                    'PROCESSED'
                            END
			  --where execution_id = gv_execution_id
                            ;

                    dbms_output.put_line('import_status column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        file_name = gv_oci_file_name_bank_accts
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('file_name column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        source_system = gv_conversion_id,
                        bank_account_type = 'CHECKING',
                        foreign_payment_use_flag = 'Y'
                    WHERE
                        file_reference_identifier IS NULL;

                    dbms_output.put_line('source_system is updated');
                END;

          -- Check if there are any error messages
                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                WHERE
                    error_message IS NOT NULL
                    AND file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    error_message IS NOT NULL
                    AND file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_reference_identifier column is updated');
                UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success
                WHERE
                    error_message IS NULL
                    AND file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_reference_identifier column is updated');
                IF lv_error_count > 0 THEN

			 -- Logging the message If data is not validated
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_failed_validation,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_bank_accts,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

  -- Logging the message
                IF
                    lv_error_count = 0
                    AND gv_oci_file_name_bank_accts IS NOT NULL
                THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_validated,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_bank_accts,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;
      --COMMIT;
                IF gv_oci_file_name_bank_accts IS NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_file_not_found,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_bank_accts,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

            ELSE
                dbms_output.put_line('No Data is found in interface tables. Data is not loaded from ext to stg ');
            END IF;

        END;

 --3rd VALIDATION 

        BEGIN
            BEGIN
                UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                SET
                    execution_id = gv_execution_id,
                    feeder_import_batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

                dbms_output.put_line('execution_id is updated');
            END;

            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
            WHERE
                execution_id = gv_execution_id;

            IF lv_row_count <> 0 THEN 

		  -- Initialize ERROR_MESSAGE to an empty string if it is NULL
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        error_message = ''
                    WHERE
                        error_message IS NULL;

                END;



          -- Validate Payee Identifier
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        error_message = error_message || '|Payee Identifier should not be null'
                    WHERE
                        temp_ext_payee_id IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

          -- Validate Payee Bank Account Identifier
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        error_message = error_message || '|Payee Bank Account Identifier should not be null'
                    WHERE
                        temp_ext_bank_acct_id IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	      -- Validate Payee Bank Account Assignment Identifier
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        error_message = error_message || '|Payee Account Assignment Identifier should not be null'
                    WHERE
                        temp_pmt_instr_use_id IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        primary_flag = 'Y';

                    dbms_output.put_line('Primary Flag is updated');
                END;

		  		  --Erroring out the record in child table as it errored out in parent table
                BEGIN
              -- Update the import_status in xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg to 'ERROR' where the concatenated feeder_import_batch_id and temp_ext_payee_id in xxcnv_ap_c004_iby_temp_ext_payees_stg has import_status 'ERROR'
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        error_message = error_message || '|Parent Record failed in validation'
                    WHERE
                        ( to_char(temp_ext_payee_id)
                          || to_char(temp_ext_bank_acct_id) ) IN (
                            SELECT
                                ( to_char(temp_ext_payee_id)
                                  || to_char(temp_ext_bank_acct_id) )
                            FROM
                                xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                            WHERE
                                import_status = 'ERROR'
                        )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;


          -- Check for uniqueness of the concatenation of Import Batch Identifier, Payee Identifier, Payee Bank Account Identifier and Payee Bank Account Assignment Identifier
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        error_message = error_message || '|Combination of Payee Identifier, Payee Bank Account Identifier and Payee Bank Account Assignment Identifier values are not unique'
                    WHERE
                        ( ( to_char(temp_ext_payee_id)
                            || to_char(temp_ext_bank_acct_id)
                            || to_char(temp_pmt_instr_use_id) ) IN (
                            SELECT
                                to_char(temp_ext_payee_id)
                                || to_char(temp_ext_bank_acct_id)
                                || to_char(temp_pmt_instr_use_id)
                            FROM
                                xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                            GROUP BY (
                                to_char(temp_ext_payee_id)
                                || to_char(temp_ext_bank_acct_id)
                                || to_char(temp_pmt_instr_use_id)
                            )
                            HAVING
                                COUNT(*) > 1
                        ) )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

          -- Check for uniqueness of the concatenation of Import Batch Identifier, Payee Identifier, Payee Bank Account Identifier and Payee Bank Account Assignment Identifier
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        error_message = error_message || '|Combination of Payee Identifier, Payee Bank Account Identifier not present in the Parent IBY_TEMP_EXT_BANK_ACCTS.csv file'
                    WHERE
                        ( ( to_char(temp_ext_payee_id)
                            || to_char(temp_ext_bank_acct_id) ) NOT IN (
                            SELECT
                                to_char(temp_ext_payee_id)
                                || to_char(temp_ext_bank_acct_id)
                            FROM
                                xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                        ) )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    SET
                        error_message = error_message || '|Child record failed in validation',
                        import_status = 'ERROR',
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                        ( to_char(temp_ext_payee_id)
                          || to_char(temp_ext_bank_acct_id) ) IN (
                            SELECT
                                ( to_char(temp_ext_payee_id)
                                  || to_char(temp_ext_bank_acct_id) )
                            FROM
                                xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                            WHERE
                                error_message IS NOT NULL
                        )
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_ext_payees_stg
                    SET
                        error_message = error_message || '|Child record failed in validation',
                        import_status = 'ERROR',
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                        ( temp_ext_payee_id ) IN (
                            SELECT
                                temp_ext_payee_id
                            FROM
                                xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                            WHERE
                                error_message IS NOT NULL
                        )
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Child 1 is validated');
                END;


		 -- Update import_status based on error_message
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        import_status =
                            CASE
                                WHEN error_message IS NOT NULL THEN
                                    'ERROR'
                                ELSE
                                    'PROCESSED'
                            END
                    WHERE
                        execution_id = gv_execution_id;

                    dbms_output.put_line('import_status is validated');
                END;

		  -- Final update to set error_message and import_status
                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        error_message = ltrim(error_message, ','),
                        import_status =
                            CASE
                                WHEN error_message IS NOT NULL THEN
                                    'ERROR'
                                ELSE
                                    'PROCESSED'
                            END
                    WHERE
                        execution_id = gv_execution_id;

                    dbms_output.put_line('import_status column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        file_name = gv_oci_file_name_pmt_instr
                    WHERE
                            execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('file_name column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    SET
                        source_system = gv_conversion_id
                    WHERE
                        file_reference_identifier IS NULL;

                    dbms_output.put_line('source_system is updated');
                END;

          -- Check if there are any error messages
                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                WHERE
                    error_message IS NOT NULL
                    AND file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    file_reference_identifier IS NULL
                    AND error_message IS NOT NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_reference_identifier column is updated');
                UPDATE xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success
                WHERE
                    error_message IS NULL
                    AND file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_reference_identifier column is updated');
                IF lv_error_count > 0 THEN
            -- Logging the message If data is not validated
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_failed,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_pmt_instr,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

  -- Logging the message
                IF
                    lv_error_count = 0
                    AND gv_oci_file_name_pmt_instr IS NOT NULL
                THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_validated,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_pmt_instr,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;
  --COMMIT;
                IF gv_oci_file_name_pmt_instr IS NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_file_not_found,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_pmt_instr,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

            ELSE
                dbms_output.put_line('No Data is found in interface tables. Data is not loaded from ext to stg ');
            END IF;

        END;
	--commit;
    END data_validations_prc;

/*==============================================================================================================================
-- PROCEDURE : create_fbdi_file_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the FBDI CSV file after all validations.
================================================================================================================================= */
    PROCEDURE create_fbdi_file_prc IS

        CURSOR batch_id_cursor_payee IS
        SELECT DISTINCT
            feeder_import_batch_id
        FROM
            xxcnv_ap_c004_iby_temp_ext_payees_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_success;

        CURSOR batch_id_cursor_bank_accts IS
        SELECT DISTINCT
            feeder_import_batch_id
        FROM
            xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_success;

        CURSOR batch_id_cursor_pmt_instr IS
        SELECT DISTINCT
            feeder_import_batch_id
        FROM
            xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_success;

        lv_success_count NUMBER;
        lv_batch_id      NUMBER;
    BEGIN
        BEGIN
            FOR g_id IN batch_id_cursor_payee LOOP
                lv_batch_id := g_id.feeder_import_batch_id;
                dbms_output.put_line('In create FBDI Processing Batch_ID: ' || lv_batch_id);
                BEGIN
                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                    SELECT
                        COUNT(*)
                    INTO lv_success_count
                    FROM
                        xxcnv_ap_c004_iby_temp_ext_payees_stg
                    WHERE
                            feeder_import_batch_id = lv_batch_id
                        AND file_reference_identifier = gv_execution_id
                                                        || '_'
                                                        || gv_status_success;

                    dbms_output.put_line('Success record count for batch_id '
                                         || lv_batch_id
                                         || '|: '
                                         || lv_success_count);
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for xxcnv_ap_c004_iby_temp_ext_payees_stg  batch_id: ' || lv_batch_id);
                        RETURN; --
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error checking success record count for xxcnv_ap_c004_iby_temp_ext_payees_stg  batch_id '
                                             || lv_batch_id
                                             || '|: '
                                             || sqlerrm);
                        RETURN; --
                END;

                IF lv_success_count > 0 THEN
                    BEGIN
                        dbms_cloud.export_data(
                            credential_name => gv_credential_name,
                            file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                             || '|/'
                                             || lv_batch_id
                                             || gv_oci_file_name_payee,
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv',
                                        'trimspaces' VALUE 'rtrim',
                                        'header' VALUE FALSE
                                    ),
                            query           => 'SELECT 
                                            FEEDER_IMPORT_BATCH_ID,          
                                            TEMP_EXT_PAYEE_ID,				
                                            BUSINESS_UNIT,    				
                                            OC_VENDOR_NUM,          			
                                            UPPER(VENDOR_SITE_CODE),     			
                                            EXCLUSIVE_PAYMENT_FLAG,  		
                                            DEFAULT_PAYMENT_METHOD_CODE, 	
                                            DELIVERY_CHANNEL_CODE,			
                                            SETTLEMENT_PRIORITY,             
                                            REMIT_ADVICE_DELIVERY_METHOD,	
                                            REMIT_ADVICE_EMAIL,				
                                            REMIT_ADVICE_FAX, 				
                                            BANK_INSTRUCTION1_CODE, 			
                                            BANK_INSTRUCTION2_CODE, 			
                                            BANK_INSTRUCTION_DETAILS,		
                                            PAYMENT_REASON_CODE, 			
                                            PAYMENT_REASON_COMMENTS,		 	
                                            PAYMENT_TEXT_MESSAGE1, 			
                                            PAYMENT_TEXT_MESSAGE2, 			
                                            PAYMENT_TEXT_MESSAGE3, 			
                                            BANK_CHARGE_BEARER
                                            FROM xxcnv_ap_c004_iby_temp_ext_payees_stg
											where 1=1
										  and import_status = '''
                                     || 'PROCESSED'
                                     || '''
											and FEEDER_IMPORT_BATCH_ID ='''
                                     || lv_batch_id
                                     || '''
											AND file_reference_identifier= '''
                                     || gv_execution_id
                                     || '_'
                                     || gv_status_success
                                     || ''''
                        );

                        dbms_output.put_line('CSV file for BATCH_ID '
                                             || lv_batch_id
                                             || '| exported successfully to OCI Object Storage.');
                        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                            p_conversion_id     => gv_conversion_id,
                            p_execution_id      => gv_execution_id,
                            p_execution_step    => gv_fbdi_export_status,
                            p_boundary_system   => gv_boundary_system,
                            p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                            p_file_name         => lv_batch_id
                                           || '|_'
                                           || gv_oci_file_name_payee,
                            p_attribute1        => lv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );

                    EXCEPTION
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error exporting data to CSV for  xxcnv_ap_c004_iby_temp_ext_payees_stg BATCH_ID '
                                                 || lv_batch_id
                                                 || '|: '
                                                 || sqlerrm);
                            RETURN;
                    END;
                ELSE
                    dbms_output.put_line('Process Stopped for xxcnv_ap_c004_iby_temp_ext_payees_stg batch_id '
                                         || lv_batch_id
                                         || '|: Error message columns contain data.');
                    RETURN;
                END IF;

            END LOOP;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '|->'
                                     || dbms_utility.format_error_backtrace);

                RETURN;
        END;

--table 2

        BEGIN
            lv_success_count := 0;
            FOR g_id IN batch_id_cursor_bank_accts LOOP
                lv_batch_id := g_id.feeder_import_batch_id;
                dbms_output.put_line('Processing Batch_ID: ' || lv_batch_id);
                BEGIN
                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                    SELECT
                        COUNT(*)
                    INTO lv_success_count
                    FROM
                        xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                    WHERE
                            feeder_import_batch_id = lv_batch_id
                        AND file_reference_identifier = gv_execution_id
                                                        || '_'
                                                        || gv_status_success;

                    dbms_output.put_line('Success record count for xxcnv_ap_c004_iby_temp_ext_bank_accts_stg batch_id '
                                         || lv_batch_id
                                         || '|: '
                                         || lv_success_count);
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for xxcnv_ap_c004_iby_temp_ext_bank_accts_stg batch_id: ' || lv_batch_id)
                        ;
                        RETURN;
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error checking success record for batch_id '
                                             || lv_batch_id
                                             || '|: '
                                             || sqlerrm);
                        RETURN;
                END;

                IF lv_success_count > 0 THEN
                    BEGIN
                        dbms_cloud.export_data(
                            credential_name => gv_credential_name,
                            file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                             || '|/'
                                             || lv_batch_id
                                             || gv_oci_file_name_bank_accts,
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv'/*,'dateformat' VALUE 'yyyy/mm/dd', 'trimspaces' VALUE 'rtrim'*/,
                                        'header' VALUE FALSE
                                    ),
                            query           => 'SELECT 
                                            FEEDER_IMPORT_BATCH_ID,      
						                    TEMP_EXT_PAYEE_ID,          
						                    TEMP_EXT_BANK_ACCT_ID,       
						                    BANK_NAME,                   
						                    OC_BRANCH_NAME,                 
						                    COUNTRY_CODE,                
						                    BANK_ACCOUNT_NAME,           
						                    BANK_ACCOUNT_NUMBER,         
						                    CURRENCY_CODE,               
						                    FOREIGN_PAYMENT_USE_FLAG, 
                                            TO_CHAR(START_DATE, ''YYYY/MM/DD'') AS START_DATE,
                                            TO_CHAR(END_DATE, ''YYYY/MM/DD'') AS END_DATE,                     IBAN,                        
						                    CHECK_DIGITS,                
						                    BANK_ACCOUNT_NAME_ALT,       
						                    BANK_ACCOUNT_TYPE,           
						                    ACCOUNT_SUFFIX,              
						                    DESCRIPTION,                 
						                    AGENCY_LOCATION_CODE,        
						                    EXCHANGE_RATE_AGREEMENT_NUM, 
						                    EXCHANGE_RATE_AGREEMENT_TYPE,
						                    EXCHANGE_RATE,               
						                    SECONDARY_ACCOUNT_REFERENCE, 
						                    ATTRIBUTE_CATEGORY,          
						                    ATTRIBUTE1,                  
						                    ATTRIBUTE2,                  
						                    ATTRIBUTE3,                  
						                    ATTRIBUTE4,                  
						                    ATTRIBUTE5,                  
						                    ATTRIBUTE6,                  
						                    ATTRIBUTE7,                  
						                    ATTRIBUTE8,                  
						                    ATTRIBUTE9,                  
						                    ATTRIBUTE10,                 
						                    ATTRIBUTE11,                 
						                    ATTRIBUTE12,                 
						                    ATTRIBUTE13,                 
						                    ATTRIBUTE14,                 
						                    ATTRIBUTE15
                                            FROM xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
										    WHERE 1=1
											and import_status = '''
                                     || 'PROCESSED'
                                     || '''
											and FEEDER_IMPORT_BATCH_ID ='''
                                     || lv_batch_id
                                     || '''
											AND file_reference_identifier= '''
                                     || gv_execution_id
                                     || '_'
                                     || gv_status_success
                                     || ''''
                        );

                        dbms_output.put_line('CSV file for BATCH_ID '
                                             || lv_batch_id
                                             || '| exported successfully to OCI Object Storage.');
                        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                            p_conversion_id     => gv_conversion_id,
                            p_execution_id      => gv_execution_id,
                            p_execution_step    => gv_fbdi_export_status,
                            p_boundary_system   => gv_boundary_system,
                            p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                            p_file_name         => lv_batch_id
                                           || '|_'
                                           || gv_oci_file_name_bank_accts,
                            p_attribute1        => lv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );

                    EXCEPTION
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c004_iby_temp_ext_bank_accts_stg batch_id '
                                                 || lv_batch_id
                                                 || '|: '
                                                 || sqlerrm);
                            RETURN;
                    END;
                ELSE
                    dbms_output.put_line('Process Stopped for xxcnv_ap_c004_iby_temp_ext_bank_accts_stg batch_id '
                                         || lv_batch_id
                                         || '|: Error message columns contain data.');
                    RETURN;
                END IF;

            END LOOP;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
                RETURN;
        END;

--table3

        BEGIN
            lv_success_count := 0;
            FOR g_id IN batch_id_cursor_pmt_instr LOOP
                lv_batch_id := g_id.feeder_import_batch_id;
                dbms_output.put_line('Processing Batch_ID: ' || lv_batch_id);
                BEGIN
                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                    SELECT
                        COUNT(*)
                    INTO lv_success_count
                    FROM
                        xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                    WHERE
                            feeder_import_batch_id = lv_batch_id
                        AND file_reference_identifier = gv_execution_id
                                                        || '_'
                                                        || gv_status_success;

                    dbms_output.put_line('Success record count for xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg batch_id '
                                         || lv_batch_id
                                         || '|: '
                                         || lv_success_count);
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg batch_id: ' || lv_batch_id)
                        ;
                        RETURN;
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error checking success record count for batch_id '
                                             || lv_batch_id
                                             || '|: '
                                             || sqlerrm);
                        RETURN;
                END;

                IF lv_success_count > 0 THEN
                    BEGIN
                        dbms_cloud.export_data(
                            credential_name => gv_credential_name,
                            file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                             || '|/'
                                             || lv_batch_id
                                             || gv_oci_file_name_pmt_instr,
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv'/*,'dateformat' VALUE 'yyyy/mm/dd', 'trimspaces' VALUE 'rtrim'*/,
                                        'header' VALUE FALSE
                                    ),
                            query           => 'SELECT 
                                            FEEDER_IMPORT_BATCH_ID,  
                                            TEMP_EXT_PAYEE_ID,        
                                            TEMP_EXT_BANK_ACCT_ID,   
                                            TEMP_PMT_INSTR_USE_ID,   
                                            PRIMARY_FLAG,            
                                            TO_CHAR(START_DATE, ''YYYY/MM/DD'') AS START_DATE,
                                            TO_CHAR(END_DATE, ''YYYY/MM/DD'') AS END_DATE             
                                            FROM xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
											 WHERE 1=1
											and import_status = '''
                                     || 'PROCESSED'
                                     || '''
											and FEEDER_IMPORT_BATCH_ID ='''
                                     || lv_batch_id
                                     || '''
											AND file_reference_identifier= '''
                                     || gv_execution_id
                                     || '_'
                                     || gv_status_success
                                     || ''''
                        );

                        dbms_output.put_line('CSV file for BATCH_ID '
                                             || lv_batch_id
                                             || '| exported successfully to OCI Object Storage.');
                        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                            p_conversion_id     => gv_conversion_id,
                            p_execution_id      => gv_execution_id,
                            p_execution_step    => gv_fbdi_export_status,
                            p_boundary_system   => gv_boundary_system,
                            p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                            p_file_name         => lv_batch_id
                                           || '|_'
                                           || gv_oci_file_name_pmt_instr,
                            p_attribute1        => lv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );

                    EXCEPTION
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error exporting data to CSV for IBY_TEMP_PMT_INSTR_USES_STG batch_id '
                                                 || lv_batch_id
                                                 || '|: '
                                                 || sqlerrm);
                            RETURN;
                    END;
                ELSE
                    dbms_output.put_line('Process Stopped for xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg batch_id '
                                         || lv_batch_id
                                         || '|: Error message columns contain data.');
                    RETURN;
                END IF;

            END LOOP;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
                RETURN;
        END;

    END create_fbdi_file_prc;

/*==============================================================================================================================
-- PROCEDURE : create_atp_validation_recon_report_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating properties file.
================================================================================================================================= */

    PROCEDURE create_atp_validation_recon_report_prc IS

        CURSOR batch_id_payee_recon IS
        SELECT DISTINCT
            feeder_import_batch_id
        FROM
            xxcnv_ap_c004_iby_temp_ext_payees_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_bank_accts_recon IS
        SELECT DISTINCT
            feeder_import_batch_id
        FROM
            xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_pmt_instr_recon IS
        SELECT DISTINCT
            feeder_import_batch_id
        FROM
            xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        lv_batch_id NUMBER;
    BEGIN
        BEGIN
            FOR g_id IN batch_id_payee_recon LOOP
                lv_batch_id := g_id.feeder_import_batch_id;
                dbms_output.put_line('Processing RECON REPORT FOR BATCH_ID: '
                                     || lv_batch_id
                                     || '|_'
                                     || gv_oci_file_path
                                     || '|_'
                                     || gv_source_folder
                                     || '|_'
                                     || gv_recon_folder);

                BEGIN
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                         || '/'
                                         || lv_batch_id
                                         || 'ATP_Recon_Iby_Temp_Ext_Payees'
                                         || sysdate,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'header' VALUE TRUE
                                ),
                        query           => 'SELECT 
                                                FEEDER_IMPORT_BATCH_ID,
                                                TEMP_EXT_PAYEE_ID,
                                                BUSINESS_UNIT,
                                                VENDOR_NUM,
                                                UPPER(VENDOR_SITE_CODE),
                                                EXCLUSIVE_PAYMENT_FLAG,
                                                DEFAULT_PAYMENT_METHOD_CODE,
                                                DELIVERY_CHANNEL_CODE,
                                                SETTLEMENT_PRIORITY,
                                                REMIT_ADVICE_DELIVERY_METHOD,
                                                REMIT_ADVICE_EMAIL,
                                                REMIT_ADVICE_FAX,
                                                BANK_INSTRUCTION1_CODE,
                                                BANK_INSTRUCTION2_CODE,
                                                BANK_INSTRUCTION_DETAILS,
                                                PAYMENT_REASON_CODE,
                                                PAYMENT_REASON_COMMENTS,
                                                PAYMENT_TEXT_MESSAGE1,
                                                PAYMENT_TEXT_MESSAGE2,
                                                PAYMENT_TEXT_MESSAGE3,
                                                BANK_CHARGE_BEARER,
                                                file_name,
                                                error_message,
                                                import_status,
                                                source_system
                                            FROM xxcnv_ap_c004_iby_temp_ext_payees_stg
                                            where 1=1
											and import_status = '''
                                 || 'ERROR'
                                 || '''
											and FEEDER_IMPORT_BATCH_ID ='''
                                 || lv_batch_id
                                 || '''
											AND file_reference_identifier= '''
                                 || gv_execution_id
                                 || '_'
                                 || gv_status_failure
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for BATCH_ID '
                                         || lv_batch_id
                                         || '| exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_recon_report,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder),
                        p_file_name         => lv_batch_id
                                       || '|_'
                                       || gv_oci_file_name_payee,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for batch_id '
                                             || lv_batch_id
                                             || '|: '
                                             || sqlerrm);
                END;

            END LOOP;
        END;

--table 2

        BEGIN
            FOR g_id IN batch_id_bank_accts_recon LOOP
                lv_batch_id := g_id.feeder_import_batch_id;
                BEGIN
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                         || '/'
                                         || lv_batch_id
                                         || 'ATP_Recon_iby_temp_ext_bank_accts'
                                         || sysdate,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'header' VALUE TRUE
                                ),
                        query           => 'SELECT 
                                                FEEDER_IMPORT_BATCH_ID,
                                                TEMP_EXT_PAYEE_ID,
                                                TEMP_EXT_BANK_ACCT_ID,
                                                BANK_NAME,
                                                BRANCH_NAME,
                                                COUNTRY_CODE,
                                                BANK_ACCOUNT_NAME,
                                                BANK_ACCOUNT_NUMBER,
                                                CURRENCY_CODE,
                                                FOREIGN_PAYMENT_USE_FLAG,
                                                TO_CHAR(START_DATE, ''YYYY/MM/DD'') AS START_DATE,
                                                TO_CHAR(END_DATE, ''YYYY/MM/DD'') AS END_DATE,
                                                IBAN,
                                                CHECK_DIGITS,
                                                BANK_ACCOUNT_NAME_ALT,
                                                BANK_ACCOUNT_TYPE,
                                                ACCOUNT_SUFFIX,
                                                DESCRIPTION,
                                                AGENCY_LOCATION_CODE,
                                                EXCHANGE_RATE_AGREEMENT_NUM,
                                                EXCHANGE_RATE_AGREEMENT_TYPE,
                                                EXCHANGE_RATE,
                                                SECONDARY_ACCOUNT_REFERENCE,
                                                ATTRIBUTE_CATEGORY,
                                                ATTRIBUTE1,
                                                ATTRIBUTE2,
                                                ATTRIBUTE3,
                                                ATTRIBUTE4,
                                                ATTRIBUTE5,
                                                ATTRIBUTE6,
                                                ATTRIBUTE7,
                                                ATTRIBUTE8,
                                                ATTRIBUTE9,
                                                ATTRIBUTE10,
                                                ATTRIBUTE11,
                                                ATTRIBUTE12,
                                                ATTRIBUTE13,
                                                ATTRIBUTE14,
                                                ATTRIBUTE15,
                                                file_name,
                                                error_message,
                                                import_status,
                                                source_system
                                            FROM xxcnv_ap_c004_iby_temp_ext_bank_accts_stg
                                            where 1=1
											and import_status = '''
                                 || 'ERROR'
                                 || '''
											and FEEDER_IMPORT_BATCH_ID ='''
                                 || lv_batch_id
                                 || '''
											AND file_reference_identifier= '''
                                 || gv_execution_id
                                 || '_'
                                 || gv_status_failure
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for BATCH_ID '
                                         || lv_batch_id
                                         || '| exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_recon_report,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder),
                        p_file_name         => lv_batch_id
                                       || '|_'
                                       || gv_oci_file_name_bank_accts,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for batch_id '
                                             || lv_batch_id
                                             || '|: '
                                             || sqlerrm);
                END;

            END LOOP;

        END;

--table3

        BEGIN
            FOR g_id IN batch_id_pmt_instr_recon LOOP
                lv_batch_id := g_id.feeder_import_batch_id;
                BEGIN
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                         || '/'
                                         || lv_batch_id
                                         || 'ATP_Recon_iby_temp_pmt_instr_uses'
                                         || sysdate,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'header' VALUE TRUE
                                ),
                        query           => '
                                       SELECT 
                                           FEEDER_IMPORT_BATCH_ID,
                                           TEMP_EXT_PAYEE_ID,
                                           TEMP_EXT_BANK_ACCT_ID,
                                           TEMP_PMT_INSTR_USE_ID,
                                           PRIMARY_FLAG,
                                           TO_CHAR(START_DATE, ''YYYY/MM/DD'') AS START_DATE,
                                           TO_CHAR(END_DATE, ''YYYY/MM/DD'') AS END_DATE,
                                           file_name,
                                           error_message,
                                           import_status,
                                           source_system
                                           FROM xxcnv_ap_c004_iby_temp_pmt_instr_uses_stg
                                            where 1=1
											and import_status = '''
                                 || 'ERROR'
                                 || '''
											and FEEDER_IMPORT_BATCH_ID ='''
                                 || lv_batch_id
                                 || '''
											AND file_reference_identifier= '''
                                 || gv_execution_id
                                 || '_'
                                 || gv_status_failure
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for BATCH_ID '
                                         || lv_batch_id
                                         || '| exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_recon_report,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder),
                        p_file_name         => lv_batch_id
                                       || '|_'
                                       || gv_oci_file_name_pmt_instr,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for batch_id '
                                             || lv_batch_id
                                             || '|: '
                                             || sqlerrm);
                END;

            END LOOP;
        END;

    END create_atp_validation_recon_report_prc;

END xxcnv_ap_c004_supplier_banks_conversion_pkg;