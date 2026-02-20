create or replace PACKAGE BODY xxcnv.xxcnv_ap_c006_employee_as_supplier_conversion_pkg IS

    /*************************************************************************************
    NAME              :     Employee Supplier Conversion
    PURPOSE           :     This package is the detailed body of all the procedures.
    -- Modification History
    -- Developer          Date         Version     Comments and changes made
    -- -------------   ------       ----------  -----------------------------------------
    -- Satya Pavani   27-Mar-2025        2.0         Initial Development
    ****************************************************************************************/

---Declaring global Variables
    gv_data_validated_success        CONSTANT VARCHAR2(50) := 'Data_Validated';
    gv_data_validated_failure        CONSTANT VARCHAR2(50) := 'Data_Not_Validated';
    gv_import_status                 VARCHAR2(256) := NULL;
    gv_error_message                 VARCHAR2(500) := NULL;
    gv_file_name                     VARCHAR2(256) := NULL;
    gv_oci_file_path                 VARCHAR2(600) := NULL;
    gv_oci_file_name                 VARCHAR2(2000) := NULL;
    gv_execution_id                  VARCHAR2(300) := NULL;
    gv_group_id                      NUMBER(18) := NULL;
    gv_batch_id                      VARCHAR2(30) := NULL;
    gv_credential_name               CONSTANT VARCHAR2(30) := 'OCI$RESOURCE_PRINCIPAL';
    gv_status_success                CONSTANT VARCHAR2(15) := 'Success';
    gv_status_failure                CONSTANT VARCHAR2(15) := 'Failure';
    gv_conversion_id                 VARCHAR2(15) := NULL;
    gv_boundary_system               VARCHAR2(25) := NULL;
    gv_status_picked                 CONSTANT VARCHAR2(50) := 'File_Picked_From_OCI_AND_Loaded_To_Stg';
    gv_status_picked_for_tr          CONSTANT VARCHAR2(50) := 'Transformed_Data_From_Ext_To_Stg';
    gv_status_validated              CONSTANT VARCHAR2(50) := 'Validated';
    gv_status_failed                 CONSTANT VARCHAR2(50) := 'Failed_At_Validation';
    gv_coa_transformation            CONSTANT VARCHAR2(50) := 'COA_Transformation';
    gv_fbdi_export_status            CONSTANT VARCHAR2(50) := 'Exported_To_Fbdi';
    gv_status_staged                 CONSTANT VARCHAR2(50) := 'Staged_For_Import';
    gv_transformed_folder            CONSTANT VARCHAR2(50) := 'Transformed_FBDI_Files';
    gv_source_folder                 CONSTANT VARCHAR2(50) := 'Source_FBDI_Files';
    gv_properties                    CONSTANT VARCHAR2(50) := 'Properties';
    gv_file_picked                   VARCHAR2(50) := 'File_Picked_From_OCI_Server';
    gv_status_failed_validation      CONSTANT VARCHAR2(50) := 'Failed_Validation';
    gv_fbdi_export_fail              CONSTANT VARCHAR2(50) := 'Failed_In_FBDI';
    gv_properties_fail               CONSTANT VARCHAR2(50) := 'Failed_In_Properties';
    gv_recon_folder                  CONSTANT VARCHAR2(50) := 'ATP_Validation_Error_Files';
    gv_recon_report                  CONSTANT VARCHAR2(50) := 'Recon_Report_Created';
    gv_oci_file_name_suppheader      VARCHAR2(100) := NULL;
    gv_oci_file_name_suppaddress     VARCHAR2(100) := NULL;
    gv_oci_file_name_suppsites       VARCHAR2(100) := NULL;
    gv_oci_file_name_suppsitesassign VARCHAR2(100) := NULL;

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
        dbms_output.put_line('conversion_id: '
                             || gv_conversion_id
                             || 'exec '
                             || gv_execution_id
                             || 'Boundary'
                             || gv_boundary_system);

        BEGIN
            BEGIN
                SELECT
                    ce.execution_id,
                    ce.file_name,
                    ce.file_path
                INTO
                    gv_execution_id,
                    gv_oci_file_name,
                    gv_oci_file_path
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

                -- Initialize loop variables
                lv_start_pos := 1;

				-- Split the concatenated file names AND assign to global variables
                LOOP
                    lv_end_pos := instr(gv_oci_file_name, '.csv', lv_start_pos) + 3;
                    EXIT WHEN lv_end_pos = 3; -- Exit loop if no more '.csv' found

                    lv_file_name := substr(gv_oci_file_name, lv_start_pos, lv_end_pos - lv_start_pos + 1);
                    dbms_output.put_line('Processing file name: ' || lv_file_name); -- Debugging output

                    CASE
                        WHEN lv_file_name LIKE '%PozSuppliersInt%.csv' THEN
                            gv_oci_file_name_suppheader := lv_file_name;
                        WHEN lv_file_name LIKE '%PozSupplierAddressesInt%.csv' THEN
                            gv_oci_file_name_suppaddress := lv_file_name;
                        WHEN lv_file_name LIKE '%PozSupplierSitesInt%.csv' THEN
                            gv_oci_file_name_suppsites := lv_file_name;
                        WHEN lv_file_name LIKE '%PozSiteAssignmentsInt%.csv' THEN
                            gv_oci_file_name_suppsitesassign := lv_file_name;
                        ELSE
                            dbms_output.put_line('No match found for file name: ' || lv_file_name); -- Debugging output
                    END CASE;

                    lv_start_pos := lv_end_pos + 1;
                END LOOP;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error fetching execution details: ' || sqlerrm);
                    RETURN;
            END;

            dbms_output.put_line('execution_id: ' || gv_execution_id);
            dbms_output.put_line('file_name: ' || gv_oci_file_name);
            dbms_output.put_line('file_path: ' || gv_oci_file_path);
        END;

    -- Call to import data from OCI to external table
        BEGIN
            import_data_from_oci_to_stg_prc(p_loading_status);
            IF p_loading_status = gv_status_failure THEN
                dbms_output.put_line('Error in import_data_from_oci_to_stg_prc');
                RETURN;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling import_data_from_oci_to_stg_prc: ' || sqlerrm);
                RETURN;
        END;


    -- Call to perform data and business validations in staging table
        BEGIN
            data_validations_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling data_validations_prc: ' || sqlerrm);
                RETURN;
        END;

    -- Call to create a CSV file from staging tables after all validations
        BEGIN
            create_fbdi_file_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_fbdi_file_prc: ' || sqlerrm);
                RETURN;
        END;

    ---create atp recon report
        BEGIN
            create_recon_report_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_recon_report_prc procedure '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);
        END;

    END main_prc;

/*=================================================================================================================
-- PROCEDURE : IMPORT_DATA_FROM_OCI_TO_EXT_PRC
-- PARAMETERS: p_loading_status
-- COMMENT   : This procedure is used to create an external table AND transfer that data from external to stg table.
===================================================================================================================*/

    PROCEDURE import_data_from_oci_to_stg_prc (
        p_loading_status OUT VARCHAR2
    ) IS
        lv_table_count NUMBER := 0;
        lv_row_count   NUMBER := 0;
    BEGIN
        BEGIN
        -- Check if the external table exists AND drop it if it does
            SELECT
                COUNT(*)
            INTO lv_table_count
            FROM
                all_objects
            WHERE
                    upper(object_name) = 'XXCNV_AP_C006_POZ_SUPPLIERS_EXT'
                AND object_type = 'TABLE';

            IF lv_table_count > 0 THEN
                EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c006_poz_suppliers_ext';
                EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c006_poz_suppliers_stg';
                dbms_output.put_line('Table xxcnv_ap_c006_poz_suppliers_ext dropped');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error dropping table xxcnv_ap_c006_poz_suppliers_ext:'
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                p_loading_status := gv_status_failure;
        END;

 --table 2	

        BEGIN
            lv_table_count := 0;
            SELECT
                COUNT(*)
            INTO lv_table_count
            FROM
                all_objects
            WHERE
                    upper(object_name) = 'XXCNV_AP_C006_POZ_SUPPLIER_ADDRESSES_EXT'
                AND object_type = 'TABLE';

            IF lv_table_count > 0 THEN
                EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c006_poz_supplier_addresses_ext';
                EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c006_poz_supplier_addresses_stg';
                dbms_output.put_line('Table xxcnv_ap_c006_poz_supplier_addresses_ext dropped');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error dropping table xxcnv_ap_c006_poz_supplier_addresses_ext: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                p_loading_status := gv_status_failure;
        END;

--table3

        BEGIN
            lv_table_count := 0;
            SELECT
                COUNT(*)
            INTO lv_table_count
            FROM
                all_objects
            WHERE
                    upper(object_name) = 'XXCNV_AP_C006_POZ_SUPPLIER_SITES_EXT'
                AND object_type = 'TABLE';

            IF lv_table_count > 0 THEN
                EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c006_poz_supplier_sites_ext';
                EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c006_poz_supplier_sites_stg';
                dbms_output.put_line('Table xxcnv_ap_c006_poz_supplier_sites_ext dropped');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error dropping table xxcnv_ap_c006_poz_supplier_sites_ext: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                p_loading_status := gv_status_failure;
        END;

--table4

        BEGIN
            lv_table_count := 0;
            SELECT
                COUNT(*)
            INTO lv_table_count
            FROM
                all_objects
            WHERE
                    upper(object_name) = 'XXCNV_AP_C006_POZ_SUP_SITE_ASSIGN_EXT'
                AND object_type = 'TABLE';

            IF lv_table_count > 0 THEN
                EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c006_poz_sup_site_assign_ext';
                EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c006_poz_sup_site_assign_stg';
                dbms_output.put_line('Table xxcnv_ap_c006_poz_sup_site_assign_ext dropped');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error dropping table xxcnv_ap_c006_poz_sup_site_assign_ext: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                p_loading_status := gv_status_failure;
        END;


    -- Create the external table
        BEGIN
            IF gv_oci_file_name_suppheader IS NOT NULL THEN
                dbms_output.put_line('Creating an external table:'
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppheader);

        -- Create the external table
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c006_poz_suppliers_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppheader,
                    format          =>
                            JSON_OBJECT(
                                'type' VALUE 'csv',
                                'skipheaders' VALUE '1',
                                'conversionerrors' VALUE 'store_NULL',
                                'dateformat' VALUE 'yyyy/mm/dd',
                                'rejectlimit' VALUE 'UNLIMITED',
                                        'blankasNULL' VALUE 'true',
                                'ignoremissingcolumns' VALUE 'true'
                            ),
                    column_list     => 'batch_id	VARCHAR2(200)
						,IMPORT_ACTION   VARCHAR2(10)
						,vendor_name        VARCHAR2(360)       
						,vendor_name_NEW    VARCHAR2(360)       
						,SEGMENT1		VARCHAR2(30)            
						,vendor_name_ALT    VARCHAR2(360)       
						,ORGANIZATION_TYPE_LOOKUP_CODE VARCHAR2(30)
						,VENDOR_TYPE_LOOKUP_CODE  VARCHAR2(30)     
						,END_DATE_ACTIVE	      DATE                       
						,BUSINESS_RELATIONSHIP	  VARCHAR2(30)                 
						,PARENT_Supplier_Name  	  VARCHAR2(360)      
						,ALIAS                    VARCHAR2(360)        
						,DUNS_NUMBER       VARCHAR2(30)     
						,ONE_TIME_FLAG	   VARCHAR2(1)      
						,CUSTOMER_NUM            VARCHAR2(25)     
						,STANDARD_INDUSTRY_CLASS  VARCHAR2(25)   
						,NI_NUMBER            VARCHAR2(30)       
						,CORPORATE_WEBSITE      VARCHAR2(150)    
						,CHIEF_EXECUTIVE_TITLE   VARCHAR2(240)   
						,CHIEF_EXECUTIVE_NAME	 VARCHAR2(240)   
						,BC_NOT_APPLICABLE_FLAG   VARCHAR2(1)    
						,TAX_COUNTRY_CODE	VARCHAR2(5)          
						,NUM_1099           VARCHAR2(30)         
						,FEDERAL_REPORTABLE_FLAG  VARCHAR2(1)    
						,TYPE_1099	        VARCHAR2(10)         
						,STATE_REPORTABLE_FLAG	 VARCHAR2(1)     
						,TAX_REPORTING_NAME   VARCHAR2(80)       
						,NAME_CONTROL	        VARCHAR2(4)      
						,TAX_VERIFICATION_DATE	 DATE                
						,ALLOW_AWT_FLAG	   VARCHAR2(1)           
						,AWT_GROUP_NAME	     VARCHAR2(30)        
						,VAT_CODE	      VARCHAR2(30)           
						,VAT_REGISTRATION_NUM	VARCHAR2(20)     
						,AUTO_TAX_CALC_OVERRIDE	 VARCHAR2(1)     
						,PAYMENT_METHOD_LOOKUP_CODE VARCHAR2(30) 
						,DELIVERY_CHANNEL_CODE	 VARCHAR2(30)    
						,BANK_INSTRUCTION1_CODE	  VARCHAR2(30)   
						,BANK_INSTRUCTION2_CODE	 VARCHAR2(30)    
						,BANK_INSTRUCTION_DETAILS  VARCHAR2(255) 
						,SETTLEMENT_PRIORITY	VARCHAR2(30)     
						,PAYMENT_TEXT_MESSAGE1	VARCHAR2(256)    
						,PAYMENT_TEXT_MESSAGE2	 VARCHAR2(256)   
						,PAYMENT_TEXT_MESSAGE3	 VARCHAR2(256)   
						,IBY_BANK_VARCHAR2GE_BEARER  VARCHAR2(30)
						,PAYMENT_REASON_CODE	VARCHAR2(30)     
						,PAYMENT_REASON_COMMENTS VARCHAR2(240)   
						,PAYMENT_format_CODE	VARCHAR2(30)     
						,ATTRIBUTE_CATEGORY	VARCHAR2(30)         
						,ATTRIBUTE1	      VARCHAR2(150) 	     
						,ATTRIBUTE2  		VARCHAR2(150) 	     
						,ATTRIBUTE3		VARCHAR2(150)			
						,ATTRIBUTE4		VARCHAR2(150)			
						,ATTRIBUTE5		VARCHAR2(150)			
						,ATTRIBUTE6		VARCHAR2(150)			
						,ATTRIBUTE7		VARCHAR2(150)			
						,ATTRIBUTE8		VARCHAR2(150)			
						,ATTRIBUTE9		VARCHAR2(150)			
						,ATTRIBUTE10		VARCHAR2(150)		
						,ATTRIBUTE11		VARCHAR2(150)		
						,ATTRIBUTE12		VARCHAR2(150)		
						,ATTRIBUTE13		VARCHAR2(150)		
						,ATTRIBUTE14		VARCHAR2(150)		
						,ATTRIBUTE15		VARCHAR2(150)		
						,ATTRIBUTE16		VARCHAR2(150)		
						,ATTRIBUTE17		VARCHAR2(150)		
						,ATTRIBUTE18		VARCHAR2(150)		
						,ATTRIBUTE19		VARCHAR2(150)		
						,ATTRIBUTE20		VARCHAR2(150)		
						,ATTRIBUTE_DATE1   		DATE				
						,ATTRIBUTE_DATE2   		DATE				 
						,ATTRIBUTE_DATE3		DATE				
						,ATTRIBUTE_DATE4		DATE				
						,ATTRIBUTE_DATE5		DATE				
						,ATTRIBUTE_DATE6		DATE				
						,ATTRIBUTE_DATE7		DATE				
						,ATTRIBUTE_DATE8		DATE				
						,ATTRIBUTE_DATE9		DATE				
						,ATTRIBUTE_DATE10		DATE				
						,ATTRIBUTE_TIMESTAMP1	TIMESTAMP 		
						,ATTRIBUTE_TIMESTAMP2	TIMESTAMP			
						,ATTRIBUTE_TIMESTAMP3	TIMESTAMP			
						,ATTRIBUTE_TIMESTAMP4	TIMESTAMP			
						,ATTRIBUTE_TIMESTAMP5	TIMESTAMP			
						,ATTRIBUTE_TIMESTAMP6	TIMESTAMP			
						,ATTRIBUTE_TIMESTAMP7	TIMESTAMP			
						,ATTRIBUTE_TIMESTAMP8	TIMESTAMP			
						,ATTRIBUTE_TIMESTAMP9	TIMESTAMP			
						,ATTRIBUTE_TIMESTAMP10	TIMESTAMP			
						,ATTRIBUTE_NUMBER1      NUMBER				
						,ATTRIBUTE_NUMBER2  	NUMBER				
						,ATTRIBUTE_NUMBER3  	NUMBER				
						,ATTRIBUTE_NUMBER4  	NUMBER				
						,ATTRIBUTE_NUMBER5  	NUMBER				
						,ATTRIBUTE_NUMBER6  	NUMBER				
						,ATTRIBUTE_NUMBER7  	NUMBER				
						,ATTRIBUTE_NUMBER8  	NUMBER				
						,ATTRIBUTE_NUMBER9  	NUMBER				
						,ATTRIBUTE_NUMBER10 	NUMBER				
						,GLOBAL_ATTRIBUTE_CATEGORY VARCHAR2(30)	 
						,GLOBAL_ATTRIBUTE1  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE2  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE3  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE4  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE5  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE6  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE7  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE8  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE9  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE10  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE11  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE12  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE13  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE14  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE15  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE16  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE17  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE18  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE19  VARCHAR2(150)		
						,GLOBAL_ATTRIBUTE20   VARCHAR2(150) 	
						,GLOBAL_ATTRIBUTE_DATE1		DATE			
						,GLOBAL_ATTRIBUTE_DATE2		DATE			
						,GLOBAL_ATTRIBUTE_DATE3		DATE			
						,GLOBAL_ATTRIBUTE_DATE4		DATE			
						,GLOBAL_ATTRIBUTE_DATE5		DATE			
						,GLOBAL_ATTRIBUTE_DATE6		DATE			
						,GLOBAL_ATTRIBUTE_DATE7		DATE			
						,GLOBAL_ATTRIBUTE_DATE8		DATE			
						,GLOBAL_ATTRIBUTE_DATE9		DATE			
						,GLOBAL_ATTRIBUTE_DATE10	DATE			
						,GLOBAL_ATTRIBUTE_TIMESTAMP1 	TIMESTAMP		
						,GLOBAL_ATTRIBUTE_TIMESTAMP2 	TIMESTAMP		
						,GLOBAL_ATTRIBUTE_TIMESTAMP3 	TIMESTAMP		
						,GLOBAL_ATTRIBUTE_TIMESTAMP4 	TIMESTAMP		
						,GLOBAL_ATTRIBUTE_TIMESTAMP5 	TIMESTAMP		
						,GLOBAL_ATTRIBUTE_TIMESTAMP6 	TIMESTAMP		
						,GLOBAL_ATTRIBUTE_TIMESTAMP7 	TIMESTAMP		
						,GLOBAL_ATTRIBUTE_TIMESTAMP8 	TIMESTAMP		
						,GLOBAL_ATTRIBUTE_TIMESTAMP9 	TIMESTAMP		
						,GLOBAL_ATTRIBUTE_TIMESTAMP10	TIMESTAMP		
						,GLOBAL_ATTRIBUTE_NUMBER1 		NUMBER		
						,GLOBAL_ATTRIBUTE_NUMBER2 		NUMBER		
						,GLOBAL_ATTRIBUTE_NUMBER3 		NUMBER		
						,GLOBAL_ATTRIBUTE_NUMBER4 		NUMBER		
						,GLOBAL_ATTRIBUTE_NUMBER5 		NUMBER		
						,GLOBAL_ATTRIBUTE_NUMBER6 		NUMBER		
						,GLOBAL_ATTRIBUTE_NUMBER7 		NUMBER		
						,GLOBAL_ATTRIBUTE_NUMBER8 		NUMBER		
						,GLOBAL_ATTRIBUTE_NUMBER9 		NUMBER		
						,GLOBAL_ATTRIBUTE_NUMBER10		NUMBER								
						,PARTY_NUMBER 	VARCHAR2(30)	         
						,SERVICE_LEVEL_CODE  VARCHAR2(30)        
						,EXCLUSIVE_PAYMENT_FLAG VARCHAR2(1)      
						,REMIT_ADVICE_DELIVERY_METHOD  VARCHAR2(30)
						,REMIT_ADVICE_EMAIL  VARCHAR2(255)       
						,REMIT_ADVICE_FAX    VARCHAR2(100)       
						,DATAFOX_COMPANY_ID VARCHAR2(30)'
                );

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c006_poz_suppliers_stg (
                                    IMPORT_ACTION
									,vendor_name
									,vendor_name_NEW
									,SEGMENT1
									,vendor_name_ALT
									,ORGANIZATION_TYPE_LOOKUP_CODE
									,VENDOR_TYPE_LOOKUP_CODE
									,END_DATE_ACTIVE
									,BUSINESS_RELATIONSHIP
									,PARENT_Supplier_Name
									,ALIAS
									,DUNS_NUMBER
									,ONE_TIME_FLAG
									,CUSTOMER_NUM
									,STANDARD_INDUSTRY_CLASS
									,NI_NUMBER
									,CORPORATE_WEBSITE
									,CHIEF_EXECUTIVE_TITLE
									,CHIEF_EXECUTIVE_NAME
									,BC_NOT_APPLICABLE_FLAG
									,TAX_COUNTRY_CODE
									,NUM_1099
									,FEDERAL_REPORTABLE_FLAG
									,TYPE_1099
									,STATE_REPORTABLE_FLAG
									,TAX_REPORTING_NAME
									,NAME_CONTROL
									,TAX_VERIFICATION_DATE
									,ALLOW_AWT_FLAG
									,AWT_GROUP_NAME
									,VAT_CODE
									,VAT_REGISTRATION_NUM
									,AUTO_TAX_CALC_OVERRIDE
									,PAYMENT_METHOD_LOOKUP_CODE
									,DELIVERY_CHANNEL_CODE
									,BANK_INSTRUCTION1_CODE
									,BANK_INSTRUCTION2_CODE
									,BANK_INSTRUCTION_DETAILS
									,SETTLEMENT_PRIORITY
									,PAYMENT_TEXT_MESSAGE1
									,PAYMENT_TEXT_MESSAGE2
									,PAYMENT_TEXT_MESSAGE3
									,IBY_BANK_VARCHAR2GE_BEARER
									,PAYMENT_REASON_CODE
									,PAYMENT_REASON_COMMENTS
									,PAYMENT_format_CODE
									,ATTRIBUTE_CATEGORY
									,ATTRIBUTE1
									,ATTRIBUTE2
									,ATTRIBUTE3
									,ATTRIBUTE4
									,ATTRIBUTE5
									,ATTRIBUTE6
									,ATTRIBUTE7
									,ATTRIBUTE8
									,ATTRIBUTE9
									,ATTRIBUTE10
									,ATTRIBUTE11
									,ATTRIBUTE12
									,ATTRIBUTE13
									,ATTRIBUTE14
									,ATTRIBUTE15
									,ATTRIBUTE16
									,ATTRIBUTE17
									,ATTRIBUTE18
									,ATTRIBUTE19
									,ATTRIBUTE20
									,ATTRIBUTE_DATE1
									,ATTRIBUTE_DATE2
									,ATTRIBUTE_DATE3
									,ATTRIBUTE_DATE4
									,ATTRIBUTE_DATE5
									,ATTRIBUTE_DATE6
									,ATTRIBUTE_DATE7
									,ATTRIBUTE_DATE8
									,ATTRIBUTE_DATE9
									,ATTRIBUTE_DATE10
									,ATTRIBUTE_TIMESTAMP1
									,ATTRIBUTE_TIMESTAMP2
									,ATTRIBUTE_TIMESTAMP3
									,ATTRIBUTE_TIMESTAMP4
									,ATTRIBUTE_TIMESTAMP5
									,ATTRIBUTE_TIMESTAMP6
									,ATTRIBUTE_TIMESTAMP7
									,ATTRIBUTE_TIMESTAMP8
									,ATTRIBUTE_TIMESTAMP9
									,ATTRIBUTE_TIMESTAMP10
									,ATTRIBUTE_NUMBER1
									,ATTRIBUTE_NUMBER2
									,ATTRIBUTE_NUMBER3
									,ATTRIBUTE_NUMBER4
									,ATTRIBUTE_NUMBER5
									,ATTRIBUTE_NUMBER6
									,ATTRIBUTE_NUMBER7
									,ATTRIBUTE_NUMBER8
									,ATTRIBUTE_NUMBER9
									,ATTRIBUTE_NUMBER10
									,GLOBAL_ATTRIBUTE_CATEGORY
									,GLOBAL_ATTRIBUTE1
									,GLOBAL_ATTRIBUTE2
									,GLOBAL_ATTRIBUTE3
									,GLOBAL_ATTRIBUTE4
									,GLOBAL_ATTRIBUTE5
									,GLOBAL_ATTRIBUTE6
									,GLOBAL_ATTRIBUTE7
									,GLOBAL_ATTRIBUTE8
									,GLOBAL_ATTRIBUTE9
									,GLOBAL_ATTRIBUTE10
									,GLOBAL_ATTRIBUTE11
									,GLOBAL_ATTRIBUTE12
									,GLOBAL_ATTRIBUTE13
									,GLOBAL_ATTRIBUTE14
									,GLOBAL_ATTRIBUTE15
									,GLOBAL_ATTRIBUTE16
									,GLOBAL_ATTRIBUTE17
									,GLOBAL_ATTRIBUTE18
									,GLOBAL_ATTRIBUTE19
									,GLOBAL_ATTRIBUTE20
									,GLOBAL_ATTRIBUTE_DATE1
									,GLOBAL_ATTRIBUTE_DATE2
									,GLOBAL_ATTRIBUTE_DATE3
									,GLOBAL_ATTRIBUTE_DATE4
									,GLOBAL_ATTRIBUTE_DATE5
									,GLOBAL_ATTRIBUTE_DATE6
									,GLOBAL_ATTRIBUTE_DATE7
									,GLOBAL_ATTRIBUTE_DATE8
									,GLOBAL_ATTRIBUTE_DATE9
									,GLOBAL_ATTRIBUTE_DATE10
									,GLOBAL_ATTRIBUTE_TIMESTAMP1
									,GLOBAL_ATTRIBUTE_TIMESTAMP2
									,GLOBAL_ATTRIBUTE_TIMESTAMP3
									,GLOBAL_ATTRIBUTE_TIMESTAMP4
									,GLOBAL_ATTRIBUTE_TIMESTAMP5
									,GLOBAL_ATTRIBUTE_TIMESTAMP6
									,GLOBAL_ATTRIBUTE_TIMESTAMP7
									,GLOBAL_ATTRIBUTE_TIMESTAMP8
									,GLOBAL_ATTRIBUTE_TIMESTAMP9
									,GLOBAL_ATTRIBUTE_TIMESTAMP10
									,GLOBAL_ATTRIBUTE_NUMBER1
									,GLOBAL_ATTRIBUTE_NUMBER2
									,GLOBAL_ATTRIBUTE_NUMBER3
									,GLOBAL_ATTRIBUTE_NUMBER4
									,GLOBAL_ATTRIBUTE_NUMBER5
									,GLOBAL_ATTRIBUTE_NUMBER6
									,GLOBAL_ATTRIBUTE_NUMBER7
									,GLOBAL_ATTRIBUTE_NUMBER8
									,GLOBAL_ATTRIBUTE_NUMBER9
									,GLOBAL_ATTRIBUTE_NUMBER10
									,batch_id
									,PARTY_NUMBER
									,SERVICE_LEVEL_CODE
									,EXCLUSIVE_PAYMENT_FLAG
									,REMIT_ADVICE_DELIVERY_METHOD
									,REMIT_ADVICE_EMAIL
									,REMIT_ADVICE_FAX
									,DATAFOX_COMPANY_ID
									,file_name
									,error_message
									,import_status
									,file_reference_identifier
									,EXECUTION_ID
									,source_system
										) 
										SELECT 
										IMPORT_ACTION
										,vendor_name
										,vendor_name_NEW
										,SEGMENT1
										,vendor_name_ALT
										,ORGANIZATION_TYPE_LOOKUP_CODE
										,VENDOR_TYPE_LOOKUP_CODE
										,END_DATE_ACTIVE
										,BUSINESS_RELATIONSHIP
										,PARENT_Supplier_Name
										,ALIAS
										,DUNS_NUMBER
										,ONE_TIME_FLAG
										,CUSTOMER_NUM
										,STANDARD_INDUSTRY_CLASS
										,NI_NUMBER
										,CORPORATE_WEBSITE
										,CHIEF_EXECUTIVE_TITLE
										,CHIEF_EXECUTIVE_NAME
										,BC_NOT_APPLICABLE_FLAG
										,TAX_COUNTRY_CODE
										,NUM_1099
										,FEDERAL_REPORTABLE_FLAG
										,TYPE_1099
										,STATE_REPORTABLE_FLAG
										,TAX_REPORTING_NAME
										,NAME_CONTROL
										,TAX_VERIFICATION_DATE
										,ALLOW_AWT_FLAG
										,AWT_GROUP_NAME
										,VAT_CODE
										,VAT_REGISTRATION_NUM
										,AUTO_TAX_CALC_OVERRIDE
										,PAYMENT_METHOD_LOOKUP_CODE
										,DELIVERY_CHANNEL_CODE
										,BANK_INSTRUCTION1_CODE
										,BANK_INSTRUCTION2_CODE
										,BANK_INSTRUCTION_DETAILS
										,SETTLEMENT_PRIORITY
										,PAYMENT_TEXT_MESSAGE1
										,PAYMENT_TEXT_MESSAGE2
										,PAYMENT_TEXT_MESSAGE3
										,IBY_BANK_VARCHAR2GE_BEARER
										,PAYMENT_REASON_CODE
										,PAYMENT_REASON_COMMENTS
										,PAYMENT_format_CODE
										,ATTRIBUTE_CATEGORY
										,ATTRIBUTE1
										,ATTRIBUTE2
										,ATTRIBUTE3
										,ATTRIBUTE4
										,ATTRIBUTE5
										,ATTRIBUTE6
										,ATTRIBUTE7
										,ATTRIBUTE8
										,ATTRIBUTE9
										,ATTRIBUTE10
										,ATTRIBUTE11
										,ATTRIBUTE12
										,ATTRIBUTE13
										,ATTRIBUTE14
										,ATTRIBUTE15
										,ATTRIBUTE16
										,ATTRIBUTE17
										,ATTRIBUTE18
										,ATTRIBUTE19
										,ATTRIBUTE20
										,ATTRIBUTE_DATE1
										,ATTRIBUTE_DATE2
										,ATTRIBUTE_DATE3
										,ATTRIBUTE_DATE4
										,ATTRIBUTE_DATE5
										,ATTRIBUTE_DATE6
										,ATTRIBUTE_DATE7
										,ATTRIBUTE_DATE8
										,ATTRIBUTE_DATE9
										,ATTRIBUTE_DATE10
										,ATTRIBUTE_TIMESTAMP1
										,ATTRIBUTE_TIMESTAMP2
										,ATTRIBUTE_TIMESTAMP3
										,ATTRIBUTE_TIMESTAMP4
										,ATTRIBUTE_TIMESTAMP5
										,ATTRIBUTE_TIMESTAMP6
										,ATTRIBUTE_TIMESTAMP7
										,ATTRIBUTE_TIMESTAMP8
										,ATTRIBUTE_TIMESTAMP9
										,ATTRIBUTE_TIMESTAMP10
										,ATTRIBUTE_NUMBER1
										,ATTRIBUTE_NUMBER2
										,ATTRIBUTE_NUMBER3
										,ATTRIBUTE_NUMBER4
										,ATTRIBUTE_NUMBER5
										,ATTRIBUTE_NUMBER6
										,ATTRIBUTE_NUMBER7
										,ATTRIBUTE_NUMBER8
										,ATTRIBUTE_NUMBER9
										,ATTRIBUTE_NUMBER10
										,GLOBAL_ATTRIBUTE_CATEGORY
										,GLOBAL_ATTRIBUTE1
										,GLOBAL_ATTRIBUTE2
										,GLOBAL_ATTRIBUTE3
										,GLOBAL_ATTRIBUTE4
										,GLOBAL_ATTRIBUTE5
										,GLOBAL_ATTRIBUTE6
										,GLOBAL_ATTRIBUTE7
										,GLOBAL_ATTRIBUTE8
										,GLOBAL_ATTRIBUTE9
										,GLOBAL_ATTRIBUTE10
										,GLOBAL_ATTRIBUTE11
										,GLOBAL_ATTRIBUTE12
										,GLOBAL_ATTRIBUTE13
										,GLOBAL_ATTRIBUTE14
										,GLOBAL_ATTRIBUTE15
										,GLOBAL_ATTRIBUTE16
										,GLOBAL_ATTRIBUTE17
										,GLOBAL_ATTRIBUTE18
										,GLOBAL_ATTRIBUTE19
										,GLOBAL_ATTRIBUTE20
										,GLOBAL_ATTRIBUTE_DATE1
										,GLOBAL_ATTRIBUTE_DATE2
										,GLOBAL_ATTRIBUTE_DATE3
										,GLOBAL_ATTRIBUTE_DATE4
										,GLOBAL_ATTRIBUTE_DATE5
										,GLOBAL_ATTRIBUTE_DATE6
										,GLOBAL_ATTRIBUTE_DATE7
										,GLOBAL_ATTRIBUTE_DATE8
										,GLOBAL_ATTRIBUTE_DATE9
										,GLOBAL_ATTRIBUTE_DATE10
										,GLOBAL_ATTRIBUTE_TIMESTAMP1
										,GLOBAL_ATTRIBUTE_TIMESTAMP2
										,GLOBAL_ATTRIBUTE_TIMESTAMP3
										,GLOBAL_ATTRIBUTE_TIMESTAMP4
										,GLOBAL_ATTRIBUTE_TIMESTAMP5
										,GLOBAL_ATTRIBUTE_TIMESTAMP6
										,GLOBAL_ATTRIBUTE_TIMESTAMP7
										,GLOBAL_ATTRIBUTE_TIMESTAMP8
										,GLOBAL_ATTRIBUTE_TIMESTAMP9
										,GLOBAL_ATTRIBUTE_TIMESTAMP10
										,GLOBAL_ATTRIBUTE_NUMBER1
										,GLOBAL_ATTRIBUTE_NUMBER2
										,GLOBAL_ATTRIBUTE_NUMBER3
										,GLOBAL_ATTRIBUTE_NUMBER4
										,GLOBAL_ATTRIBUTE_NUMBER5
										,GLOBAL_ATTRIBUTE_NUMBER6
										,GLOBAL_ATTRIBUTE_NUMBER7
										,GLOBAL_ATTRIBUTE_NUMBER8
										,GLOBAL_ATTRIBUTE_NUMBER9
										,GLOBAL_ATTRIBUTE_NUMBER10	
										,batch_id										
										,PARTY_NUMBER
										,SERVICE_LEVEL_CODE
										,EXCLUSIVE_PAYMENT_FLAG
										,REMIT_ADVICE_DELIVERY_METHOD
										,REMIT_ADVICE_EMAIL
										,REMIT_ADVICE_FAX
										,DATAFOX_COMPANY_ID
										,NULL
										,NULL
										,NULL
										,NULL
										--,'
                                  || chr(39)
                                  || gv_execution_id
                                  || chr(39)
                                  || ' 
                                        ,NULL
										,NULL
										FROM xxcnv_ap_c006_poz_suppliers_ext';

                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c006_poz_suppliers_stg: ' || SQL%rowcount);
            END IF;

--TABLE2

            IF gv_oci_file_name_suppaddress IS NOT NULL THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c006_poz_supplier_addresses_stg');
                dbms_output.put_line(' xxcnv_ap_c006_poz_supplier_addresses_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppaddress);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c006_poz_supplier_addresses_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppaddress,
                    format          =>
                            JSON_OBJECT(
                                'type' VALUE 'csv',
                                'skipheaders' VALUE '1',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'conversionerrors' VALUE 'store_NULL',
                                'dateformat' VALUE 'yyyy/mm/dd',
                                        'ignoremissingcolumns' VALUE 'true',
                                'blankasNULL' VALUE 'true'
                            ),
                    column_list     => 'Batch_ID	VARCHAR2(200),
				    Import_Action 	VARCHAR2(10),
					vendor_name	VARCHAR2(360),
					PARTY_SITE_NAME	VARCHAR2(240),
		            PARTY_SITE_NAME_NEW VARCHAR2(240), 
					COUNTRY	VARCHAR2(60),
					ADDRESS_LINE1	VARCHAR2(240),
					ADDRESS_LINE2	VARCHAR2(240),
					ADDRESS_LINE3	VARCHAR2(240),
					ADDRESS_LINE4	VARCHAR2(240),
					ADDRESS_LINES_PHONETIC	VARCHAR2(560),
					ADDR_ELEMENT_ATTRIBUTE1	VARCHAR2(150),
					ADDR_ELEMENT_ATTRIBUTE2	VARCHAR2(150),
					ADDR_ELEMENT_ATTRIBUTE3	VARCHAR2(150),
					ADDR_ELEMENT_ATTRIBUTE4	VARCHAR2(150),
					ADDR_ELEMENT_ATTRIBUTE5	VARCHAR2(150),
					BUILDING	VARCHAR2(240),
					FLOOR_NUMBER	VARCHAR2(40),
					CITY	VARCHAR2(60),
					STATE	VARCHAR2(60),
					PROVINCE	VARCHAR2(60),
					COUNTY	VARCHAR2(60),
					POSTAL_CODE	VARCHAR2(60),
					POSTAL_PLUS4_CODE	VARCHAR2(10),
					ADDRESSEE	VARCHAR2(360),
					GLOBAL_LOCATION_NUMBER	VARCHAR2(40),
					PARTY_SITE_LANGUAGE	VARCHAR2(4),
					INACTIVE_DATE	DATE,
					PHONE_COUNTRY_CODE	VARCHAR2(10),
					PHONE_AREA_CODE	VARCHAR2(10),
					PHONE	VARCHAR2(40),
					PHONE_EXTENSION	VARCHAR2(20),
					FAX_COUNTRY_CODE	VARCHAR2(10),
					FAX_AREA_CODE	VARCHAR2(10),
					FAX	VARCHAR2(40),
					RFQ_OR_BIDDING_PURPOSE_FLAG	VARCHAR2(1),
					ORDERING_PURPOSE_FLAG	VARCHAR2(1),
					REMIT_TO_PURPOSE_FLAG	VARCHAR2(1),
					ATTRIBUTE_CATEGORY	VARCHAR2(30),
					ATTRIBUTE1	VARCHAR2(150),
					ATTRIBUTE2	VARCHAR2(150),
					ATTRIBUTE3	VARCHAR2(150),
					ATTRIBUTE4	VARCHAR2(150),
					ATTRIBUTE5	VARCHAR2(150),
					ATTRIBUTE6	VARCHAR2(150),
					ATTRIBUTE7	VARCHAR2(150),
					ATTRIBUTE8	VARCHAR2(150),
					ATTRIBUTE9	VARCHAR2(150),
					ATTRIBUTE10	VARCHAR2(150),
					ATTRIBUTE11	VARCHAR2(150),
					ATTRIBUTE12	VARCHAR2(150),
					ATTRIBUTE13	VARCHAR2(150),
					ATTRIBUTE14	VARCHAR2(150),
					ATTRIBUTE15	VARCHAR2(150),
					ATTRIBUTE16	VARCHAR2(150),
					ATTRIBUTE17	VARCHAR2(150),
					ATTRIBUTE18	VARCHAR2(150),
					ATTRIBUTE19	VARCHAR2(150),
					ATTRIBUTE20	VARCHAR2(150),
					ATTRIBUTE21	VARCHAR2(150),
					ATTRIBUTE22	VARCHAR2(150),
					ATTRIBUTE23	VARCHAR2(150),
					ATTRIBUTE24	VARCHAR2(150),
					ATTRIBUTE25	VARCHAR2(150),
					ATTRIBUTE26	VARCHAR2(150),
					ATTRIBUTE27	VARCHAR2(150),
					ATTRIBUTE28	VARCHAR2(150),
					ATTRIBUTE29	VARCHAR2(150),
					ATTRIBUTE30	VARCHAR2(255),
					ATTRIBUTE_NUMBER1	NUMBER,
					ATTRIBUTE_NUMBER2	NUMBER,
					ATTRIBUTE_NUMBER3	NUMBER,
					ATTRIBUTE_NUMBER4	NUMBER,
					ATTRIBUTE_NUMBER5	NUMBER,
					ATTRIBUTE_NUMBER6	NUMBER,
					ATTRIBUTE_NUMBER7	NUMBER,
					ATTRIBUTE_NUMBER8	NUMBER,
					ATTRIBUTE_NUMBER9	NUMBER,
					ATTRIBUTE_NUMBER10	NUMBER,
					ATTRIBUTE_NUMBER11	NUMBER,
					ATTRIBUTE_NUMBER12	NUMBER,
					ATTRIBUTE_DATE1	DATE,
					ATTRIBUTE_DATE2	DATE,
					ATTRIBUTE_DATE3	DATE,
					ATTRIBUTE_DATE4	DATE,
					ATTRIBUTE_DATE5	DATE,
					ATTRIBUTE_DATE6	DATE,
					ATTRIBUTE_DATE7	DATE,
					ATTRIBUTE_DATE8	DATE,
					ATTRIBUTE_DATE9	DATE,
					ATTRIBUTE_DATE10	DATE,
					ATTRIBUTE_DATE11	DATE,
					ATTRIBUTE_DATE12	DATE,
					EMAIL_ADDRESS	VARCHAR2(320),
					DELIVERY_CHANNEL_CODE	VARCHAR2(30),
					BANK_INSTRUCTION1	VARCHAR2(30),
					BANK_INSTRUCTION2	VARCHAR2(30),
					BANK_INSTRUCTION	VARCHAR2(30),
					SETTLEMENT_PRIORITY	VARCHAR2(30),
					PAYMENT_TEXT_MESSAGE1	VARCHAR2(256),
					PAYMENT_TEXT_MESSAGE2	VARCHAR2(256),
					PAYMENT_TEXT_MESSAGE3	VARCHAR2(256),
					SERVICE_LEVEL_CODE	VARCHAR2(30),
					EXCLUSIVE_PAYMENT_FLAG	VARCHAR2(1),
					IBY_BANK_CHARGE_BEARER	VARCHAR2(30),
					PAYMENT_REASON_CODE	VARCHAR2(30),
					PAYMENT_REASON_COMMENTS	VARCHAR2(240),
					REMIT_ADVICE_DELIVERY_METHOD	VARCHAR2(30),
					REMITTANCE_EMAIL	VARCHAR2(255),
					REMIT_ADVICE_FAX	VARCHAR2(100)'
                );

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c006_poz_supplier_addresses_stg (
						Import_Action ,
						vendor_name,
						PARTY_SITE_NAME,
						PARTY_SITE_NAME_NEW,
						COUNTRY,
						ADDRESS_LINE1,
						ADDRESS_LINE2,
						ADDRESS_LINE3,
						ADDRESS_LINE4,
						ADDRESS_LINES_PHONETIC,
						ADDR_ELEMENT_ATTRIBUTE1,
						ADDR_ELEMENT_ATTRIBUTE2,
						ADDR_ELEMENT_ATTRIBUTE3,
						ADDR_ELEMENT_ATTRIBUTE4,
						ADDR_ELEMENT_ATTRIBUTE5,
						BUILDING,
						FLOOR_NUMBER,
						CITY,
						STATE,
						PROVINCE,
						COUNTY,
						POSTAL_CODE,
						POSTAL_PLUS4_CODE,
						ADDRESSEE,
						GLOBAL_LOCATION_NUMBER,
						PARTY_SITE_LANGUAGE,
						INACTIVE_DATE,
						PHONE_COUNTRY_CODE,
						PHONE_AREA_CODE,
						PHONE,
						PHONE_EXTENSION,
						FAX_COUNTRY_CODE,
						FAX_AREA_CODE,
						FAX,
						RFQ_OR_BIDDING_PURPOSE_FLAG,
						ORDERING_PURPOSE_FLAG,
						REMIT_TO_PURPOSE_FLAG,
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
						ATTRIBUTE16,
						ATTRIBUTE17,
						ATTRIBUTE18,
						ATTRIBUTE19,
						ATTRIBUTE20,
						ATTRIBUTE21,
						ATTRIBUTE22,
						ATTRIBUTE23,
						ATTRIBUTE24,
						ATTRIBUTE25,
						ATTRIBUTE26,
						ATTRIBUTE27,
						ATTRIBUTE28,
						ATTRIBUTE29,
						ATTRIBUTE30,
						ATTRIBUTE_NUMBER1,
						ATTRIBUTE_NUMBER2,
						ATTRIBUTE_NUMBER3,
						ATTRIBUTE_NUMBER4,
						ATTRIBUTE_NUMBER5,
						ATTRIBUTE_NUMBER6,
						ATTRIBUTE_NUMBER7,
						ATTRIBUTE_NUMBER8,
						ATTRIBUTE_NUMBER9,
						ATTRIBUTE_NUMBER10,
						ATTRIBUTE_NUMBER11,
						ATTRIBUTE_NUMBER12,
						ATTRIBUTE_DATE1,
						ATTRIBUTE_DATE2,
						ATTRIBUTE_DATE3,
						ATTRIBUTE_DATE4,
						ATTRIBUTE_DATE5,
						ATTRIBUTE_DATE6,
						ATTRIBUTE_DATE7,
						ATTRIBUTE_DATE8,
						ATTRIBUTE_DATE9,
						ATTRIBUTE_DATE10,
						ATTRIBUTE_DATE11,
						ATTRIBUTE_DATE12,
						EMAIL_ADDRESS,
						Batch_ID,
						DELIVERY_CHANNEL_CODE,
						BANK_INSTRUCTION1,
						BANK_INSTRUCTION2,
						BANK_INSTRUCTION,
						SETTLEMENT_PRIORITY,
						PAYMENT_TEXT_MESSAGE1,
						PAYMENT_TEXT_MESSAGE2,
						PAYMENT_TEXT_MESSAGE3,
						SERVICE_LEVEL_CODE,
						EXCLUSIVE_PAYMENT_FLAG,
						IBY_BANK_CHARGE_BEARER,
						PAYMENT_REASON_CODE,
						PAYMENT_REASON_COMMENTS,
						REMIT_ADVICE_DELIVERY_METHOD,
						REMITTANCE_EMAIL,
						REMIT_ADVICE_FAX,
						FILE_NAME
						,error_message
						,IMPORT_STATUS
                        ,FILE_REFERENCE_IDENTIFIER
						,EXECUTION_ID						 
						,SOURCE_SYSTEM )
					SELECT 
						Import_Action ,
						vendor_name,
						PARTY_SITE_NAME,
						PARTY_SITE_NAME_NEW,
						COUNTRY,
						ADDRESS_LINE1,
						ADDRESS_LINE2,
						ADDRESS_LINE3,
						ADDRESS_LINE4,
						ADDRESS_LINES_PHONETIC,
						ADDR_ELEMENT_ATTRIBUTE1,
						ADDR_ELEMENT_ATTRIBUTE2,
						ADDR_ELEMENT_ATTRIBUTE3,
						ADDR_ELEMENT_ATTRIBUTE4,
						ADDR_ELEMENT_ATTRIBUTE5,
						BUILDING,
						FLOOR_NUMBER,
						CITY,
						STATE,
						PROVINCE,
						COUNTY,
						POSTAL_CODE,
						POSTAL_PLUS4_CODE,
						ADDRESSEE,
						GLOBAL_LOCATION_NUMBER,
						PARTY_SITE_LANGUAGE,
						INACTIVE_DATE,
						PHONE_COUNTRY_CODE,
						PHONE_AREA_CODE,
						PHONE,
						PHONE_EXTENSION,
						FAX_COUNTRY_CODE,
						FAX_AREA_CODE,
						FAX,
						RFQ_OR_BIDDING_PURPOSE_FLAG,
						ORDERING_PURPOSE_FLAG,
						REMIT_TO_PURPOSE_FLAG,
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
						ATTRIBUTE16,
						ATTRIBUTE17,
						ATTRIBUTE18,
						ATTRIBUTE19,
						ATTRIBUTE20,
						ATTRIBUTE21,
						ATTRIBUTE22,
						ATTRIBUTE23,
						ATTRIBUTE24,
						ATTRIBUTE25,
						ATTRIBUTE26,
						ATTRIBUTE27,
						ATTRIBUTE28,
						ATTRIBUTE29,
						ATTRIBUTE30,
						ATTRIBUTE_NUMBER1,
						ATTRIBUTE_NUMBER2,
						ATTRIBUTE_NUMBER3,
						ATTRIBUTE_NUMBER4,
						ATTRIBUTE_NUMBER5,
						ATTRIBUTE_NUMBER6,
						ATTRIBUTE_NUMBER7,
						ATTRIBUTE_NUMBER8,
						ATTRIBUTE_NUMBER9,
						ATTRIBUTE_NUMBER10,
						ATTRIBUTE_NUMBER11,
						ATTRIBUTE_NUMBER12,
						ATTRIBUTE_DATE1,
						ATTRIBUTE_DATE2,
						ATTRIBUTE_DATE3,
						ATTRIBUTE_DATE4,
						ATTRIBUTE_DATE5,
						ATTRIBUTE_DATE6,
						ATTRIBUTE_DATE7,
						ATTRIBUTE_DATE8,
						ATTRIBUTE_DATE9,
						ATTRIBUTE_DATE10,
						ATTRIBUTE_DATE11,
						ATTRIBUTE_DATE12,
						EMAIL_ADDRESS,
						Batch_ID,
						DELIVERY_CHANNEL_CODE,
						BANK_INSTRUCTION1,
						BANK_INSTRUCTION2,
						BANK_INSTRUCTION,
						SETTLEMENT_PRIORITY,
						PAYMENT_TEXT_MESSAGE1,
						PAYMENT_TEXT_MESSAGE2,
						PAYMENT_TEXT_MESSAGE3,
						SERVICE_LEVEL_CODE,
						EXCLUSIVE_PAYMENT_FLAG,
						IBY_BANK_CHARGE_BEARER,
						PAYMENT_REASON_CODE,
						PAYMENT_REASON_COMMENTS,
						REMIT_ADVICE_DELIVERY_METHOD,
						REMITTANCE_EMAIL,
						REMIT_ADVICE_FAX,
						NULL,
						NULL,
						NULL,
						NULL,
						NULL ,
                        NULL
						FROM xxcnv_ap_c006_poz_supplier_addresses_ext';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c006_poz_supplier_addresses_stg: ' || SQL%rowcount);
            END IF;


--TABLE3

            IF gv_oci_file_name_suppsites IS NOT NULL THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c006_poz_supplier_sites_stg');
                dbms_output.put_line(' xxcnv_ap_c006_poz_supplier_sites_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppsites);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c006_poz_supplier_sites_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppsites,
                    format          =>
                            JSON_OBJECT(
                                'type' VALUE 'csv',
                                'skipheaders' VALUE '1',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'conversionerrors' VALUE 'store_NULL',
                                'dateformat' VALUE 'yyyy/mm/dd',
                                        'ignoremissingcolumns' VALUE 'true',
                                'blankasNULL' VALUE 'true',
                                'ignoreblanklines' VALUE 'true'
                            ),
                    column_list     => 'batch_id VARCHAR2(200),
					IMPORT_ACTION VARCHAR2(10),
					vendor_name VARCHAR2(360),
					PROCUREMENT_BUSINESS_UNIT_NAME VARCHAR2(240),
					PARTY_SITE_NAME VARCHAR2(240),
					VENDOR_SITE_CODE VARCHAR2(240),
					VENDOR_SITE_CODE_NEW VARCHAR2(240),
					INACTIVE_DATE DATE,
					RFQ_ONLY_SITE_FLAG VARCHAR2(1),
					PURCHASING_SITE_FLAG VARCHAR2(1),
					PCARD_SITE_FLAG VARCHAR2(1),
					PAY_SITE_FLAG VARCHAR2(1),
					PRIMARY_PAY_SITE_FLAG VARCHAR2(1),
					TAX_REPORTING_SITE_FLAG VARCHAR2(1),
					VENDOR_SITE_CODE_ALT VARCHAR2(320),
					CUSTOMER_NUM VARCHAR2(25),
					B2B_COMMUNICATION_METHOD VARCHAR2(30),
					B2B_SITE_CODE VARCHAR2(256),
					SUPPLIER_NOTIF_METHOD VARCHAR2(25),
					EMAIL_ADDRESS VARCHAR2(2000),
					FAX_COUNTRY_CODE VARCHAR2(10),
					FAX_AREA_CODE VARCHAR2(10),
					FAX VARCHAR2(15),
					HOLD_FLAG VARCHAR2(1),
					PURCHASING_HOLD_REASON VARCHAR2(240),
					CARRIER VARCHAR2(360),
					MODE_OF_TRANSPORT_CODE VARCHAR2(30),
					SERVICE_LEVEL_CODE VARCHAR2(30),
					FREIGHT_TERMS_LOOKUP_CODE VARCHAR2(30),
					PAY_ON_CODE VARCHAR2(25),
					FOB_LOOKUP_CODE VARCHAR2(25),
					COUNTRY_OF_ORIGIN_CODE VARCHAR2(2),
					BUYER_MANAGED_TRANSPORT_FLAG VARCHAR2(1),
					PAY_ON_USE_FLAG VARCHAR2(1),
					AGING_ONSET_POINT VARCHAR2(30),
					AGING_PERIOD_DAYS NUMBER(5),
					CONSUMPTION_ADVICE_FREQUENCY VARCHAR2(30),
					CONSUMPTION_ADVICE_SUMMARY VARCHAR2(30),
					DEFAULT_PAY_SITE_CODE VARCHAR2(15),
					PAY_ON_RECEIPT_SUMMARY_CODE VARCHAR2(25),
					GAPLESS_INV_NUM_FLAG VARCHAR2(1),
					SELLING_COMPANY_IDENTIFIER VARCHAR2(10),
					CREATE_DEBIT_MEMO_FLAG VARCHAR2(25),
					ENFORCE_SHIP_TO_LOCATION_CODE VARCHAR2(25),
					RECEIVING_ROUTING_ID VARCHAR2(18),
					QTY_RCV_TOLERANCE NUMBER,
					QTY_RCV_EXCEPTION_CODE VARCHAR2(25),
					DAYS_EARLY_RECEIPT_ALLOWED NUMBER,
					DAYS_LATE_RECEIPT_ALLOWED NUMBER,
					ALLOW_SUBSTITUTE_RECEIPTS_FLAG VARCHAR2(1),
					ALLOW_UNORDERED_RECEIPTS_FLAG VARCHAR2(1),
					RECEIPT_DAYS_EXCEPTION_CODE VARCHAR2(25),
					INVOICE_CURRENCY_CODE VARCHAR2(15),
					INVOICE_AMOUNT_LIMIT NUMBER,
					MATCH_OPTION VARCHAR2(25),
					MATCH_APPROVAL_LEVEL VARCHAR2(1),
					PAYMENT_CURRENCY_CODE VARCHAR2(15),
					PAYMENT_PRIORITY NUMBER,
					PAY_GROUP_LOOKUP_CODE VARCHAR2(25),
					TOLERANCE_NAME  VARCHAR2(255),
					SERVICES_TOLERANCE VARCHAR2(255),
					HOLD_ALL_PAYMENTS_FLAG VARCHAR2(1),
					HOLD_UNMATCHED_INVOICES_FLAG VARCHAR2(1),
					HOLD_FUTURE_PAYMENTS_FLAG VARCHAR2(1),
					HOLD_BY NUMBER,
					PAYMENT_HOLD_DATE DATE,
					HOLD_REASON VARCHAR2(240),
					TERMS_NAME VARCHAR2(50),
					TERMS_DATE_BASICS VARCHAR2(25),
					PAY_DATE_BASIS_LOOKUP_CODE VARCHAR2(25),
					BANK_CHARGE_DEDUCTION_TYPE VARCHAR2(1),
					ALWAYS_TAKE_DISC_FLAG VARCHAR2(1),
					EXCLUDE_FREIGHT_FROM_DISCOUNT VARCHAR2(1),
					EXCLUDE_TAX_FROM_DISCOUNT VARCHAR2(1),
					AUTO_CALCULATE_INTEREST_FLAG VARCHAR2(1),
                    VAT_CODE_OBSOLETED VARCHAR2(30),
                    TAX_REGISTRATION_NUMBER_OBSOLETED VARCHAR2(20), 
					PAYMENT_METHOD_LOOKUP_CODE VARCHAR2(30),
					DELIVERY_CHANNEL_CODE VARCHAR2(30),
					BANK_INSTRUCTION1_CODE  VARCHAR2(30)   ,
					BANK_INSTRUCTION2_CODE VARCHAR2(30)    ,
					BANK_INSTRUCTION_DETAILS  VARCHAR2(255) ,
					SETTLEMENT_PRIORITY VARCHAR2(30)     ,
					PAYMENT_TEXT_MESSAGE1 VARCHAR2(256)    ,
					PAYMENT_TEXT_MESSAGE2 VARCHAR2(256)   ,
					PAYMENT_TEXT_MESSAGE3 VARCHAR2(256) 
					,IBY_BANK_VARCHAR2GE_BEARER  VARCHAR2(30)
					,PAYMENT_REASON_CODE VARCHAR2(30)     
					,PAYMENT_REASON_COMMENTS VARCHAR2(240)  
					,REMIT_ADVICE_DELIVERY_METHOD VARCHAR2(30)
					,REMITTANCE_EMAIL VARCHAR2(255)
					,REMIT_ADVICE_FAX VARCHAR2(100) 
					,ATTRIBUTE_CATEGORY VARCHAR2(30)         
					,ATTRIBUTE1      VARCHAR2(150)      
					,ATTRIBUTE2  VARCHAR2(150)      
					,ATTRIBUTE3 VARCHAR2(150)
					,ATTRIBUTE4 VARCHAR2(150)
					,ATTRIBUTE5 VARCHAR2(150)
					,ATTRIBUTE6 VARCHAR2(150)
					,ATTRIBUTE7 VARCHAR2(150)
					,ATTRIBUTE8 VARCHAR2(150)
					,ATTRIBUTE9 VARCHAR2(150)
					,ATTRIBUTE10 VARCHAR2(150)
					,ATTRIBUTE11 VARCHAR2(150)
					,ATTRIBUTE12 VARCHAR2(150)
					,ATTRIBUTE13 VARCHAR2(150)
					,ATTRIBUTE14 VARCHAR2(150)
					,ATTRIBUTE15 VARCHAR2(150)
					,ATTRIBUTE16 VARCHAR2(150)
					,ATTRIBUTE17 VARCHAR2(150)
					,ATTRIBUTE18 VARCHAR2(150)
					,ATTRIBUTE19 VARCHAR2(150)
					,ATTRIBUTE20 VARCHAR2(150)
					,ATTRIBUTE_DATE1   DATE
					,ATTRIBUTE_DATE2   DATE 
					,ATTRIBUTE_DATE3 DATE
					,ATTRIBUTE_DATE4 DATE
					,ATTRIBUTE_DATE5 DATE
					,ATTRIBUTE_DATE6 DATE
					,ATTRIBUTE_DATE7 DATE
					,ATTRIBUTE_DATE8 DATE
					,ATTRIBUTE_DATE9 DATE
					,ATTRIBUTE_DATE10 DATE
					,ATTRIBUTE_TIMESTAMP1 TIMESTAMP 
					,ATTRIBUTE_TIMESTAMP2 TIMESTAMP
					,ATTRIBUTE_TIMESTAMP3 TIMESTAMP
					,ATTRIBUTE_TIMESTAMP4 TIMESTAMP
					,ATTRIBUTE_TIMESTAMP5 TIMESTAMP
					,ATTRIBUTE_TIMESTAMP6 TIMESTAMP
					,ATTRIBUTE_TIMESTAMP7 TIMESTAMP
					,ATTRIBUTE_TIMESTAMP8 TIMESTAMP
					,ATTRIBUTE_TIMESTAMP9 TIMESTAMP
					,ATTRIBUTE_TIMESTAMP10 TIMESTAMP
					,ATTRIBUTE_NUMBER1  NUMBER
					,ATTRIBUTE_NUMBER2  NUMBER
					,ATTRIBUTE_NUMBER3  NUMBER
					,ATTRIBUTE_NUMBER4  NUMBER
					,ATTRIBUTE_NUMBER5  NUMBER
					,ATTRIBUTE_NUMBER6  NUMBER
					,ATTRIBUTE_NUMBER7  NUMBER
					,ATTRIBUTE_NUMBER8  NUMBER
					,ATTRIBUTE_NUMBER9  NUMBER
					,ATTRIBUTE_NUMBER10 NUMBER
					,GLOBAL_ATTRIBUTE_CATEGORY VARCHAR2(30) 
					,GLOBAL_ATTRIBUTE1  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE2  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE3  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE4  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE5  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE6  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE7  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE8  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE9  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE10  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE11  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE12  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE13  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE14  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE15  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE16  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE17  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE18  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE19  VARCHAR2(150)
					,GLOBAL_ATTRIBUTE20   VARCHAR2(150) 
					,GLOBAL_ATTRIBUTE_DATE1 DATE
					,GLOBAL_ATTRIBUTE_DATE2 DATE
					,GLOBAL_ATTRIBUTE_DATE3 DATE
					,GLOBAL_ATTRIBUTE_DATE4 DATE
					,GLOBAL_ATTRIBUTE_DATE5 DATE
					,GLOBAL_ATTRIBUTE_DATE6 DATE
					,GLOBAL_ATTRIBUTE_DATE7 DATE
					,GLOBAL_ATTRIBUTE_DATE8 DATE
					,GLOBAL_ATTRIBUTE_DATE9 DATE
					,GLOBAL_ATTRIBUTE_DATE10 DATE
					,GLOBAL_ATTRIBUTE_TIMESTAMP1 TIMESTAMP
					,GLOBAL_ATTRIBUTE_TIMESTAMP2 TIMESTAMP
					,GLOBAL_ATTRIBUTE_TIMESTAMP3 TIMESTAMP
					,GLOBAL_ATTRIBUTE_TIMESTAMP4 TIMESTAMP
					,GLOBAL_ATTRIBUTE_TIMESTAMP5 TIMESTAMP
					,GLOBAL_ATTRIBUTE_TIMESTAMP6 TIMESTAMP
					,GLOBAL_ATTRIBUTE_TIMESTAMP7 TIMESTAMP
					,GLOBAL_ATTRIBUTE_TIMESTAMP8 TIMESTAMP
					,GLOBAL_ATTRIBUTE_TIMESTAMP9 TIMESTAMP
					,GLOBAL_ATTRIBUTE_TIMESTAMP10 TIMESTAMP
					,GLOBAL_ATTRIBUTE_NUMBER1 NUMBER
					,GLOBAL_ATTRIBUTE_NUMBER2 NUMBER
					,GLOBAL_ATTRIBUTE_NUMBER3 NUMBER
					,GLOBAL_ATTRIBUTE_NUMBER4 NUMBER
					,GLOBAL_ATTRIBUTE_NUMBER5 NUMBER
					,GLOBAL_ATTRIBUTE_NUMBER6 NUMBER
					,GLOBAL_ATTRIBUTE_NUMBER7 NUMBER
					,GLOBAL_ATTRIBUTE_NUMBER8 NUMBER
					,GLOBAL_ATTRIBUTE_NUMBER9 NUMBER
					,GLOBAL_ATTRIBUTE_NUMBER10 NUMBER
					,PO_ACK_REQD_CODE VARCHAR2(30)
					,PO_ACK_REQD_DAYS NUMBER
					,INVOICE_CHANNEL VARCHAR2(30)
					,PAYEE_SERVICE_LEVEL_CODE VARCHAR2(30)
					,EXCLUSIVE_PARENT_FLAG VARCHAR2(1)'
                );

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c006_poz_supplier_sites_stg (
					IMPORT_ACTION 
					,vendor_name
					,PROCUREMENT_BUSINESS_UNIT_NAME
					,PARTY_SITE_NAME
					,VENDOR_SITE_CODE
					,VENDOR_SITE_CODE_NEW
					,INACTIVE_DATE
					,RFQ_ONLY_SITE_FLAG
					,PURCHASING_SITE_FLAG
					,PCARD_SITE_FLAG
					,PAY_SITE_FLAG
					,PRIMARY_PAY_SITE_FLAG
					,TAX_REPORTING_SITE_FLAG
					,VENDOR_SITE_CODE_ALT
					,CUSTOMER_NUM
					,B2B_COMMUNICATION_METHOD
					,B2B_SITE_CODE
					,SUPPLIER_NOTIF_METHOD
					,EMAIL_ADDRESS
					,FAX_COUNTRY_CODE
					,FAX_AREA_CODE
					,FAX
					,HOLD_FLAG
					,PURCHASING_HOLD_REASON
					,CARRIER
					,MODE_OF_TRANSPORT_CODE
					,SERVICE_LEVEL_CODE
					,FREIGHT_TERMS_LOOKUP_CODE
					,PAY_ON_CODE
					,FOB_LOOKUP_CODE
					,COUNTRY_OF_ORIGIN_CODE
					,BUYER_MANAGED_TRANSPORT_FLAG
					,PAY_ON_USE_FLAG
					,AGING_ONSET_POINT
					,AGING_PERIOD_DAYS
					,CONSUMPTION_ADVICE_FREQUENCY
					,CONSUMPTION_ADVICE_SUMMARY
					,DEFAULT_PAY_SITE_CODE
					,PAY_ON_RECEIPT_SUMMARY_CODE
					,GAPLESS_INV_NUM_FLAG
					,SELLING_COMPANY_IDENTIFIER
					,CREATE_DEBIT_MEMO_FLAG
					,ENFORCE_SHIP_TO_LOCATION_CODE
					,RECEIVING_ROUTING_ID
					,QTY_RCV_TOLERANCE
					,QTY_RCV_EXCEPTION_CODE
					,DAYS_EARLY_RECEIPT_ALLOWED
					,DAYS_LATE_RECEIPT_ALLOWED
					,ALLOW_SUBSTITUTE_RECEIPTS_FLAG
					,ALLOW_UNORDERED_RECEIPTS_FLAG
					,RECEIPT_DAYS_EXCEPTION_CODE
					,INVOICE_CURRENCY_CODE
					,INVOICE_AMOUNT_LIMIT
					,MATCH_OPTION
					,MATCH_APPROVAL_LEVEL
					,PAYMENT_CURRENCY_CODE
					,PAYMENT_PRIORITY
					,PAY_GROUP_LOOKUP_CODE
					,TOLERANCE_NAME  
					,SERVICES_TOLERANCE 
					,HOLD_ALL_PAYMENTS_FLAG
					,HOLD_UNMATCHED_INVOICES_FLAG
					,HOLD_FUTURE_PAYMENTS_FLAG
					,HOLD_BY
					,PAYMENT_HOLD_DATE 
					,HOLD_REASON
					,TERMS_NAME 
					,PAY_DATE_BASIS_LOOKUP_CODE 
					,BANK_CHARGE_DEDUCTION_TYPE
					,TERMS_DATE_BASICS 
					,ALWAYS_TAKE_DISC_FLAG
					,EXCLUDE_FREIGHT_FROM_DISCOUNT
					,EXCLUDE_TAX_FROM_DISCOUNT
					,AUTO_CALCULATE_INTEREST_FLAG
					,PAYMENT_METHOD_LOOKUP_CODE
					,DELIVERY_CHANNEL_CODE
					,BANK_INSTRUCTION1_CODE
					,BANK_INSTRUCTION2_CODE
					,BANK_INSTRUCTION_DETAILS
					,SETTLEMENT_PRIORITY
					,PAYMENT_TEXT_MESSAGE1
					,PAYMENT_TEXT_MESSAGE2 
					,PAYMENT_TEXT_MESSAGE3  
					,IBY_BANK_VARCHAR2GE_BEARER
					,PAYMENT_REASON_CODE     
					,PAYMENT_REASON_COMMENTS   
					,REMIT_ADVICE_DELIVERY_METHOD
					,REMITTANCE_EMAIL
					,REMIT_ADVICE_FAX 
					,ATTRIBUTE_CATEGORY        
					,ATTRIBUTE1         
					,ATTRIBUTE2       
					,ATTRIBUTE3
					,ATTRIBUTE4
					,ATTRIBUTE5
					,ATTRIBUTE6
					,ATTRIBUTE7
					,ATTRIBUTE8
					,ATTRIBUTE9
					,ATTRIBUTE10
					,ATTRIBUTE11
					,ATTRIBUTE12
					,ATTRIBUTE13
					,ATTRIBUTE14
					,ATTRIBUTE15
					,ATTRIBUTE16
					,ATTRIBUTE17
					,ATTRIBUTE18
					,ATTRIBUTE19
					,ATTRIBUTE20
					,ATTRIBUTE_DATE1
					,ATTRIBUTE_DATE2 
					,ATTRIBUTE_DATE3
					,ATTRIBUTE_DATE4
					,ATTRIBUTE_DATE5
					,ATTRIBUTE_DATE6
					,ATTRIBUTE_DATE7
					,ATTRIBUTE_DATE8
					,ATTRIBUTE_DATE9
					,ATTRIBUTE_DATE10
					,ATTRIBUTE_TIMESTAMP1
					,ATTRIBUTE_TIMESTAMP2
					,ATTRIBUTE_TIMESTAMP3
					,ATTRIBUTE_TIMESTAMP4
					,ATTRIBUTE_TIMESTAMP5
					,ATTRIBUTE_TIMESTAMP6
					,ATTRIBUTE_TIMESTAMP7
					,ATTRIBUTE_TIMESTAMP8
					,ATTRIBUTE_TIMESTAMP9
					,ATTRIBUTE_TIMESTAMP10
					,ATTRIBUTE_NUMBER1   
					,ATTRIBUTE_NUMBER2  
					,ATTRIBUTE_NUMBER3  
					,ATTRIBUTE_NUMBER4  
					,ATTRIBUTE_NUMBER5  
					,ATTRIBUTE_NUMBER6  
					,ATTRIBUTE_NUMBER7  
					,ATTRIBUTE_NUMBER8  
					,ATTRIBUTE_NUMBER9  
					,ATTRIBUTE_NUMBER10 
					,GLOBAL_ATTRIBUTE_CATEGORY  
					,GLOBAL_ATTRIBUTE1  
					,GLOBAL_ATTRIBUTE2  
					,GLOBAL_ATTRIBUTE3  
					,GLOBAL_ATTRIBUTE4  
					,GLOBAL_ATTRIBUTE5  
					,GLOBAL_ATTRIBUTE6  
					,GLOBAL_ATTRIBUTE7  
					,GLOBAL_ATTRIBUTE8  
					,GLOBAL_ATTRIBUTE9  
					,GLOBAL_ATTRIBUTE10 
					,GLOBAL_ATTRIBUTE11 
					,GLOBAL_ATTRIBUTE12 
					,GLOBAL_ATTRIBUTE13 
					,GLOBAL_ATTRIBUTE14 
					,GLOBAL_ATTRIBUTE15 
					,GLOBAL_ATTRIBUTE16 
					,GLOBAL_ATTRIBUTE17 
					,GLOBAL_ATTRIBUTE18 
					,GLOBAL_ATTRIBUTE19 
					,GLOBAL_ATTRIBUTE20  
					,GLOBAL_ATTRIBUTE_DATE1
					,GLOBAL_ATTRIBUTE_DATE2
					,GLOBAL_ATTRIBUTE_DATE3
					,GLOBAL_ATTRIBUTE_DATE4
					,GLOBAL_ATTRIBUTE_DATE5
					,GLOBAL_ATTRIBUTE_DATE6
					,GLOBAL_ATTRIBUTE_DATE7
					,GLOBAL_ATTRIBUTE_DATE8
					,GLOBAL_ATTRIBUTE_DATE9
					,GLOBAL_ATTRIBUTE_DATE10
					,GLOBAL_ATTRIBUTE_TIMESTAMP1 
					,GLOBAL_ATTRIBUTE_TIMESTAMP2 
					,GLOBAL_ATTRIBUTE_TIMESTAMP3 
					,GLOBAL_ATTRIBUTE_TIMESTAMP4 
					,GLOBAL_ATTRIBUTE_TIMESTAMP5 
					,GLOBAL_ATTRIBUTE_TIMESTAMP6 
					,GLOBAL_ATTRIBUTE_TIMESTAMP7 
					,GLOBAL_ATTRIBUTE_TIMESTAMP8 
					,GLOBAL_ATTRIBUTE_TIMESTAMP9 
					,GLOBAL_ATTRIBUTE_TIMESTAMP10
					,GLOBAL_ATTRIBUTE_NUMBER1 
					,GLOBAL_ATTRIBUTE_NUMBER2 
					,GLOBAL_ATTRIBUTE_NUMBER3 
					,GLOBAL_ATTRIBUTE_NUMBER4 
					,GLOBAL_ATTRIBUTE_NUMBER5 
					,GLOBAL_ATTRIBUTE_NUMBER6 
					,GLOBAL_ATTRIBUTE_NUMBER7 
					,GLOBAL_ATTRIBUTE_NUMBER8 
					,GLOBAL_ATTRIBUTE_NUMBER9 
					,GLOBAL_ATTRIBUTE_NUMBER10
					,PO_ACK_REQD_CODE
					,PO_ACK_REQD_DAYS
					,INVOICE_CHANNEL
					,batch_id
					,PAYEE_SERVICE_LEVEL_CODE
					,EXCLUSIVE_PARENT_FLAG
					,FILE_NAME
					,error_message
					,IMPORT_STATUS
                    ,FILE_REFERENCE_IDENTIFIER 
					,EXECUTION_ID					
					,SOURCE_SYSTEM) 

					SELECT 
					IMPORT_ACTION 
					,vendor_name
					,PROCUREMENT_BUSINESS_UNIT_NAME
					,PARTY_SITE_NAME
					,VENDOR_SITE_CODE
					,VENDOR_SITE_CODE_NEW
					,INACTIVE_DATE
					,RFQ_ONLY_SITE_FLAG
					,PURCHASING_SITE_FLAG
					,PCARD_SITE_FLAG
					,PAY_SITE_FLAG
					,PRIMARY_PAY_SITE_FLAG
					,TAX_REPORTING_SITE_FLAG
					,VENDOR_SITE_CODE_ALT
					,CUSTOMER_NUM
					,B2B_COMMUNICATION_METHOD
					,B2B_SITE_CODE
					,SUPPLIER_NOTIF_METHOD
					,EMAIL_ADDRESS
					,FAX_COUNTRY_CODE
					,FAX_AREA_CODE
					,FAX
					,HOLD_FLAG
					,PURCHASING_HOLD_REASON
					,CARRIER
					,MODE_OF_TRANSPORT_CODE
					,SERVICE_LEVEL_CODE
					,FREIGHT_TERMS_LOOKUP_CODE
					,PAY_ON_CODE
					,FOB_LOOKUP_CODE
					,COUNTRY_OF_ORIGIN_CODE
					,BUYER_MANAGED_TRANSPORT_FLAG
					,PAY_ON_USE_FLAG
					,AGING_ONSET_POINT
					,AGING_PERIOD_DAYS
					,CONSUMPTION_ADVICE_FREQUENCY
					,CONSUMPTION_ADVICE_SUMMARY
					,DEFAULT_PAY_SITE_CODE
					,PAY_ON_RECEIPT_SUMMARY_CODE
					,GAPLESS_INV_NUM_FLAG
					,SELLING_COMPANY_IDENTIFIER
					,CREATE_DEBIT_MEMO_FLAG
					,ENFORCE_SHIP_TO_LOCATION_CODE
					,RECEIVING_ROUTING_ID
					,QTY_RCV_TOLERANCE
					,QTY_RCV_EXCEPTION_CODE
					,DAYS_EARLY_RECEIPT_ALLOWED
					,DAYS_LATE_RECEIPT_ALLOWED
					,ALLOW_SUBSTITUTE_RECEIPTS_FLAG
					,ALLOW_UNORDERED_RECEIPTS_FLAG
					,RECEIPT_DAYS_EXCEPTION_CODE
					,INVOICE_CURRENCY_CODE
					,INVOICE_AMOUNT_LIMIT
					,MATCH_OPTION
					,MATCH_APPROVAL_LEVEL
					,PAYMENT_CURRENCY_CODE
					,PAYMENT_PRIORITY
					,PAY_GROUP_LOOKUP_CODE
					,TOLERANCE_NAME  
					,SERVICES_TOLERANCE 
					,HOLD_ALL_PAYMENTS_FLAG
					,HOLD_UNMATCHED_INVOICES_FLAG
					,HOLD_FUTURE_PAYMENTS_FLAG
					,HOLD_BY
					,PAYMENT_HOLD_DATE 
					,HOLD_REASON
					,TERMS_NAME 
					,PAY_DATE_BASIS_LOOKUP_CODE 
					,BANK_CHARGE_DEDUCTION_TYPE
					,TERMS_DATE_BASICS 
					,ALWAYS_TAKE_DISC_FLAG
					,EXCLUDE_FREIGHT_FROM_DISCOUNT
					,EXCLUDE_TAX_FROM_DISCOUNT
					,AUTO_CALCULATE_INTEREST_FLAG
					,PAYMENT_METHOD_LOOKUP_CODE
					,DELIVERY_CHANNEL_CODE
					,BANK_INSTRUCTION1_CODE
					,BANK_INSTRUCTION2_CODE
					,BANK_INSTRUCTION_DETAILS
					,SETTLEMENT_PRIORITY
					,PAYMENT_TEXT_MESSAGE1
					,PAYMENT_TEXT_MESSAGE2 
					,PAYMENT_TEXT_MESSAGE3  
					,IBY_BANK_VARCHAR2GE_BEARER
					,PAYMENT_REASON_CODE     
					,PAYMENT_REASON_COMMENTS   
					,REMIT_ADVICE_DELIVERY_METHOD
					,REMITTANCE_EMAIL
					,REMIT_ADVICE_FAX 
					,ATTRIBUTE_CATEGORY        
					,ATTRIBUTE1         
					,ATTRIBUTE2       
					,ATTRIBUTE3
					,ATTRIBUTE4
					,ATTRIBUTE5
					,ATTRIBUTE6
					,ATTRIBUTE7
					,ATTRIBUTE8
					,ATTRIBUTE9
					,ATTRIBUTE10
					,ATTRIBUTE11
					,ATTRIBUTE12
					,ATTRIBUTE13
					,ATTRIBUTE14
					,ATTRIBUTE15
					,ATTRIBUTE16
					,ATTRIBUTE17
					,ATTRIBUTE18
					,ATTRIBUTE19
					,ATTRIBUTE20
					,ATTRIBUTE_DATE1
					,ATTRIBUTE_DATE2 
					,ATTRIBUTE_DATE3
					,ATTRIBUTE_DATE4
					,ATTRIBUTE_DATE5
					,ATTRIBUTE_DATE6
					,ATTRIBUTE_DATE7
					,ATTRIBUTE_DATE8
					,ATTRIBUTE_DATE9
					,ATTRIBUTE_DATE10
					,ATTRIBUTE_TIMESTAMP1
					,ATTRIBUTE_TIMESTAMP2
					,ATTRIBUTE_TIMESTAMP3
					,ATTRIBUTE_TIMESTAMP4
					,ATTRIBUTE_TIMESTAMP5
					,ATTRIBUTE_TIMESTAMP6
					,ATTRIBUTE_TIMESTAMP7
					,ATTRIBUTE_TIMESTAMP8
					,ATTRIBUTE_TIMESTAMP9
					,ATTRIBUTE_TIMESTAMP10
					,ATTRIBUTE_NUMBER1   
					,ATTRIBUTE_NUMBER2  
					,ATTRIBUTE_NUMBER3  
					,ATTRIBUTE_NUMBER4  
					,ATTRIBUTE_NUMBER5  
					,ATTRIBUTE_NUMBER6  
					,ATTRIBUTE_NUMBER7  
					,ATTRIBUTE_NUMBER8  
					,ATTRIBUTE_NUMBER9  
					,ATTRIBUTE_NUMBER10 
					,GLOBAL_ATTRIBUTE_CATEGORY  
					,GLOBAL_ATTRIBUTE1  
					,GLOBAL_ATTRIBUTE2  
					,GLOBAL_ATTRIBUTE3  
					,GLOBAL_ATTRIBUTE4  
					,GLOBAL_ATTRIBUTE5  
					,GLOBAL_ATTRIBUTE6  
					,GLOBAL_ATTRIBUTE7  
					,GLOBAL_ATTRIBUTE8  
					,GLOBAL_ATTRIBUTE9  
					,GLOBAL_ATTRIBUTE10 
					,GLOBAL_ATTRIBUTE11 
					,GLOBAL_ATTRIBUTE12 
					,GLOBAL_ATTRIBUTE13 
					,GLOBAL_ATTRIBUTE14 
					,GLOBAL_ATTRIBUTE15 
					,GLOBAL_ATTRIBUTE16 
					,GLOBAL_ATTRIBUTE17 
					,GLOBAL_ATTRIBUTE18 
					,GLOBAL_ATTRIBUTE19 
					,GLOBAL_ATTRIBUTE20  
					,GLOBAL_ATTRIBUTE_DATE1
					,GLOBAL_ATTRIBUTE_DATE2
					,GLOBAL_ATTRIBUTE_DATE3
					,GLOBAL_ATTRIBUTE_DATE4
					,GLOBAL_ATTRIBUTE_DATE5
					,GLOBAL_ATTRIBUTE_DATE6
					,GLOBAL_ATTRIBUTE_DATE7
					,GLOBAL_ATTRIBUTE_DATE8
					,GLOBAL_ATTRIBUTE_DATE9
					,GLOBAL_ATTRIBUTE_DATE10
					,GLOBAL_ATTRIBUTE_TIMESTAMP1 
					,GLOBAL_ATTRIBUTE_TIMESTAMP2 
					,GLOBAL_ATTRIBUTE_TIMESTAMP3 
					,GLOBAL_ATTRIBUTE_TIMESTAMP4 
					,GLOBAL_ATTRIBUTE_TIMESTAMP5 
					,GLOBAL_ATTRIBUTE_TIMESTAMP6 
					,GLOBAL_ATTRIBUTE_TIMESTAMP7 
					,GLOBAL_ATTRIBUTE_TIMESTAMP8 
					,GLOBAL_ATTRIBUTE_TIMESTAMP9 
					,GLOBAL_ATTRIBUTE_TIMESTAMP10
					,GLOBAL_ATTRIBUTE_NUMBER1 
					,GLOBAL_ATTRIBUTE_NUMBER2 
					,GLOBAL_ATTRIBUTE_NUMBER3 
					,GLOBAL_ATTRIBUTE_NUMBER4 
					,GLOBAL_ATTRIBUTE_NUMBER5 
					,GLOBAL_ATTRIBUTE_NUMBER6 
					,GLOBAL_ATTRIBUTE_NUMBER7 
					,GLOBAL_ATTRIBUTE_NUMBER8 
					,GLOBAL_ATTRIBUTE_NUMBER9 
					,GLOBAL_ATTRIBUTE_NUMBER10
					,PO_ACK_REQD_CODE
					,PO_ACK_REQD_DAYS
					,INVOICE_CHANNEL
					,batch_id 
					,PAYEE_SERVICE_LEVEL_CODE
					,EXCLUSIVE_PARENT_FLAG
					,NULL
					,NULL
					,NULL
					,NULL
					,NULL	
                    ,NULL
					FROM xxcnv_ap_c006_poz_supplier_sites_ext';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c006_poz_supplier_sites_stg: ' || SQL%rowcount);
            END IF;

--TABLE4

            IF gv_oci_file_name_suppsitesassign IS NOT NULL THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c006_poz_sup_site_assign_ext');
                dbms_output.put_line(' xxcnv_ap_c006_poz_sup_site_assign_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppsitesassign);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c006_poz_sup_site_assign_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppsitesassign,
                    format          =>
                            JSON_OBJECT(
                                'type' VALUE 'csv',
                                'skipheaders' VALUE '1',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'dateformat' VALUE 'yyyy/mm/dd',
                                'ignoremissingcolumns' VALUE 'true',
                                        'blankasNULL' VALUE 'true'
                            ),
                    column_list     => 'batch_id	VARCHAR2(200),
					IMPORT_ACTION 	VARCHAR2(10)	,
					vendor_name	VARCHAR2(360)	,
					VENDOR_SITE_CODE	VARCHAR2(240)	,
					PROCUREMENT_BUSINESS_UNIT_NAME	VARCHAR2(240)	,
					BUSINESS_UNIT_NAME	VARCHAR2(240)	,
					BILL_TO_BU_NAME	VARCHAR2(240)	,
					SHIP_TO_LOCATION_CODE	VARCHAR2(60)	,
					BILL_TO_LOCATION_CODE	VARCHAR2(60)	,
					ALLOW_AWT_LAG	VARCHAR2(1)	,
					AWT_GROUP_NAME	VARCHAR2(30)	,
					ACCTS_PAY_CONCATENATED_SEGMENTS	VARCHAR2(800)	,
					PREPAY_CONCAT_SEGMENTS	VARCHAR2(800)	,
					FUTURE_DATED_CONCAT_SEGMENTS	VARCHAR2(800)	,
					DISTRIBUTION_SET_NAME VARCHAR2(50)	,
					INACTIVE_DATE	DATE'
                );

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c006_poz_sup_site_assign_stg (
						IMPORT_ACTION,
						vendor_name,
						VENDOR_SITE_CODE,
						PROCUREMENT_BUSINESS_UNIT_NAME,
						BUSINESS_UNIT_NAME,
						BILL_TO_BU_NAME	,
						SHIP_TO_LOCATION_CODE,
						BILL_TO_LOCATION_CODE,
						ALLOW_AWT_LAG,
						AWT_GROUP_NAME,
						ACCTS_PAY_CONCATENATED_SEGMENTS,
						PREPAY_CONCAT_SEGMENTS,
						FUTURE_DATED_CONCAT_SEGMENTS,
						DISTRIBUTION_SET_NAME,
						INACTIVE_DATE,
						batch_id,
						FILE_NAME,
						error_message,
						IMPORT_STATUS,
                        FILE_REFERENCE_IDENTIFIER ,
						EXECUTION_ID,						
						SOURCE_SYSTEM	) 
						SELECT 
						IMPORT_ACTION,
						vendor_name,
						VENDOR_SITE_CODE,
						PROCUREMENT_BUSINESS_UNIT_NAME,
						BUSINESS_UNIT_NAME,
						BILL_TO_BU_NAME	,
						SHIP_TO_LOCATION_CODE,
						BILL_TO_LOCATION_CODE,
						ALLOW_AWT_LAG,
						AWT_GROUP_NAME,
						ACCTS_PAY_CONCATENATED_SEGMENTS,
						PREPAY_CONCAT_SEGMENTS,
						FUTURE_DATED_CONCAT_SEGMENTS,
						DISTRIBUTION_SET_NAME,
						INACTIVE_DATE,
						batch_id,
						NULL,
						NULL,
						NULL,
						NULL						
                        ,NULL	
                        ,NULL
						FROM xxcnv_ap_c006_poz_sup_site_assign_ext';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c006_poz_sup_site_assign_stg: ' || SQL%rowcount);
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                p_loading_status := gv_status_failure;
                dbms_output.put_line('Error in load_staging_table: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

    -- Count the number of rows in the stage table
        BEGIN
            IF gv_oci_file_name_suppheader IS NOT NULL THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c006_poz_suppliers_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c006_poz_suppliers_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;
--TABLE 2	
            IF gv_oci_file_name_suppaddress IS NOT NULL THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c006_poz_supplier_addresses_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c006_poz_supplier_addresses_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;
--TABLE 3		
            IF gv_oci_file_name_suppsites IS NOT NULL THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c006_poz_supplier_sites_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c006_poz_supplier_sites_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;
--TABLE 4 

            IF gv_oci_file_name_suppsitesassign IS NOT NULL THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c006_poz_sup_site_assign_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c006_poz_sup_site_assign_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in the external tables: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

    -- Select batch_id from the stage table
        BEGIN
        -- Count the number of rows in the stage table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c006_poz_suppliers_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c006_poz_suppliers_stg from external table: ' || lv_row_count)
            ;
            IF lv_row_count > 0 THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_picked,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppheader,
                    p_attribute1        => NULL,
                    p_attribute2        => lv_row_count,
                    p_process_reference => NULL
                );
            END IF;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_ap_c006_poz_suppliers_stg: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

--table 2

        BEGIN
        -- Count the number of rows in the stage table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c006_poz_supplier_addresses_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c006_poz_supplier_addresses_stg from external table: ' || lv_row_count
            );
            IF lv_row_count > 0 THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_picked,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppaddress,
                    p_attribute1        => NULL,
                    p_attribute2        => lv_row_count,
                    p_process_reference => NULL
                );
            END IF;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_ap_c006_poz_supplier_addresses_stg: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;



--table 3

        BEGIN
        -- Count the number of rows in the stage table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c006_poz_supplier_sites_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c006_poz_supplier_sites_stg from external table: ' || lv_row_count
            );
            IF lv_row_count > 0 THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_picked,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppsites,
                    p_attribute1        => NULL,
                    p_attribute2        => lv_row_count,
                    p_process_reference => NULL
                );
            END IF;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_ap_c006_poz_supplier_sites_stg: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

--table 4

        BEGIN
        -- Count the number of rows in the stage table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c006_poz_sup_site_assign_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c006_poz_sup_site_assign_stg from external table: ' || lv_row_count
            );
            IF lv_row_count > 0 THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_picked,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppsitesassign,
                    p_attribute1        => NULL,
                    p_attribute2        => lv_row_count,
                    p_process_reference => NULL
                );
            END IF;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_ap_c006_poz_sup_site_assign_stg: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

    END import_data_from_oci_to_stg_prc;

/*=================================================================================================================
-- PROCEDURE : data_validations_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for the validating the mANDatory columns AND business validations as per lean spec
===================================================================================================================*/
    PROCEDURE data_validations_prc IS

  -- Declaring Local Variables for validation.

        lv_row_count   NUMBER;
        lv_error_count NUMBER;
    BEGIN 

     -- Initializing batch_id to current time stamp --

        SELECT
            to_char(sysdate, 'YYYYMMDDHHMM')
        INTO gv_batch_id
        FROM
            dual;
  -- Initialize error_message to an empty string if it IS NULL

        BEGIN
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c006_poz_suppliers_stg;

            IF lv_row_count = 0 THEN
                dbms_output.put_line('No Data is found in the xxcnv_ap_c006_poz_suppliers_stg table');
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);
        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                execution_id = gv_execution_id,
                batch_id = gv_batch_id
            WHERE
                file_reference_identifier IS NULL;

        END;
        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = ''
            WHERE
                error_message IS NULL
                AND execution_id = gv_execution_id;

        END;

    ----------------------Supplier Validations-----------

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = error_message || '|Supplier Name should not be NULL'
            WHERE
                vendor_name IS NULL;

            dbms_output.put_line('Supplier Name is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = error_message || '|Supplier Number should not be NULL'
            WHERE
                segment1 IS NULL;

            dbms_output.put_line('Supplier Number is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = error_message || '|Duplicate Supplier Names'
            WHERE
                vendor_name IN (
                    SELECT
                        vendor_name
                    FROM
                        xxcnv_ap_c006_poz_suppliers_stg
                    WHERE
                        vendor_name IS NOT NULL
                    GROUP BY
                        vendor_name
                    HAVING
                        COUNT(1) > 1
                );

        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = error_message || '|Duplicate Supplier Numbers'
            WHERE
                segment1 IN (
                    SELECT
                        segment1
                    FROM
                        xxcnv_ap_c006_poz_suppliers_stg
                    WHERE
                        segment1 IS NOT NULL
                    GROUP BY
                        segment1
                    HAVING
                        COUNT(1) > 1
                );

        END;

  --Supplier Type
        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = error_message || '|Supplier Type should be present in the valid list of values'
            WHERE
                nvl(vendor_type_lookup_code, 'BS') NOT IN ( 'Contractor', 'Employee', 'Consultant' );

            dbms_output.put_line('Supplier Type is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                vendor_type_lookup_code = 'Employee'
            WHERE
                vendor_type_lookup_code IN ( 'Employee', 'Consultant' );

            dbms_output.put_line('Supplier Type is updated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = error_message || '|Taxpayer Country should not be NULL'
            WHERE
                tax_country_code IS NULL;

            dbms_output.put_line('Taxpayer Country is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = error_message || '|Duplicate Taxpayer_IDs'
            WHERE
                num_1099 IN (
                    SELECT
                        num_1099
                    FROM
                        xxcnv_ap_c006_poz_suppliers_stg
                    WHERE
                        num_1099 IS NOT NULL
                    GROUP BY
                        num_1099
                    HAVING
                        COUNT(1) > 1
                );

        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = error_message || '|Duplicate DUNS Number'
            WHERE
                duns_number IS NOT NULL
                AND duns_number IN (
                    SELECT
                        duns_number
                    FROM
                        xxcnv_ap_c006_poz_suppliers_stg
                    WHERE
                        duns_number IS NOT NULL
                    GROUP BY
                        duns_number
                    HAVING
                        COUNT(1) > 1
                );

        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = error_message || '|Duplicate Tax Registration Number'
            WHERE
                vat_registration_num IS NOT NULL
                AND vat_registration_num IN (
                    SELECT
                        vat_registration_num
                    FROM
                        xxcnv_ap_c006_poz_suppliers_stg
                    WHERE
                        vat_registration_num IS NOT NULL
                    GROUP BY
                        vat_registration_num
                    HAVING
                        COUNT(1) > 1
                );

        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                payment_method_lookup_code = (
                    SELECT
                        oc_value
                    FROM
                        xxcnv_ap_payment_method_mapping
                    WHERE
                        ns_value = payment_method_lookup_code
                )
            WHERE
                payment_method_lookup_code IS NOT NULL;

            dbms_output.put_line('Payment Method is updated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                remit_advice_delivery_method = 'EMAIL'
            WHERE
                    1 = 1
                AND remit_advice_email IS NOT NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Remittance E-mail is updated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                vendor_name = '"'
                              || vendor_name
                              || '"'
            WHERE
                vendor_name LIKE '%,%'
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Supplier Name with comma is updated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                vendor_name_alt = '"'
                                  || vendor_name_alt
                                  || '"'
            WHERE
                vendor_name_alt LIKE '%,%'
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Alternate Name with comma is updated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = error_message || '|At least one of the following fields must be filled: Taxpayer ID, Tax Registration Number, or DUNS Number'
            WHERE
                ( duns_number IS NULL
                  AND num_1099 IS NULL
                  AND vat_registration_num IS NULL )
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Tax Number is validated');
        END;


    -- Updating constant values --

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                import_action = 'CREATE',
                organization_type_lookup_code = 'Individual',
                business_relationship = 'SPEND_AUTHORIZED';

            dbms_output.put_line('Constant fields are updated');
        END;

  -- Update import_status based on error_message
        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                import_status =
                    CASE
                        WHEN error_message IS NOT NULL THEN
                            'ERROR'
                        ELSE
                            'PROCESSED'
                    END;

            dbms_output.put_line('Import_status is validated');
        END;

 --  Final update to set error_message AND import_status
        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                error_message = ltrim(error_message, ','),
                import_status =
                    CASE
                        WHEN error_message IS NOT NULL THEN
                            'ERROR'
                        ELSE
                            'PROCESSED'
                    END;

            dbms_output.put_line('Import_status is updated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure
            WHERE
                error_message IS NOT NULL
                AND file_reference_identifier IS NULL;

        END;

        BEGIN
            UPDATE xxcnv_ap_c006_poz_suppliers_stg
            SET
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_success
            WHERE
                error_message IS NULL
                AND file_reference_identifier IS NULL;

        END;

  -- Check if there are any error messages
        SELECT
            COUNT(*)
        INTO lv_error_count
        FROM
            xxcnv_ap_c006_poz_suppliers_stg
        WHERE
            error_message IS NOT NULL;

        IF lv_error_count > 0 THEN

	 -- Logging the message
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_failed_validation,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_suppheader,
                p_process_reference => NULL,
                p_attribute1        => gv_batch_id,
                p_attribute2        => NULL
            );
        ELSIF gv_oci_file_name_suppheader IS NOT NULL THEN
         -- Logging the message
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_validated,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_suppheader,
                p_attribute1        => gv_batch_id,
                p_attribute2        => NULL,
                p_process_reference => NULL
            );
        ELSE
            NULL;
        END IF;

    ----------------------Supplier Address Validations-----------
        lv_row_count := 0;
        BEGIN
            BEGIN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c006_poz_supplier_addresses_stg;

                IF lv_row_count = 0 THEN
                    dbms_output.put_line('No Data is found in the xxcnv_ap_c006_poz_supplier_addresses_stg table');
                    RETURN;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    execution_id = gv_execution_id,
                    batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END;
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    vendor_name = '"'
                                  || vendor_name
                                  || '"'
                WHERE
                    vendor_name LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    party_site_name = '"'
                                      || party_site_name
                                      || '"'
                WHERE
                    party_site_name LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    address_line1 = '"'
                                    || address_line1
                                    || '"'
                WHERE
                    address_line1 LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    address_line2 = '"'
                                    || address_line2
                                    || '"'
                WHERE
                    address_line2 LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    address_line3 = '"'
                                    || address_line3
                                    || '"'
                WHERE
                    address_line3 LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    address_line4 = '"'
                                    || address_line4
                                    || '"'
                WHERE
                    address_line4 LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    city = '"'
                           || city
                           || '"'
                WHERE
                    city LIKE '%,%';

            END;
  -- Initialize error_message to an empty string if it IS NULL
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = ''
                WHERE
                    error_message IS NULL;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred while initializing error_message: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Supplier Name not found in Supplier Header',
                    import_status = 'ERROR'
                WHERE
                    vendor_name NOT IN (
                        SELECT
                            vendor_name
                        FROM
                            xxcnv_ap_c006_poz_suppliers_stg
                        WHERE
                            execution_id = gv_execution_id
                    )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Child record failed because Parent failed',
                    import_status = 'ERROR'
                WHERE
                    ( vendor_name IN (
                        SELECT
                            vendor_name
                        FROM
                            xxcnv_ap_c006_poz_suppliers_stg
                        WHERE
                                import_status = 'ERROR'
                            AND execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Supplier Name should not be NULL'
                WHERE
                    vendor_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Name is validated');
            END;
  ------------------------------PARTY_SITE_NAME------------------------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Address Name should not be NULL'
                WHERE
                    party_site_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Address Name is validated');
            END;
  --------COUNTRY -------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Country should not be NULL and be of 2 characters'
                WHERE
                    country IS NULL
                    AND length(country) != 2
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Country is validated');
            END;

  -----------------------------ADDRESS_LINE1------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Address line1 should not be NULL'
                WHERE
                    address_line1 IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Address line1 is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Province should not be NULL'
                WHERE
                    province IS NULL
                    AND country IN ( 'CN', 'CA' )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Province is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|City should not be NULL'
                WHERE
                    city IS NULL
                    AND country IN ( 'AU', 'CH', 'DE', 'GB', 'IE',
                                     'IN', 'NL', 'NZ', 'GE' )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('City is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Postal Code should not be NULL'
                WHERE
                    postal_code IS NULL
                    AND country IN ( 'CH', 'DE', 'IN', 'NL', 'NZ',
                                     'SE' )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Postal Code is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    remit_advice_delivery_method = 'EMAIL'
                WHERE
                        1 = 1
                    AND remittance_email IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Remittance E-mail is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Duplicate Address Name'
                WHERE
                    party_site_name IS NOT NULL
                    AND party_site_name IN (
                        SELECT
                            party_site_name
                        FROM
                            xxcnv_ap_c006_poz_supplier_addresses_stg
                        WHERE
                            party_site_name IS NOT NULL
                        GROUP BY
                            party_site_name
                        HAVING
                            COUNT(1) > 1
                    );

            END;

    -- Updating constant values --

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    import_action = 'CREATE',
                    rfq_or_bidding_purpose_flag = 'N',
                    ordering_purpose_flag = 'N',
                    remit_to_purpose_flag = 'Y';

                dbms_output.put_line('Constant fields are updated');
            END;

  ---------------------Update import_status based on error_message----------------

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    file_name = gv_oci_file_name_suppaddress
                WHERE
                    file_reference_identifier IS NULL;

                dbms_output.put_line('File_name column is updated');
            END;

  -- Final update to set error_message AND import_status
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    error_message = ltrim(error_message, ','),
                    import_status =
                        CASE
                            WHEN error_message IS NOT NULL THEN
                                'ERROR'
                            ELSE
                                'PROCESSED'
                        END;

                dbms_output.put_line('Import_status column is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    error_message IS NOT NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('File_reference_identifier column is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_addresses_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success
                WHERE
                    error_message IS NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('file_reference_identifier column is updated');
            END;
  -- Check if there are any error messages
            SELECT
                COUNT(*)
            INTO lv_error_count
            FROM
                xxcnv_ap_c006_poz_supplier_addresses_stg
            WHERE
                error_message IS NOT NULL;

            IF lv_error_count > 0 THEN

    -- Logging the message
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_failed_validation,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppaddress,
                    p_attribute1        => NULL,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );
            ELSIF gv_oci_file_name_suppaddress IS NOT NULL THEN
  -- Logging the message
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_validated,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppaddress,
                    p_attribute1        => NULL,
                    p_attribute2        => gv_data_validated_success,
                    p_process_reference => NULL
                );
            ELSE
                NULL;
            END IF;

        END;

    ----------------------Supplier Site Validations-----------

        BEGIN
            BEGIN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c006_poz_supplier_sites_stg;

                IF lv_row_count = 0 THEN
                    dbms_output.put_line('No Data is found in the xxcnv_ap_c006_poz_supplier_sites_stg table');
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    execution_id = gv_execution_id,
                    batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END;
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    vendor_name = '"'
                                  || vendor_name
                                  || '"'
                WHERE
                    vendor_name LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    party_site_name = '"'
                                      || party_site_name
                                      || '"'
                WHERE
                    party_site_name LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    vendor_site_code = '"'
                                       || vendor_site_code
                                       || '"'
                WHERE
                    vendor_site_code LIKE '%,%';

            END;
	-- Initialize error_message to an empty string if it IS NULL
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = ''
                WHERE
                    error_message IS NULL;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred while initializing error_message: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Child record failed because Parent failed',
                    import_status = 'ERROR'
                WHERE
                    ( vendor_name || party_site_name IN (
                        SELECT
                            vendor_name || party_site_name
                        FROM
                            xxcnv_ap_c006_poz_supplier_addresses_stg
                        WHERE
                            import_status = 'ERROR'
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Supplier Name not found in Supplier header table'
                WHERE
                    ( vendor_name NOT IN (
                        SELECT
                            vendor_name
                        FROM
                            xxcnv_ap_c006_poz_suppliers_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Address Name not found in Supplier address table'
                WHERE
                    ( party_site_name NOT IN (
                        SELECT
                            party_site_name
                        FROM
                            xxcnv_ap_c006_poz_supplier_addresses_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

	-----VENDOR NAME--------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Supplier Name should not be NULL'
                WHERE
                    vendor_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Name is validated');
            END;

  -----PROCUREMENT_BUSINESS_UNIT_NAME------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    procurement_business_unit_name = 'US USD BU'
                WHERE
                        1 = 1
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Procurement BU is updated');
            END;
  -------PARTY_SITE_NAME------------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Address Name should not be NULL'
                WHERE
                    party_site_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Address Name  is validated');
            END;

  -----VENDOR_SITE_CODE------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Supplier Site should not be NULL'
                WHERE
                    vendor_site_code IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Site is validated');
            END;

   ---------COMMUNICATION_METHOD---------------------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    supplier_notif_method = 'EMAIL'
                WHERE
                        1 = 1
                    AND email_address IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Notification Method is updated');
            END;

   ---------EMAIL_ADDRESS---------------------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = error_message || 'Email address is in incorrect format'
                WHERE
                        supplier_notif_method = 'EMAIL'
                    AND email_address IS NOT NULL
                    AND email_address NOT LIKE '%@%'
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Email address is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = error_message || 'Payment Method should not be NULL'
                WHERE
                    payment_method_lookup_code IS NULL;

                dbms_output.put_line('Payment Method is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    payment_method_lookup_code = (
                        SELECT
                            oc_value
                        FROM
                            xxcnv_ap_payment_method_mapping
                        WHERE
                            ns_value = payment_method_lookup_code
                    )
                WHERE
                    payment_method_lookup_code IS NOT NULL;

                dbms_output.put_line('Payment Method is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = error_message || 'Payment Terms should not be NULL'
                WHERE
                    terms_name IS NULL;

                dbms_output.put_line('Payment Terms is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    terms_name = (
                        SELECT
                            oc_value
                        FROM
                            xxcnv_ap_payment_terms_mapping
                        WHERE
                            ns_value = terms_name
                    )
                WHERE
                    terms_name IS NOT NULL;

                dbms_output.put_line('Payment Terms is updated');
            END;

   ---------REMITTANCE_EMAIL---------------------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    remit_advice_delivery_method = 'EMAIL'
                WHERE
                        1 = 1
                    AND remittance_email IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Remittance E-Mail is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Either Email_Address or Fax should be provided but not both'
                WHERE
                        1 = 1
                    AND ( email_address IS NOT NULL
                          AND ( fax_country_code IS NOT NULL
                                OR fax_area_code IS NOT NULL
                                OR fax IS NOT NULL ) )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Email_Address is validated');
            END;

	-- Updating constant values --

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    import_action = 'CREATE',
                    rfq_only_site_flag = 'N',
                    purchasing_site_flag = 'N',
                    pcard_site_flag = 'N',
                    pay_site_flag = 'Y',
                    primary_pay_site_flag = 'N',
                    payment_priority = 99;

                dbms_output.put_line('Constant fields are updated');
            END;

	/* Remove this for SIT1 */
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    email_address = 'no-reply@xyz.com'
                WHERE
                    email_address IS NOT NULL
                    AND file_reference_identifier IS NULL;

            END;



  -- ---------------Final update to set error_message AND import_status
            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    error_message = ltrim(error_message, ','),
                    import_status =
                        CASE
                            WHEN error_message IS NOT NULL THEN
                                'ERROR'
                            ELSE
                                'PROCESSED'
                        END;

                dbms_output.put_line('import_status column is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    error_message IS NOT NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('file_reference_identifier column is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_supplier_sites_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success
                WHERE
                    error_message IS NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('file_reference_identifier column is updated');
            END;

  -- Check if there are any error messages
            SELECT
                COUNT(*)
            INTO lv_error_count
            FROM
                xxcnv_ap_c006_poz_supplier_sites_stg
            WHERE
                error_message IS NOT NULL;

            IF lv_error_count > 0 THEN

    -- Logging the message
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_failed_validation,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppsites,
                    p_attribute1        => NULL,
                    p_attribute2        => gv_data_validated_failure,
                    p_process_reference => NULL
                );
            ELSIF gv_oci_file_name_suppsites IS NOT NULL THEN
  -- Logging the message
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_validated,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppsites,
                    p_attribute1        => NULL,
                    p_attribute2        => gv_data_validated_success,
                    p_process_reference => NULL
                );
            ELSE
                NULL;
            END IF;

        END;
  ---------------------------SUPPLIER SITE ASSIGNMENTS VALIDATIONS------------------

        BEGIN
            BEGIN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c006_poz_sup_site_assign_stg;

                IF lv_row_count = 0 THEN
                    dbms_output.put_line('No Data is found in the xxcnv_ap_c006_poz_sup_site_assign_stg table');
                    RETURN;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    execution_id = gv_execution_id,
                    batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END; 

  -- Initialize error_message to an empty string if it IS NULL
            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    error_message = ''
                WHERE
                    error_message IS NULL;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred while initializing error_message: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    vendor_site_code = '"'
                                       || vendor_site_code
                                       || '"'
                WHERE
                    vendor_site_code LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    vendor_name = '"'
                                  || vendor_name
                                  || '"'
                WHERE
                    vendor_name LIKE '%,%';

            END;

      -----Vendor Site Validation--------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Child record failed because Parent failed'
                WHERE
                    ( vendor_name || vendor_site_code IN (
                        SELECT
                            vendor_name || vendor_site_code
                        FROM
                            xxcnv_ap_c006_poz_supplier_sites_stg
                        WHERE
                                import_status = 'ERROR'
                            AND execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Supplier Name not found in Supplier header table'
                WHERE
                    ( vendor_name NOT IN (
                        SELECT
                            vendor_name
                        FROM
                            xxcnv_ap_c006_poz_suppliers_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Supplier Site not found in Supplier sites table'
                WHERE
                    ( vendor_site_code NOT IN (
                        SELECT
                            vendor_site_code
                        FROM
                            xxcnv_ap_c006_poz_supplier_sites_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

	-----VENDOR NAME--------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Supplier Name should not be NULL'
                WHERE
                    vendor_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Name is validated');
            END;
  -----VENDOR_SITE_CODE------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Supplier Site should not be NULL'
                WHERE
                    vendor_site_code IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Site is validated');
            END;
  -----PROCUREMENT_BUSINESS_UNIT_NAME------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Procurement BU should not be NULL'
                WHERE
                    procurement_business_unit_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Procurement BU is validated');
            END;
	-------BUSINESS_UNIT_NAME------------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Client BU should not be NULL'
                WHERE
                    business_unit_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Client BU is validated');
            END;

	-------BILL_TO_BU_NAME------------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Bill to BU should not be NULL'
                WHERE
                    bill_to_bu_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Client BU is validated');
            END;


	-------BUSINESS_UNIT_NAME------------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    business_unit_name = (
                        SELECT
                            oc_business_unit_name
                        FROM
                            xxcnv_gl_le_bu_mapping
                        WHERE
                            ns_legal_entity_name = business_unit_name
                    )
                WHERE
                    business_unit_name IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Client BU is updated');
            END;

	-------bill_to_bu_name------------
            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    bill_to_bu_name = (
                        SELECT
                            oc_business_unit_name
                        FROM
                            xxcnv_gl_le_bu_mapping
                        WHERE
                            ns_legal_entity_name = bill_to_bu_name
                    )
                WHERE
                    bill_to_bu_name IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Bill_to_bu_name is updated');
            END;

	    -- Updating constant values --

            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    import_action = 'CREATE';

                dbms_output.put_line('Constant fields are updated');
            END;

  -- Final update to set error_message AND import_status
            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    error_message = ltrim(error_message, ','),
                    import_status =
                        CASE
                            WHEN error_message IS NOT NULL THEN
                                'ERROR'
                            ELSE
                                'PROCESSED'
                        END;

                dbms_output.put_line('import_status column is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    error_message IS NOT NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('file_reference_identifier column is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c006_poz_sup_site_assign_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success
                WHERE
                    error_message IS NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('file_reference_identifier column is updated');
            END;


  -- Check if there are any error messages
            SELECT
                COUNT(*)
            INTO lv_error_count
            FROM
                xxcnv_ap_c006_poz_sup_site_assign_stg
            WHERE
                error_message IS NOT NULL;
  --AND file_reference_identifier IS NULL;

            IF lv_error_count > 0 THEN

    -- Logging the message
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_failed_validation,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppsitesassign,
                    p_attribute1        => NULL,
                    p_attribute2        => gv_data_validated_failure,
                    p_process_reference => NULL
                );
            ELSIF gv_oci_file_name_suppsitesassign IS NOT NULL THEN
  -- Logging the message
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_validated,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppsitesassign,
                    p_attribute1        => NULL,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );
            ELSE
                NULL;
            END IF;

        END;

    END data_validations_prc;

/*==============================================================================================================================
-- PROCEDURE : create_fbdi_file_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the FBDI CSV file by using the data in the staging tables after all validations.
================================================================================================================================= */
    PROCEDURE create_fbdi_file_prc IS
        lv_success_count INTEGER;
        lv_batch_id      VARCHAR2(200);
    BEGIN
        BEGIN
            lv_success_count := 0;
            dbms_output.put_line('file_reference_identifier'
                                 || gv_execution_id
                                 || '_'
                                 || gv_status_success);
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_ap_c006_poz_suppliers_stg
                WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No batch_id is found for xxcnv_ap_c006_poz_suppliers_stg');
                    RETURN;
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking batch_id for xxcnv_ap_c006_poz_suppliers_stg ' || sqlerrm);
                    RETURN;
            END;

            BEGIN
                -- Count the number of rows which are validated successfully for the current batch_id
                SELECT
                    COUNT(1)
                INTO lv_success_count
                FROM
                    xxcnv_ap_c006_poz_suppliers_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Success record count for xxcnv_ap_c006_poz_suppliers_stg batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_success_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c006_poz_suppliers_stg batch_id: ' || lv_batch_id);
                    RETURN;
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking success record count for xxcnv_ap_c006_poz_suppliers_stg batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
                    RETURN;
            END;

            IF lv_success_count > 0 THEN
                BEGIN
                    dbms_output.put_line('FilePath: '
                                         || replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder));
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                         || '/'
                                         || lv_batch_id
                                         || gv_oci_file_name_suppheader,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
                                            IMPORT_ACTION
											,vendor_name
											,vendor_name_NEW
											,SEGMENT1
											,vendor_name_ALT
											,ORGANIZATION_TYPE_LOOKUP_CODE
											,VENDOR_TYPE_LOOKUP_CODE
											,END_DATE_ACTIVE
											,BUSINESS_RELATIONSHIP
											,PARENT_Supplier_Name
											,ALIAS
											,DUNS_NUMBER
											,ONE_TIME_FLAG
											,CUSTOMER_NUM
											,STANDARD_INDUSTRY_CLASS
											,NI_NUMBER
											,CORPORATE_WEBSITE
											,CHIEF_EXECUTIVE_TITLE
											,CHIEF_EXECUTIVE_NAME
											,BC_NOT_APPLICABLE_FLAG
											,TAX_COUNTRY_CODE
											,NUM_1099
											,FEDERAL_REPORTABLE_FLAG
											,TYPE_1099
											,STATE_REPORTABLE_FLAG
											,TAX_REPORTING_NAME
											,NAME_CONTROL
											,TAX_VERIFICATION_DATE
											,ALLOW_AWT_FLAG
											,AWT_GROUP_NAME
											,VAT_CODE
											,VAT_REGISTRATION_NUM
											,AUTO_TAX_CALC_OVERRIDE
											,PAYMENT_METHOD_LOOKUP_CODE
											,DELIVERY_CHANNEL_CODE
											,BANK_INSTRUCTION1_CODE
											,BANK_INSTRUCTION2_CODE
											,BANK_INSTRUCTION_DETAILS
											,SETTLEMENT_PRIORITY
											,PAYMENT_TEXT_MESSAGE1
											,PAYMENT_TEXT_MESSAGE2
											,PAYMENT_TEXT_MESSAGE3
											,IBY_BANK_VARCHAR2GE_BEARER
											,PAYMENT_REASON_CODE
											,PAYMENT_REASON_COMMENTS
											,PAYMENT_format_CODE
											,ATTRIBUTE_CATEGORY
											,ATTRIBUTE1
											,ATTRIBUTE2
											,ATTRIBUTE3
											,ATTRIBUTE4
											,ATTRIBUTE5
											,ATTRIBUTE6
											,ATTRIBUTE7
											,ATTRIBUTE8
											,ATTRIBUTE9
											,ATTRIBUTE10
											,ATTRIBUTE11
											,ATTRIBUTE12
											,ATTRIBUTE13
											,ATTRIBUTE14
											,ATTRIBUTE15
											,ATTRIBUTE16
											,ATTRIBUTE17
											,ATTRIBUTE18
											,ATTRIBUTE19
											,ATTRIBUTE20
											,ATTRIBUTE_DATE1
											,ATTRIBUTE_DATE2
											,ATTRIBUTE_DATE3
											,ATTRIBUTE_DATE4
											,ATTRIBUTE_DATE5
											,ATTRIBUTE_DATE6
											,ATTRIBUTE_DATE7
											,ATTRIBUTE_DATE8
											,ATTRIBUTE_DATE9
											,ATTRIBUTE_DATE10
											,ATTRIBUTE_TIMESTAMP1
											,ATTRIBUTE_TIMESTAMP2
											,ATTRIBUTE_TIMESTAMP3
											,ATTRIBUTE_TIMESTAMP4
											,ATTRIBUTE_TIMESTAMP5
											,ATTRIBUTE_TIMESTAMP6
											,ATTRIBUTE_TIMESTAMP7
											,ATTRIBUTE_TIMESTAMP8
											,ATTRIBUTE_TIMESTAMP9
											,ATTRIBUTE_TIMESTAMP10
											,ATTRIBUTE_NUMBER1
											,ATTRIBUTE_NUMBER2
											,ATTRIBUTE_NUMBER3
											,ATTRIBUTE_NUMBER4
											,ATTRIBUTE_NUMBER5
											,ATTRIBUTE_NUMBER6
											,ATTRIBUTE_NUMBER7
											,ATTRIBUTE_NUMBER8
											,ATTRIBUTE_NUMBER9
											,ATTRIBUTE_NUMBER10
											,GLOBAL_ATTRIBUTE_CATEGORY
											,GLOBAL_ATTRIBUTE1
											,GLOBAL_ATTRIBUTE2
											,GLOBAL_ATTRIBUTE3
											,GLOBAL_ATTRIBUTE4
											,GLOBAL_ATTRIBUTE5
											,GLOBAL_ATTRIBUTE6
											,GLOBAL_ATTRIBUTE7
											,GLOBAL_ATTRIBUTE8
											,GLOBAL_ATTRIBUTE9
											,GLOBAL_ATTRIBUTE10
											,GLOBAL_ATTRIBUTE11
											,GLOBAL_ATTRIBUTE12
											,GLOBAL_ATTRIBUTE13
											,GLOBAL_ATTRIBUTE14
											,GLOBAL_ATTRIBUTE15
											,GLOBAL_ATTRIBUTE16
											,GLOBAL_ATTRIBUTE17
											,GLOBAL_ATTRIBUTE18
											,GLOBAL_ATTRIBUTE19
											,GLOBAL_ATTRIBUTE20
											,GLOBAL_ATTRIBUTE_DATE1
											,GLOBAL_ATTRIBUTE_DATE2
											,GLOBAL_ATTRIBUTE_DATE3
											,GLOBAL_ATTRIBUTE_DATE4
											,GLOBAL_ATTRIBUTE_DATE5
											,GLOBAL_ATTRIBUTE_DATE6
											,GLOBAL_ATTRIBUTE_DATE7
											,GLOBAL_ATTRIBUTE_DATE8
											,GLOBAL_ATTRIBUTE_DATE9
											,GLOBAL_ATTRIBUTE_DATE10
											,GLOBAL_ATTRIBUTE_TIMESTAMP1
											,GLOBAL_ATTRIBUTE_TIMESTAMP2
											,GLOBAL_ATTRIBUTE_TIMESTAMP3
											,GLOBAL_ATTRIBUTE_TIMESTAMP4
											,GLOBAL_ATTRIBUTE_TIMESTAMP5
											,GLOBAL_ATTRIBUTE_TIMESTAMP6
											,GLOBAL_ATTRIBUTE_TIMESTAMP7
											,GLOBAL_ATTRIBUTE_TIMESTAMP8
											,GLOBAL_ATTRIBUTE_TIMESTAMP9
											,GLOBAL_ATTRIBUTE_TIMESTAMP10
											,GLOBAL_ATTRIBUTE_NUMBER1
											,GLOBAL_ATTRIBUTE_NUMBER2
											,GLOBAL_ATTRIBUTE_NUMBER3
											,GLOBAL_ATTRIBUTE_NUMBER4
											,GLOBAL_ATTRIBUTE_NUMBER5
											,GLOBAL_ATTRIBUTE_NUMBER6
											,GLOBAL_ATTRIBUTE_NUMBER7
											,GLOBAL_ATTRIBUTE_NUMBER8
											,GLOBAL_ATTRIBUTE_NUMBER9
											,GLOBAL_ATTRIBUTE_NUMBER10
											,batch_id
											,PARTY_NUMBER
											,SERVICE_LEVEL_CODE
											,EXCLUSIVE_PAYMENT_FLAG
											,REMIT_ADVICE_DELIVERY_METHOD
											,REMIT_ADVICE_EMAIL
											,REMIT_ADVICE_FAX
											,DATAFOX_COMPANY_ID
                                            ,NULL
                                            FROM xxcnv_ap_c006_poz_suppliers_stg
                                            WHERE import_status = '''
                                 || 'PROCESSED'
                                 || '''
											AND batch_id ='''
                                 || lv_batch_id
                                 || '''
											AND file_reference_identifier= '''
                                 || gv_execution_id
                                 || '_'
                                 || gv_status_success
                                 || ''''
                    );

                    dbms_output.put_line('xxcnv_ap_c006_poz_suppliers_stg CSV file for batch_id '
                                         || lv_batch_id
                                         || ' exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_fbdi_export_status,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                        p_file_name         => lv_batch_id
                                       || '_'
                                       || gv_oci_file_name_suppheader
                                       || '.csv',
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for  xxcnv_ap_c006_poz_suppliers_stg batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for xxcnv_ap_c006_poz_suppliers_stg batch_id '
                                     || lv_batch_id
                                     || ': Error message columns contain data.');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
                RETURN;
        END;

--table 2

        BEGIN
            lv_success_count := 0;
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_ap_c006_poz_supplier_addresses_stg
                WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No batch_id is found for xxcnv_ap_c006_poz_supplier_addresses_stg');
                    RETURN;
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking batch_id for xxcnv_ap_c006_poz_supplier_addresses_stg ' || sqlerrm);
                    RETURN;
            END;

            BEGIN
                -- Count the success record count for the current batch_id
                SELECT
                    COUNT(1)
                INTO lv_success_count
                FROM
                    xxcnv_ap_c006_poz_supplier_addresses_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Success record count for xxcnv_ap_c006_poz_supplier_addresses_stg batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_success_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c006_poz_supplier_addresses_stg batch_id: ' || lv_batch_id);
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking success record count for batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
            END;

            IF lv_success_count > 0 THEN
                BEGIN
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                         || '/'
                                         || lv_batch_id
                                         || gv_oci_file_name_suppaddress,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
											Import_Action,
											vendor_name,
											PARTY_SITE_NAME,
											PARTY_SITE_NAME_NEW,
											COUNTRY,
											ADDRESS_LINE1,
											ADDRESS_LINE2,
											ADDRESS_LINE3,
											ADDRESS_LINE4,
											ADDRESS_LINES_PHONETIC,
											ADDR_ELEMENT_ATTRIBUTE1,
											ADDR_ELEMENT_ATTRIBUTE2,
											ADDR_ELEMENT_ATTRIBUTE3,
											ADDR_ELEMENT_ATTRIBUTE4,
											ADDR_ELEMENT_ATTRIBUTE5,
											BUILDING,
											FLOOR_NUMBER,
											CITY,
											STATE,
											PROVINCE,
											COUNTY,
											POSTAL_CODE,
											POSTAL_PLUS4_CODE,
											ADDRESSEE,
											GLOBAL_LOCATION_NUMBER,
											PARTY_SITE_LANGUAGE,
											INACTIVE_DATE,
											PHONE_COUNTRY_CODE,
											PHONE_AREA_CODE,
											PHONE,
											PHONE_EXTENSION,
											FAX_COUNTRY_CODE,
											FAX_AREA_CODE,
											FAX,
											RFQ_OR_BIDDING_PURPOSE_FLAG,
											ORDERING_PURPOSE_FLAG,
											REMIT_TO_PURPOSE_FLAG,
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
											ATTRIBUTE16,
											ATTRIBUTE17,
											ATTRIBUTE18,
											ATTRIBUTE19,
											ATTRIBUTE20,
											ATTRIBUTE21,
											ATTRIBUTE22,
											ATTRIBUTE23,
											ATTRIBUTE24,
											ATTRIBUTE25,
											ATTRIBUTE26,
											ATTRIBUTE27,
											ATTRIBUTE28,
											ATTRIBUTE29,
											ATTRIBUTE30,
											ATTRIBUTE_NUMBER1,
											ATTRIBUTE_NUMBER2,
											ATTRIBUTE_NUMBER3,
											ATTRIBUTE_NUMBER4,
											ATTRIBUTE_NUMBER5,
											ATTRIBUTE_NUMBER6,
											ATTRIBUTE_NUMBER7,
											ATTRIBUTE_NUMBER8,
											ATTRIBUTE_NUMBER9,
											ATTRIBUTE_NUMBER10,
											ATTRIBUTE_NUMBER11,
											ATTRIBUTE_NUMBER12,
											ATTRIBUTE_DATE1,
											ATTRIBUTE_DATE2,
											ATTRIBUTE_DATE3,
											ATTRIBUTE_DATE4,
											ATTRIBUTE_DATE5,
											ATTRIBUTE_DATE6,
											ATTRIBUTE_DATE7,
											ATTRIBUTE_DATE8,
											ATTRIBUTE_DATE9,
											ATTRIBUTE_DATE10,
											ATTRIBUTE_DATE11,
											ATTRIBUTE_DATE12,
											EMAIL_ADDRESS,
											--DELIVERY_CHANNEL_CODE,
											--BANK_INSTRUCTION1,
											--BANK_INSTRUCTION2,
											--BANK_INSTRUCTION,
											--SETTLEMENT_PRIORITY,
											--PAYMENT_TEXT_MESSAGE1,
											--PAYMENT_TEXT_MESSAGE2,
											--PAYMENT_TEXT_MESSAGE3,
											--SERVICE_LEVEL_CODE,
											--EXCLUSIVE_PAYMENT_FLAG,
											--IBY_BANK_CHARGE_BEARER,
											--PAYMENT_REASON_CODE,
											--PAYMENT_REASON_COMMENTS,
											--REMIT_ADVICE_DELIVERY_METHOD,
											--REMITTANCE_EMAIL,
											--REMIT_ADVICE_FAX,
                                            Batch_ID
                                            FROM xxcnv_ap_c006_poz_supplier_addresses_stg
                                            WHERE import_status = '''
                                 || 'PROCESSED'
                                 || '''
											AND batch_id ='''
                                 || lv_batch_id
                                 || '''
											AND file_reference_identifier= '''
                                 || gv_execution_id
                                 || '_'
                                 || gv_status_success
                                 || ''''
                    );

                    dbms_output.put_line('xxcnv_ap_c006_poz_supplier_addresses_stg CSV file for batch_id '
                                         || lv_batch_id
                                         || ' exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_fbdi_export_status,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                        p_file_name         => lv_batch_id
                                       || '_'
                                       || gv_oci_file_name_suppaddress,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c006_poz_supplier_addresses_stg batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for xxcnv_ap_c006_poz_supplier_addresses_stg batch_id '
                                     || lv_batch_id
                                     || ': Error message columns contain data.');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
                RETURN;
        END;

--table3

        BEGIN
            lv_success_count := 0;
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_ap_c006_poz_supplier_sites_stg
                WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No batch_id is found for xxcnv_ap_c006_poz_supplier_sites_stg');
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking batch_id for xxcnv_ap_c006_poz_supplier_sites_stg ' || sqlerrm);
            END;
             --dbms_output.put_line('DISTINCT batch for xxcnv_ap_c006_poz_supplier_sites_stg batch_id ' || lv_batch_id);
            BEGIN
                SELECT
                    COUNT(1)
                INTO lv_success_count
                FROM
                    xxcnv_ap_c006_poz_supplier_sites_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Success record count for xxcnv_ap_c006_poz_supplier_sites_stg batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_success_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c006_poz_supplier_sites_stg batch_id: ' || lv_batch_id);
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking success record count for batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
            END;

            IF lv_success_count > 0 THEN
                BEGIN
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                         || '/'
                                         || lv_batch_id
                                         || gv_oci_file_name_suppsites,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
											IMPORT_ACTION 
											,vendor_name
											,PROCUREMENT_BUSINESS_UNIT_NAME
											,PARTY_SITE_NAME
											,VENDOR_SITE_CODE
											,VENDOR_SITE_CODE_NEW
											,INACTIVE_DATE
											,RFQ_ONLY_SITE_FLAG
											,PURCHASING_SITE_FLAG
											,PCARD_SITE_FLAG
											,PAY_SITE_FLAG
											,PRIMARY_PAY_SITE_FLAG
											,TAX_REPORTING_SITE_FLAG
											,VENDOR_SITE_CODE_ALT
											,CUSTOMER_NUM
											,B2B_COMMUNICATION_METHOD
											,B2B_SITE_CODE
											,SUPPLIER_NOTIF_METHOD
											,EMAIL_ADDRESS
											,FAX_COUNTRY_CODE
											,FAX_AREA_CODE
											,FAX
											,HOLD_FLAG
											,PURCHASING_HOLD_REASON
											,CARRIER
											,MODE_OF_TRANSPORT_CODE
											,SERVICE_LEVEL_CODE
											,FREIGHT_TERMS_LOOKUP_CODE
											,PAY_ON_CODE
											,FOB_LOOKUP_CODE
											,COUNTRY_OF_ORIGIN_CODE
											,BUYER_MANAGED_TRANSPORT_FLAG
											,PAY_ON_USE_FLAG
											,AGING_ONSET_POINT
											,AGING_PERIOD_DAYS
											,CONSUMPTION_ADVICE_FREQUENCY
											,CONSUMPTION_ADVICE_SUMMARY
											,DEFAULT_PAY_SITE_CODE
											,PAY_ON_RECEIPT_SUMMARY_CODE
											,GAPLESS_INV_NUM_FLAG
											,SELLING_COMPANY_IDENTIFIER
											,CREATE_DEBIT_MEMO_FLAG
											,ENFORCE_SHIP_TO_LOCATION_CODE
											,RECEIVING_ROUTING_ID
											,QTY_RCV_TOLERANCE
											,QTY_RCV_EXCEPTION_CODE
											,DAYS_EARLY_RECEIPT_ALLOWED
											,DAYS_LATE_RECEIPT_ALLOWED
											,ALLOW_SUBSTITUTE_RECEIPTS_FLAG
											,ALLOW_UNORDERED_RECEIPTS_FLAG
											,RECEIPT_DAYS_EXCEPTION_CODE
											,INVOICE_CURRENCY_CODE
											,INVOICE_AMOUNT_LIMIT
											,MATCH_OPTION
											,MATCH_APPROVAL_LEVEL
											,PAYMENT_CURRENCY_CODE
											,PAYMENT_PRIORITY
											,PAY_GROUP_LOOKUP_CODE
											,TOLERANCE_NAME  
											,SERVICES_TOLERANCE 
											,HOLD_ALL_PAYMENTS_FLAG
											,HOLD_UNMATCHED_INVOICES_FLAG
											,HOLD_FUTURE_PAYMENTS_FLAG
											,HOLD_BY
											,PAYMENT_HOLD_DATE 
											,HOLD_REASON
											,TERMS_NAME 
											,PAY_DATE_BASIS_LOOKUP_CODE 
											,BANK_CHARGE_DEDUCTION_TYPE
											,TERMS_DATE_BASICS 
											,ALWAYS_TAKE_DISC_FLAG
											,EXCLUDE_FREIGHT_FROM_DISCOUNT
											,EXCLUDE_TAX_FROM_DISCOUNT
											,AUTO_CALCULATE_INTEREST_FLAG
                                            ,NULL AS  NULL1
                                            ,NULL AS  NULL2
											,PAYMENT_METHOD_LOOKUP_CODE
											,DELIVERY_CHANNEL_CODE
											,BANK_INSTRUCTION1_CODE
											,BANK_INSTRUCTION2_CODE
											,BANK_INSTRUCTION_DETAILS
											,SETTLEMENT_PRIORITY
											,PAYMENT_TEXT_MESSAGE1
											,PAYMENT_TEXT_MESSAGE2 
											,PAYMENT_TEXT_MESSAGE3  
											,IBY_BANK_VARCHAR2GE_BEARER
											,PAYMENT_REASON_CODE     
											,PAYMENT_REASON_COMMENTS   
											,REMIT_ADVICE_DELIVERY_METHOD
											,REMITTANCE_EMAIL
											,REMIT_ADVICE_FAX 
											,ATTRIBUTE_CATEGORY        
											,ATTRIBUTE1         
											,ATTRIBUTE2       
											,ATTRIBUTE3
											,ATTRIBUTE4
											,ATTRIBUTE5
											,ATTRIBUTE6
											,ATTRIBUTE7
											,ATTRIBUTE8
											,ATTRIBUTE9
											,ATTRIBUTE10
											,ATTRIBUTE11
											,ATTRIBUTE12
											,ATTRIBUTE13
											,ATTRIBUTE14
											,ATTRIBUTE15
											,ATTRIBUTE16
											,ATTRIBUTE17
											,ATTRIBUTE18
											,ATTRIBUTE19
											,ATTRIBUTE20
											,ATTRIBUTE_DATE1
											,ATTRIBUTE_DATE2 
											,ATTRIBUTE_DATE3
											,ATTRIBUTE_DATE4
											,ATTRIBUTE_DATE5
											,ATTRIBUTE_DATE6
											,ATTRIBUTE_DATE7
											,ATTRIBUTE_DATE8
											,ATTRIBUTE_DATE9
											,ATTRIBUTE_DATE10
											,ATTRIBUTE_TIMESTAMP1
											,ATTRIBUTE_TIMESTAMP2
											,ATTRIBUTE_TIMESTAMP3
											,ATTRIBUTE_TIMESTAMP4
											,ATTRIBUTE_TIMESTAMP5
											,ATTRIBUTE_TIMESTAMP6
											,ATTRIBUTE_TIMESTAMP7
											,ATTRIBUTE_TIMESTAMP8
											,ATTRIBUTE_TIMESTAMP9
											,ATTRIBUTE_TIMESTAMP10
											,ATTRIBUTE_NUMBER1   
											,ATTRIBUTE_NUMBER2  
											,ATTRIBUTE_NUMBER3  
											,ATTRIBUTE_NUMBER4  
											,ATTRIBUTE_NUMBER5  
											,ATTRIBUTE_NUMBER6  
											,ATTRIBUTE_NUMBER7  
											,ATTRIBUTE_NUMBER8  
											,ATTRIBUTE_NUMBER9  
											,ATTRIBUTE_NUMBER10 
											,GLOBAL_ATTRIBUTE_CATEGORY  
											,GLOBAL_ATTRIBUTE1  
											,GLOBAL_ATTRIBUTE2  
											,GLOBAL_ATTRIBUTE3  
											,GLOBAL_ATTRIBUTE4  
											,GLOBAL_ATTRIBUTE5  
											,GLOBAL_ATTRIBUTE6  
											,GLOBAL_ATTRIBUTE7  
											,GLOBAL_ATTRIBUTE8  
											,GLOBAL_ATTRIBUTE9  
											,GLOBAL_ATTRIBUTE10 
											,GLOBAL_ATTRIBUTE11 
											,GLOBAL_ATTRIBUTE12 
											,GLOBAL_ATTRIBUTE13 
											,GLOBAL_ATTRIBUTE14 
											,GLOBAL_ATTRIBUTE15 
											,GLOBAL_ATTRIBUTE16 
											,GLOBAL_ATTRIBUTE17 
											,GLOBAL_ATTRIBUTE18 
											,GLOBAL_ATTRIBUTE19 
											,GLOBAL_ATTRIBUTE20  
											,GLOBAL_ATTRIBUTE_DATE1
											,GLOBAL_ATTRIBUTE_DATE2
											,GLOBAL_ATTRIBUTE_DATE3
											,GLOBAL_ATTRIBUTE_DATE4
											,GLOBAL_ATTRIBUTE_DATE5
											,GLOBAL_ATTRIBUTE_DATE6
											,GLOBAL_ATTRIBUTE_DATE7
											,GLOBAL_ATTRIBUTE_DATE8
											,GLOBAL_ATTRIBUTE_DATE9
											,GLOBAL_ATTRIBUTE_DATE10
											,GLOBAL_ATTRIBUTE_TIMESTAMP1 
											,GLOBAL_ATTRIBUTE_TIMESTAMP2 
											,GLOBAL_ATTRIBUTE_TIMESTAMP3 
											,GLOBAL_ATTRIBUTE_TIMESTAMP4 
											,GLOBAL_ATTRIBUTE_TIMESTAMP5 
											,GLOBAL_ATTRIBUTE_TIMESTAMP6 
											,GLOBAL_ATTRIBUTE_TIMESTAMP7 
											,GLOBAL_ATTRIBUTE_TIMESTAMP8 
											,GLOBAL_ATTRIBUTE_TIMESTAMP9 
											,GLOBAL_ATTRIBUTE_TIMESTAMP10
											,GLOBAL_ATTRIBUTE_NUMBER1 
											,GLOBAL_ATTRIBUTE_NUMBER2 
											,GLOBAL_ATTRIBUTE_NUMBER3 
											,GLOBAL_ATTRIBUTE_NUMBER4 
											,GLOBAL_ATTRIBUTE_NUMBER5 
											,GLOBAL_ATTRIBUTE_NUMBER6 
											,GLOBAL_ATTRIBUTE_NUMBER7 
											,GLOBAL_ATTRIBUTE_NUMBER8 
											,GLOBAL_ATTRIBUTE_NUMBER9 
											,GLOBAL_ATTRIBUTE_NUMBER10
											,PO_ACK_REQD_CODE
											,PO_ACK_REQD_DAYS                                            
                                            ,NULL AS  NULL3
                                            ,batch_id
											,INVOICE_CHANNEL
											,PAYEE_SERVICE_LEVEL_CODE
											,EXCLUSIVE_PARENT_FLAG
                                            ,NULL AS  NULL4
                                            FROM xxcnv_ap_c006_poz_supplier_sites_stg
                                            WHERE import_status = '''
                                 || 'PROCESSED'
                                 || '''
											AND batch_id ='''
                                 || lv_batch_id
                                 || '''
											AND file_reference_identifier= '''
                                 || gv_execution_id
                                 || '_'
                                 || gv_status_success
                                 || ''''
                    );

                    dbms_output.put_line('xxcnv_ap_c006_poz_supplier_sites_stg CSV file batch_id '
                                         || lv_batch_id
                                         || ' exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_fbdi_export_status,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                        p_file_name         => lv_batch_id
                                       || '_'
                                       || gv_oci_file_name_suppsites,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c006_poz_supplier_sites_stg batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for Supplier batch_id '
                                     || lv_batch_id
                                     || ': Error message columns contain data.');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
                RETURN;
        END;

--4th table
        BEGIN
            lv_success_count := 0;
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_ap_c006_poz_sup_site_assign_stg
                WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No batch_id is found for xxcnv_ap_c006_poz_sup_site_assign_stg');
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking batch_id for xxcnv_ap_c006_poz_sup_site_assign_stg ' || sqlerrm);
            END;

            BEGIN
                -- Count the success record count for the current batch_id
                SELECT
                    COUNT(1)
                INTO lv_success_count
                FROM
                    xxcnv_ap_c006_poz_sup_site_assign_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Success record count for xxcnv_ap_c006_poz_sup_site_assign_stg batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_success_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c006_poz_sup_site_assign_stg batch_id: ' || lv_batch_id);
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking success record count for batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
            END;

            IF lv_success_count > 0 THEN
                BEGIN
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                         || '/'
                                         || lv_batch_id
                                         || gv_oci_file_name_suppsitesassign,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
											IMPORT_ACTION,
											vendor_name,
											VENDOR_SITE_CODE,
											PROCUREMENT_BUSINESS_UNIT_NAME,
											BUSINESS_UNIT_NAME,
											BILL_TO_BU_NAME	,
											SHIP_TO_LOCATION_CODE,
											BILL_TO_LOCATION_CODE,
											ALLOW_AWT_LAG,
											AWT_GROUP_NAME,
											ACCTS_PAY_CONCATENATED_SEGMENTS,
											PREPAY_CONCAT_SEGMENTS,
											FUTURE_DATED_CONCAT_SEGMENTS,
											DISTRIBUTION_SET_NAME,
											INACTIVE_DATE,
                                            batch_id
                                            FROM xxcnv_ap_c006_poz_sup_site_assign_stg
                                            WHERE import_status = '''
                                 || 'PROCESSED'
                                 || '''
											AND batch_id ='''
                                 || lv_batch_id
                                 || '''
											AND file_reference_identifier= '''
                                 || gv_execution_id
                                 || '_'
                                 || gv_status_success
                                 || ''''
                    );

                    dbms_output.put_line('xxcnv_ap_c006_poz_sup_site_assign_stg CSV file for batch_id '
                                         || lv_batch_id
                                         || ' exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_fbdi_export_status,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                        p_file_name         => lv_batch_id
                                       || '_'
                                       || gv_oci_file_name_suppsitesassign,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c006_poz_sup_site_assign_stg batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for xxcnv_ap_c006_poz_sup_site_assign_stg batch_id '
                                     || lv_batch_id
                                     || ': Error message columns contain data.');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
        END;

    END create_fbdi_file_prc;

/*==============================================================================================================================
-- PROCEDURE : create_recon_report_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the recon report files by using the data in the staging tables after all validations.
================================================================================================================================= */
    PROCEDURE create_recon_report_prc IS

        CURSOR batch_id_sup IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c006_poz_suppliers_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_sup_address IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c006_poz_supplier_addresses_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_sup_site IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c006_poz_supplier_sites_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_sup_site_assign IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c006_poz_sup_site_assign_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        lv_error_count NUMBER;
        lv_batch_id    VARCHAR(200);
    BEGIN
        BEGIN
-- Table 1
            FOR g_id IN batch_id_sup LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing recon report for xxcnv_ap_c006_poz_suppliers_stg for batch_id: '
                                     || lv_batch_id
                                     || '_'
                                     || gv_oci_file_path
                                     || '_'
                                     || gv_source_folder
                                     || '_'
                                     || gv_recon_folder);

                BEGIN
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                         || '/'
                                         || lv_batch_id
                                         || 'ATP_Recon_Supplier'
                                         || '_'
                                         || sysdate,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600',
                                    'header' VALUE TRUE,
                                    'quote' VALUE '"'
                                ),
                        query           => 'SELECT 
                                            IMPORT_ACTION
											,vendor_name
											,vendor_name_NEW
											,SEGMENT1
											,vendor_name_ALT
											,ORGANIZATION_TYPE_LOOKUP_CODE
											,VENDOR_TYPE_LOOKUP_CODE
											,END_DATE_ACTIVE
											,BUSINESS_RELATIONSHIP
											,PARENT_Supplier_Name
											,ALIAS
											,DUNS_NUMBER
											,ONE_TIME_FLAG
											,CUSTOMER_NUM
											,STANDARD_INDUSTRY_CLASS
											,NI_NUMBER
											,CORPORATE_WEBSITE
											,CHIEF_EXECUTIVE_TITLE
											,CHIEF_EXECUTIVE_NAME
											,BC_NOT_APPLICABLE_FLAG
											,TAX_COUNTRY_CODE
											,NUM_1099
											,FEDERAL_REPORTABLE_FLAG
											,TYPE_1099
											,STATE_REPORTABLE_FLAG
											,TAX_REPORTING_NAME
											,NAME_CONTROL
											,TAX_VERIFICATION_DATE
											,ALLOW_AWT_FLAG
											,AWT_GROUP_NAME
											,VAT_CODE
											,VAT_REGISTRATION_NUM
											,AUTO_TAX_CALC_OVERRIDE
											,PAYMENT_METHOD_LOOKUP_CODE
											,DELIVERY_CHANNEL_CODE
											,BANK_INSTRUCTION1_CODE
											,BANK_INSTRUCTION2_CODE
											,BANK_INSTRUCTION_DETAILS
											,SETTLEMENT_PRIORITY
											,PAYMENT_TEXT_MESSAGE1
											,PAYMENT_TEXT_MESSAGE2
											,PAYMENT_TEXT_MESSAGE3
											,IBY_BANK_VARCHAR2GE_BEARER
											,PAYMENT_REASON_CODE
											,PAYMENT_REASON_COMMENTS
											,PAYMENT_format_CODE
											,ATTRIBUTE_CATEGORY
											,ATTRIBUTE1
											,ATTRIBUTE2
											,ATTRIBUTE3
											,ATTRIBUTE4
											,ATTRIBUTE5
											,ATTRIBUTE6
											,ATTRIBUTE7
											,ATTRIBUTE8
											,ATTRIBUTE9
											,ATTRIBUTE10
											,ATTRIBUTE11
											,ATTRIBUTE12
											,ATTRIBUTE13
											,ATTRIBUTE14
											,ATTRIBUTE15
											,ATTRIBUTE16
											,ATTRIBUTE17
											,ATTRIBUTE18
											,ATTRIBUTE19
											,ATTRIBUTE20
											,ATTRIBUTE_DATE1
											,ATTRIBUTE_DATE2
											,ATTRIBUTE_DATE3
											,ATTRIBUTE_DATE4
											,ATTRIBUTE_DATE5
											,ATTRIBUTE_DATE6
											,ATTRIBUTE_DATE7
											,ATTRIBUTE_DATE8
											,ATTRIBUTE_DATE9
											,ATTRIBUTE_DATE10
											,ATTRIBUTE_TIMESTAMP1
											,ATTRIBUTE_TIMESTAMP2
											,ATTRIBUTE_TIMESTAMP3
											,ATTRIBUTE_TIMESTAMP4
											,ATTRIBUTE_TIMESTAMP5
											,ATTRIBUTE_TIMESTAMP6
											,ATTRIBUTE_TIMESTAMP7
											,ATTRIBUTE_TIMESTAMP8
											,ATTRIBUTE_TIMESTAMP9
											,ATTRIBUTE_TIMESTAMP10
											,ATTRIBUTE_NUMBER1
											,ATTRIBUTE_NUMBER2
											,ATTRIBUTE_NUMBER3
											,ATTRIBUTE_NUMBER4
											,ATTRIBUTE_NUMBER5
											,ATTRIBUTE_NUMBER6
											,ATTRIBUTE_NUMBER7
											,ATTRIBUTE_NUMBER8
											,ATTRIBUTE_NUMBER9
											,ATTRIBUTE_NUMBER10
											,GLOBAL_ATTRIBUTE_CATEGORY
											,GLOBAL_ATTRIBUTE1
											,GLOBAL_ATTRIBUTE2
											,GLOBAL_ATTRIBUTE3
											,GLOBAL_ATTRIBUTE4
											,GLOBAL_ATTRIBUTE5
											,GLOBAL_ATTRIBUTE6
											,GLOBAL_ATTRIBUTE7
											,GLOBAL_ATTRIBUTE8
											,GLOBAL_ATTRIBUTE9
											,GLOBAL_ATTRIBUTE10
											,GLOBAL_ATTRIBUTE11
											,GLOBAL_ATTRIBUTE12
											,GLOBAL_ATTRIBUTE13
											,GLOBAL_ATTRIBUTE14
											,GLOBAL_ATTRIBUTE15
											,GLOBAL_ATTRIBUTE16
											,GLOBAL_ATTRIBUTE17
											,GLOBAL_ATTRIBUTE18
											,GLOBAL_ATTRIBUTE19
											,GLOBAL_ATTRIBUTE20
											,GLOBAL_ATTRIBUTE_DATE1
											,GLOBAL_ATTRIBUTE_DATE2
											,GLOBAL_ATTRIBUTE_DATE3
											,GLOBAL_ATTRIBUTE_DATE4
											,GLOBAL_ATTRIBUTE_DATE5
											,GLOBAL_ATTRIBUTE_DATE6
											,GLOBAL_ATTRIBUTE_DATE7
											,GLOBAL_ATTRIBUTE_DATE8
											,GLOBAL_ATTRIBUTE_DATE9
											,GLOBAL_ATTRIBUTE_DATE10
											,GLOBAL_ATTRIBUTE_TIMESTAMP1
											,GLOBAL_ATTRIBUTE_TIMESTAMP2
											,GLOBAL_ATTRIBUTE_TIMESTAMP3
											,GLOBAL_ATTRIBUTE_TIMESTAMP4
											,GLOBAL_ATTRIBUTE_TIMESTAMP5
											,GLOBAL_ATTRIBUTE_TIMESTAMP6
											,GLOBAL_ATTRIBUTE_TIMESTAMP7
											,GLOBAL_ATTRIBUTE_TIMESTAMP8
											,GLOBAL_ATTRIBUTE_TIMESTAMP9
											,GLOBAL_ATTRIBUTE_TIMESTAMP10
											,GLOBAL_ATTRIBUTE_NUMBER1
											,GLOBAL_ATTRIBUTE_NUMBER2
											,GLOBAL_ATTRIBUTE_NUMBER3
											,GLOBAL_ATTRIBUTE_NUMBER4
											,GLOBAL_ATTRIBUTE_NUMBER5
											,GLOBAL_ATTRIBUTE_NUMBER6
											,GLOBAL_ATTRIBUTE_NUMBER7
											,GLOBAL_ATTRIBUTE_NUMBER8
											,GLOBAL_ATTRIBUTE_NUMBER9
											,GLOBAL_ATTRIBUTE_NUMBER10
											,batch_id
											,PARTY_NUMBER
											,SERVICE_LEVEL_CODE
											,EXCLUSIVE_PAYMENT_FLAG
											,REMIT_ADVICE_DELIVERY_METHOD
											,REMIT_ADVICE_EMAIL
											,REMIT_ADVICE_FAX
											,DATAFOX_COMPANY_ID
                                            ,NULL
									FROM xxcnv_ap_c006_poz_suppliers_stg
                                    where import_status = '''
                                 || 'ERROR'
                                 || '''
									and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c006_poz_suppliers_stg for batch_id '
                                         || lv_batch_id
                                         || ' exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_recon_report,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder),
                        p_file_name         => lv_batch_id
                                       || '_'
                                       || gv_oci_file_name_suppheader,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c006_poz_suppliers_stg for batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);

                        RETURN;
                END;

            END LOOP;
        END;

        BEGIN
-- Table 2
            FOR g_id IN batch_id_sup_address LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing recon report for xxcnv_ap_c006_poz_supplier_addresses_stg for batch_id: '
                                     || lv_batch_id
                                     || '_'
                                     || gv_oci_file_path
                                     || '_'
                                     || gv_source_folder
                                     || '_'
                                     || gv_recon_folder);

                BEGIN
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                         || '/'
                                         || lv_batch_id
                                         || 'ATP_Recon_Supplier_Address'
                                         || '_'
                                         || sysdate,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600',
                                    'header' VALUE TRUE,
                                    'quote' VALUE '"'
                                ),
                        query           => 'SELECT 
											Import_Action,
											vendor_name,
											PARTY_SITE_NAME,
											PARTY_SITE_NAME_NEW,
											COUNTRY,
											ADDRESS_LINE1,
											ADDRESS_LINE2,
											ADDRESS_LINE3,
											ADDRESS_LINE4,
											ADDRESS_LINES_PHONETIC,
											ADDR_ELEMENT_ATTRIBUTE1,
											ADDR_ELEMENT_ATTRIBUTE2,
											ADDR_ELEMENT_ATTRIBUTE3,
											ADDR_ELEMENT_ATTRIBUTE4,
											ADDR_ELEMENT_ATTRIBUTE5,
											BUILDING,
											FLOOR_NUMBER,
											CITY,
											STATE,
											PROVINCE,
											COUNTY,
											POSTAL_CODE,
											POSTAL_PLUS4_CODE,
											ADDRESSEE,
											GLOBAL_LOCATION_NUMBER,
											PARTY_SITE_LANGUAGE,
											INACTIVE_DATE,
											PHONE_COUNTRY_CODE,
											PHONE_AREA_CODE,
											PHONE,
											PHONE_EXTENSION,
											FAX_COUNTRY_CODE,
											FAX_AREA_CODE,
											FAX,
											RFQ_OR_BIDDING_PURPOSE_FLAG,
											ORDERING_PURPOSE_FLAG,
											REMIT_TO_PURPOSE_FLAG,
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
											ATTRIBUTE16,
											ATTRIBUTE17,
											ATTRIBUTE18,
											ATTRIBUTE19,
											ATTRIBUTE20,
											ATTRIBUTE21,
											ATTRIBUTE22,
											ATTRIBUTE23,
											ATTRIBUTE24,
											ATTRIBUTE25,
											ATTRIBUTE26,
											ATTRIBUTE27,
											ATTRIBUTE28,
											ATTRIBUTE29,
											ATTRIBUTE30,
											ATTRIBUTE_NUMBER1,
											ATTRIBUTE_NUMBER2,
											ATTRIBUTE_NUMBER3,
											ATTRIBUTE_NUMBER4,
											ATTRIBUTE_NUMBER5,
											ATTRIBUTE_NUMBER6,
											ATTRIBUTE_NUMBER7,
											ATTRIBUTE_NUMBER8,
											ATTRIBUTE_NUMBER9,
											ATTRIBUTE_NUMBER10,
											ATTRIBUTE_NUMBER11,
											ATTRIBUTE_NUMBER12,
											ATTRIBUTE_DATE1,
											ATTRIBUTE_DATE2,
											ATTRIBUTE_DATE3,
											ATTRIBUTE_DATE4,
											ATTRIBUTE_DATE5,
											ATTRIBUTE_DATE6,
											ATTRIBUTE_DATE7,
											ATTRIBUTE_DATE8,
											ATTRIBUTE_DATE9,
											ATTRIBUTE_DATE10,
											ATTRIBUTE_DATE11,
											ATTRIBUTE_DATE12,
											EMAIL_ADDRESS,
											--DELIVERY_CHANNEL_CODE,
											--BANK_INSTRUCTION1,
											--BANK_INSTRUCTION2,
											--BANK_INSTRUCTION,
											--SETTLEMENT_PRIORITY,
											--PAYMENT_TEXT_MESSAGE1,
											--PAYMENT_TEXT_MESSAGE2,
											--PAYMENT_TEXT_MESSAGE3,
											--SERVICE_LEVEL_CODE,
											--EXCLUSIVE_PAYMENT_FLAG,
											--IBY_BANK_CHARGE_BEARER,
											--PAYMENT_REASON_CODE,
											--PAYMENT_REASON_COMMENTS,
											--REMIT_ADVICE_DELIVERY_METHOD,
											--REMITTANCE_EMAIL,
											--REMIT_ADVICE_FAX,
                                            Batch_ID
									FROM xxcnv_ap_c006_poz_supplier_addresses_stg
                                    where import_status = '''
                                 || 'ERROR'
                                 || '''
									and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c006_poz_supplier_addresses_stg for batch_id '
                                         || lv_batch_id
                                         || ' exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_recon_report,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder),
                        p_file_name         => lv_batch_id
                                       || '_'
                                       || gv_oci_file_name_suppaddress,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c006_poz_supplier_addresses_stg for batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);

                        RETURN;
                END;

            END LOOP;

        END;

-- Table 3
        BEGIN
            FOR g_id IN batch_id_sup_site LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing recon report for xxcnv_ap_c006_poz_supplier_sites_stg for batch_id: '
                                     || lv_batch_id
                                     || '_'
                                     || gv_oci_file_path
                                     || '_'
                                     || gv_source_folder
                                     || '_'
                                     || gv_recon_folder);

                BEGIN
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                         || '/'
                                         || lv_batch_id
                                         || 'ATP_Recon_Supplier_Sites'
                                         || '_'
                                         || sysdate,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600',
                                    'header' VALUE TRUE,
                                    'quote' VALUE '"'
                                ),
                        query           => 'SELECT 
											IMPORT_ACTION 
											,vendor_name
											,PROCUREMENT_BUSINESS_UNIT_NAME
											,PARTY_SITE_NAME
											,VENDOR_SITE_CODE
											,VENDOR_SITE_CODE_NEW
											,INACTIVE_DATE
											,RFQ_ONLY_SITE_FLAG
											,PURCHASING_SITE_FLAG
											,PCARD_SITE_FLAG
											,PAY_SITE_FLAG
											,PRIMARY_PAY_SITE_FLAG
											,TAX_REPORTING_SITE_FLAG
											,VENDOR_SITE_CODE_ALT
											,CUSTOMER_NUM
											,B2B_COMMUNICATION_METHOD
											,B2B_SITE_CODE
											,SUPPLIER_NOTIF_METHOD
											,EMAIL_ADDRESS
											,FAX_COUNTRY_CODE
											,FAX_AREA_CODE
											,FAX
											,HOLD_FLAG
											,PURCHASING_HOLD_REASON
											,CARRIER
											,MODE_OF_TRANSPORT_CODE
											,SERVICE_LEVEL_CODE
											,FREIGHT_TERMS_LOOKUP_CODE
											,PAY_ON_CODE
											,FOB_LOOKUP_CODE
											,COUNTRY_OF_ORIGIN_CODE
											,BUYER_MANAGED_TRANSPORT_FLAG
											,PAY_ON_USE_FLAG
											,AGING_ONSET_POINT
											,AGING_PERIOD_DAYS
											,CONSUMPTION_ADVICE_FREQUENCY
											,CONSUMPTION_ADVICE_SUMMARY
											,DEFAULT_PAY_SITE_CODE
											,PAY_ON_RECEIPT_SUMMARY_CODE
											,GAPLESS_INV_NUM_FLAG
											,SELLING_COMPANY_IDENTIFIER
											,CREATE_DEBIT_MEMO_FLAG
											,ENFORCE_SHIP_TO_LOCATION_CODE
											,RECEIVING_ROUTING_ID
											,QTY_RCV_TOLERANCE
											,QTY_RCV_EXCEPTION_CODE
											,DAYS_EARLY_RECEIPT_ALLOWED
											,DAYS_LATE_RECEIPT_ALLOWED
											,ALLOW_SUBSTITUTE_RECEIPTS_FLAG
											,ALLOW_UNORDERED_RECEIPTS_FLAG
											,RECEIPT_DAYS_EXCEPTION_CODE
											,INVOICE_CURRENCY_CODE
											,INVOICE_AMOUNT_LIMIT
											,MATCH_OPTION
											,MATCH_APPROVAL_LEVEL
											,PAYMENT_CURRENCY_CODE
											,PAYMENT_PRIORITY
											,PAY_GROUP_LOOKUP_CODE
											,TOLERANCE_NAME  
											,SERVICES_TOLERANCE 
											,HOLD_ALL_PAYMENTS_FLAG
											,HOLD_UNMATCHED_INVOICES_FLAG
											,HOLD_FUTURE_PAYMENTS_FLAG
											,HOLD_BY
											,PAYMENT_HOLD_DATE 
											,HOLD_REASON
											,TERMS_NAME 
											,PAY_DATE_BASIS_LOOKUP_CODE 
											,BANK_CHARGE_DEDUCTION_TYPE
											,TERMS_DATE_BASICS 
											,ALWAYS_TAKE_DISC_FLAG
											,EXCLUDE_FREIGHT_FROM_DISCOUNT
											,EXCLUDE_TAX_FROM_DISCOUNT
											,AUTO_CALCULATE_INTEREST_FLAG
                                            ,NULL AS  NULL1
                                            ,NULL AS  NULL2
											,PAYMENT_METHOD_LOOKUP_CODE
											,DELIVERY_CHANNEL_CODE
											,BANK_INSTRUCTION1_CODE
											,BANK_INSTRUCTION2_CODE
											,BANK_INSTRUCTION_DETAILS
											,SETTLEMENT_PRIORITY
											,PAYMENT_TEXT_MESSAGE1
											,PAYMENT_TEXT_MESSAGE2 
											,PAYMENT_TEXT_MESSAGE3  
											,IBY_BANK_VARCHAR2GE_BEARER
											,PAYMENT_REASON_CODE     
											,PAYMENT_REASON_COMMENTS   
											,REMIT_ADVICE_DELIVERY_METHOD
											,REMITTANCE_EMAIL
											,REMIT_ADVICE_FAX 
											,ATTRIBUTE_CATEGORY        
											,ATTRIBUTE1         
											,ATTRIBUTE2       
											,ATTRIBUTE3
											,ATTRIBUTE4
											,ATTRIBUTE5
											,ATTRIBUTE6
											,ATTRIBUTE7
											,ATTRIBUTE8
											,ATTRIBUTE9
											,ATTRIBUTE10
											,ATTRIBUTE11
											,ATTRIBUTE12
											,ATTRIBUTE13
											,ATTRIBUTE14
											,ATTRIBUTE15
											,ATTRIBUTE16
											,ATTRIBUTE17
											,ATTRIBUTE18
											,ATTRIBUTE19
											,ATTRIBUTE20
											,ATTRIBUTE_DATE1
											,ATTRIBUTE_DATE2 
											,ATTRIBUTE_DATE3
											,ATTRIBUTE_DATE4
											,ATTRIBUTE_DATE5
											,ATTRIBUTE_DATE6
											,ATTRIBUTE_DATE7
											,ATTRIBUTE_DATE8
											,ATTRIBUTE_DATE9
											,ATTRIBUTE_DATE10
											,ATTRIBUTE_TIMESTAMP1
											,ATTRIBUTE_TIMESTAMP2
											,ATTRIBUTE_TIMESTAMP3
											,ATTRIBUTE_TIMESTAMP4
											,ATTRIBUTE_TIMESTAMP5
											,ATTRIBUTE_TIMESTAMP6
											,ATTRIBUTE_TIMESTAMP7
											,ATTRIBUTE_TIMESTAMP8
											,ATTRIBUTE_TIMESTAMP9
											,ATTRIBUTE_TIMESTAMP10
											,ATTRIBUTE_NUMBER1   
											,ATTRIBUTE_NUMBER2  
											,ATTRIBUTE_NUMBER3  
											,ATTRIBUTE_NUMBER4  
											,ATTRIBUTE_NUMBER5  
											,ATTRIBUTE_NUMBER6  
											,ATTRIBUTE_NUMBER7  
											,ATTRIBUTE_NUMBER8  
											,ATTRIBUTE_NUMBER9  
											,ATTRIBUTE_NUMBER10 
											,GLOBAL_ATTRIBUTE_CATEGORY  
											,GLOBAL_ATTRIBUTE1  
											,GLOBAL_ATTRIBUTE2  
											,GLOBAL_ATTRIBUTE3  
											,GLOBAL_ATTRIBUTE4  
											,GLOBAL_ATTRIBUTE5  
											,GLOBAL_ATTRIBUTE6  
											,GLOBAL_ATTRIBUTE7  
											,GLOBAL_ATTRIBUTE8  
											,GLOBAL_ATTRIBUTE9  
											,GLOBAL_ATTRIBUTE10 
											,GLOBAL_ATTRIBUTE11 
											,GLOBAL_ATTRIBUTE12 
											,GLOBAL_ATTRIBUTE13 
											,GLOBAL_ATTRIBUTE14 
											,GLOBAL_ATTRIBUTE15 
											,GLOBAL_ATTRIBUTE16 
											,GLOBAL_ATTRIBUTE17 
											,GLOBAL_ATTRIBUTE18 
											,GLOBAL_ATTRIBUTE19 
											,GLOBAL_ATTRIBUTE20  
											,GLOBAL_ATTRIBUTE_DATE1
											,GLOBAL_ATTRIBUTE_DATE2
											,GLOBAL_ATTRIBUTE_DATE3
											,GLOBAL_ATTRIBUTE_DATE4
											,GLOBAL_ATTRIBUTE_DATE5
											,GLOBAL_ATTRIBUTE_DATE6
											,GLOBAL_ATTRIBUTE_DATE7
											,GLOBAL_ATTRIBUTE_DATE8
											,GLOBAL_ATTRIBUTE_DATE9
											,GLOBAL_ATTRIBUTE_DATE10
											,GLOBAL_ATTRIBUTE_TIMESTAMP1 
											,GLOBAL_ATTRIBUTE_TIMESTAMP2 
											,GLOBAL_ATTRIBUTE_TIMESTAMP3 
											,GLOBAL_ATTRIBUTE_TIMESTAMP4 
											,GLOBAL_ATTRIBUTE_TIMESTAMP5 
											,GLOBAL_ATTRIBUTE_TIMESTAMP6 
											,GLOBAL_ATTRIBUTE_TIMESTAMP7 
											,GLOBAL_ATTRIBUTE_TIMESTAMP8 
											,GLOBAL_ATTRIBUTE_TIMESTAMP9 
											,GLOBAL_ATTRIBUTE_TIMESTAMP10
											,GLOBAL_ATTRIBUTE_NUMBER1 
											,GLOBAL_ATTRIBUTE_NUMBER2 
											,GLOBAL_ATTRIBUTE_NUMBER3 
											,GLOBAL_ATTRIBUTE_NUMBER4 
											,GLOBAL_ATTRIBUTE_NUMBER5 
											,GLOBAL_ATTRIBUTE_NUMBER6 
											,GLOBAL_ATTRIBUTE_NUMBER7 
											,GLOBAL_ATTRIBUTE_NUMBER8 
											,GLOBAL_ATTRIBUTE_NUMBER9 
											,GLOBAL_ATTRIBUTE_NUMBER10
											,PO_ACK_REQD_CODE
											,PO_ACK_REQD_DAYS                                            
                                            ,NULL AS  NULL3
                                            ,batch_id
											,INVOICE_CHANNEL
											,PAYEE_SERVICE_LEVEL_CODE
											,EXCLUSIVE_PARENT_FLAG
                                            ,NULL AS  NULL4
                                            FROM xxcnv_ap_c006_poz_supplier_sites_stg
											where import_status = '''
                                 || 'ERROR'
                                 || '''
											and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c006_poz_supplier_sites_stg for batch_id '
                                         || lv_batch_id
                                         || ' exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_recon_report,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder),
                        p_file_name         => lv_batch_id
                                       || '_'
                                       || gv_oci_file_name_suppsites,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c006_poz_supplier_sites_stg for batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);

                        RETURN;
                END;

            END LOOP;
        END;

-- Table 4
        BEGIN
            FOR g_id IN batch_id_sup_site_assign LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing recon report for xxcnv_ap_c006_poz_sup_site_assign_stg for batch_id: '
                                     || lv_batch_id
                                     || '_'
                                     || gv_oci_file_path
                                     || '_'
                                     || gv_source_folder
                                     || '_'
                                     || gv_recon_folder);

                BEGIN
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                         || '/'
                                         || lv_batch_id
                                         || 'ATP_Recon_Supplier_Site_Assignments'
                                         || '_'
                                         || sysdate,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600',
                                    'header' VALUE TRUE,
                                    'quote' VALUE '"'
                                ),
                        query           => 'SELECT 
											IMPORT_ACTION,
											vendor_name,
											VENDOR_SITE_CODE,
											PROCUREMENT_BUSINESS_UNIT_NAME,
											BUSINESS_UNIT_NAME,
											BILL_TO_BU_NAME	,
											SHIP_TO_LOCATION_CODE,
											BILL_TO_LOCATION_CODE,
											ALLOW_AWT_LAG,
											AWT_GROUP_NAME,
											ACCTS_PAY_CONCATENATED_SEGMENTS,
											PREPAY_CONCAT_SEGMENTS,
											FUTURE_DATED_CONCAT_SEGMENTS,
											DISTRIBUTION_SET_NAME,
											INACTIVE_DATE,
                                            batch_id
                                            FROM xxcnv_ap_c006_poz_sup_site_assign_stg
											where import_status = '''
                                 || 'ERROR'
                                 || '''
											and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c006_poz_sup_site_assign_stg for batch_id '
                                         || lv_batch_id
                                         || ' exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_recon_report,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder),
                        p_file_name         => lv_batch_id
                                       || '_'
                                       || gv_oci_file_name_suppsitesassign,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c006_poz_sup_site_assign_stg for batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);

                        RETURN;
                END;

            END LOOP;
        END;

    END create_recon_report_prc;

END xxcnv_ap_c006_employee_as_supplier_conversion_pkg;