create or replace PACKAGE BODY xxcnv.xxcnv_ap_c003_supplier_conversion_pkg IS

    /*************************************************************************************
    NAME              :     Supplier Master Conversion
    PURPOSE           :     This package is the detailed body of all the procedures.
    -- Modification History
    -- Developer          Date         Version     Comments and changes made
    -- -------------   ------       ----------  -----------------------------------------
    -- Satya Pavani   27-Mar-2025        1.0         Initial Development
	-- Satya Pavani   11-Sep-2025        1.1         LTCI-8926 - Update Payment_reason_comments field format
    ****************************************************************************************/

---Declaring global Variables
    gv_data_validated_success           CONSTANT VARCHAR2(50) := 'Data_Validated';
    gv_data_validated_failure           CONSTANT VARCHAR2(50) := 'Data_Not_Validated';
    gv_import_status                    VARCHAR2(256) := NULL;
    gv_error_message                    VARCHAR2(500) := NULL;
    gv_file_name                        VARCHAR2(256) := NULL;
    gv_oci_file_path                    VARCHAR2(600) := NULL;
    gv_oci_file_name                    VARCHAR2(2000) := NULL;
    gv_execution_id                     VARCHAR2(300) := NULL;
    gv_group_id                         NUMBER(18) := NULL;
    gv_batch_id                         VARCHAR2(30) := NULL;
    gv_credential_name                  CONSTANT VARCHAR2(30) := 'OCI$RESOURCE_PRINCIPAL';
    gv_status_success                   CONSTANT VARCHAR2(15) := 'Success';
    gv_status_failure                   CONSTANT VARCHAR2(15) := 'Failure';
    gv_conversion_id                    VARCHAR2(15) := NULL;
    gv_boundary_system                  VARCHAR2(25) := NULL;
    gv_status_picked                    CONSTANT VARCHAR2(50) := 'File_Picked_From_OCI_AND_Loaded_To_Stg';
    gv_status_picked_for_tr             CONSTANT VARCHAR2(50) := 'Transformed_Data_From_Ext_To_Stg';
    gv_status_validated                 CONSTANT VARCHAR2(50) := 'Validated';
    gv_status_failed                    CONSTANT VARCHAR2(50) := 'Failed_At_Validation';
    gv_coa_transformation               CONSTANT VARCHAR2(50) := 'COA_Transformation';
    gv_fbdi_export_status               CONSTANT VARCHAR2(50) := 'Exported_To_Fbdi';
    gv_status_staged                    CONSTANT VARCHAR2(50) := 'Staged_For_Import';
    gv_transformed_folder               CONSTANT VARCHAR2(50) := 'Transformed_FBDI_Files';
    gv_source_folder                    CONSTANT VARCHAR2(50) := 'Source_FBDI_Files';
    gv_properties                       CONSTANT VARCHAR2(50) := 'Properties';
    gv_file_picked                      VARCHAR2(50) := 'File_Picked_From_OCI_Server';
    gv_status_failed_validation         CONSTANT VARCHAR2(50) := 'Failed_Validation';
    gv_fbdi_export_fail                 CONSTANT VARCHAR2(50) := 'Failed_In_FBDI';
    gv_properties_fail                  CONSTANT VARCHAR2(50) := 'Failed_In_Properties';
    gv_recon_folder                     CONSTANT VARCHAR2(50) := 'ATP_Validation_Error_Files';
    gv_recon_report                     CONSTANT VARCHAR2(50) := 'Recon_Report_Created';
    gv_oci_file_name_suppheader         VARCHAR2(100) := NULL;
    gv_oci_file_name_suppcontacts       VARCHAR2(100) := NULL;
    gv_oci_file_name_suppcontactaddress VARCHAR2(100) := NULL;
    gv_oci_file_name_suppaddress        VARCHAR2(100) := NULL;
    gv_oci_file_name_suppsites          VARCHAR2(100) := NULL;
    gv_oci_file_name_suppsitesassign    VARCHAR2(100) := NULL;
    gv_oci_file_name_suppclassstg       VARCHAR2(100) := NULL;

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
                        WHEN lv_file_name LIKE '%PozSupContactsInt%.csv' THEN
                            gv_oci_file_name_suppcontacts := lv_file_name;
                        WHEN lv_file_name LIKE '%PozSupplierAddressesInt%.csv' THEN
                            gv_oci_file_name_suppaddress := lv_file_name;
                        WHEN lv_file_name LIKE '%PozSupplierSitesInt%.csv' THEN
                            gv_oci_file_name_suppsites := lv_file_name;
                        WHEN lv_file_name LIKE '%PozSupBusClassInt%.csv' THEN
                            gv_oci_file_name_suppclassstg := lv_file_name;
                        WHEN lv_file_name LIKE '%PozSiteAssignmentsInt%.csv' THEN
                            gv_oci_file_name_suppsitesassign := lv_file_name;
                        WHEN lv_file_name LIKE '%PozSupContactAddressesInt%.csv' THEN
                            gv_oci_file_name_suppcontactaddress := lv_file_name;
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
            BEGIN
        -- Check if the external table exists AND drop it if it does
                SELECT
                    COUNT(*)
                INTO lv_table_count
                FROM
                    all_objects
                WHERE
                        upper(object_name) = 'XXCNV_AP_C003_POZ_SUPPLIERS_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c003_poz_suppliers_ext';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c003_poz_suppliers_stg';
                    dbms_output.put_line('Table xxcnv_ap_c003_poz_suppliers_ext dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_ap_c003_poz_suppliers_ext:'
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
                        upper(object_name) = 'XXCNV_AP_C003_POZ_SUPPLIER_ADDRESSES_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c003_poz_supplier_addresses_ext';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c003_poz_supplier_addresses_stg';
                    dbms_output.put_line('Table xxcnv_ap_c003_poz_supplier_addresses_ext dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_ap_c003_poz_supplier_addresses_ext: '
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
                        upper(object_name) = 'XXCNV_AP_C003_POZ_SUPPLIER_SITES_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c003_poz_supplier_sites_ext';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c003_poz_supplier_sites_stg';
                    dbms_output.put_line('Table xxcnv_ap_c003_poz_supplier_sites_ext dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_ap_c003_poz_supplier_sites_ext: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    p_loading_status := gv_status_failure;
			--RETURN;		

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
                        upper(object_name) = 'XXCNV_AP_C003_POZ_SUP_CONTACTS_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c003_poz_sup_contacts_ext';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c003_poz_sup_contacts_stg';
                    dbms_output.put_line('Table xxcnv_ap_c003_poz_sup_contacts_ext dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_ap_c003_poz_sup_contacts_ext: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    p_loading_status := gv_status_failure;
			--RETURN;

            END;

--table5

            BEGIN
                lv_table_count := 0;
                SELECT
                    COUNT(*)
                INTO lv_table_count
                FROM
                    all_objects
                WHERE
                        upper(object_name) = 'XXCNV_AP_C003_POZ_SUP_SITE_ASSIGN_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c003_poz_sup_site_assign_ext';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c003_poz_sup_site_assign_stg';
                    dbms_output.put_line('Table xxcnv_ap_c003_poz_sup_site_assign_ext dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_ap_c003_poz_sup_site_assign_ext: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    p_loading_status := gv_status_failure;
			--RETURN;

            END;

--table6
            BEGIN
                lv_table_count := 0;
                SELECT
                    COUNT(*)
                INTO lv_table_count
                FROM
                    all_objects
                WHERE
                        upper(object_name) = 'XXCNV_AP_C003_POZ_SUP_CONT_ADDR_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c003_poz_sup_cont_addr_ext';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c003_poz_sup_cont_addr_stg';
                    dbms_output.put_line('Table xxcnv_ap_c003_poz_sup_cont_addr_ext dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_ap_c003_poz_sup_cont_addr_ext: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    p_loading_status := gv_status_failure;
			--RETURN;
            END;

---table7 

            BEGIN
                lv_table_count := 0;
                SELECT
                    COUNT(*)
                INTO lv_table_count
                FROM
                    all_objects
                WHERE
                        upper(object_name) = 'XXCNV_AP_C003_POZ_SUP_BUS_CLASS_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c003_poz_sup_bus_class_ext';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c003_poz_sup_bus_class_stg';
                    dbms_output.put_line('Table xxcnv_ap_c003_poz_sup_bus_class_ext dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_ap_c003_poz_sup_bus_class_ext: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    p_loading_status := gv_status_failure;
			--RETURN;	
            END;

        END;

    -- Create the external table
        BEGIN
            IF gv_oci_file_name_suppheader IS NOT NULL THEN
                dbms_output.put_line('Creating an external table:'
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppheader);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c003_poz_suppliers_ext',
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

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c003_poz_suppliers_stg (
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
                                        ,NULL
										,NULL
										FROM xxcnv_ap_c003_poz_suppliers_ext';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c003_poz_suppliers_stg: ' || SQL%rowcount);
            END IF;

--TABLE2

            IF gv_oci_file_name_suppaddress IS NOT NULL THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c003_poz_supplier_addresses_stg');
                dbms_output.put_line(' xxcnv_ap_c003_poz_supplier_addresses_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppaddress);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c003_poz_supplier_addresses_ext',
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
                    column_list     => 'batch_id	VARCHAR2(200),
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

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c003_poz_supplier_addresses_stg (
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
						--'
                                  || chr(39)
                                  || gv_execution_id
                                  || chr(39)
                                  || ' ,
                        NULL,
                        NULL
						FROM xxcnv_ap_c003_poz_supplier_addresses_ext';

                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c003_poz_supplier_addresses_stg: ' || SQL%rowcount);
            END IF;

--TABLE3

            IF gv_oci_file_name_suppsites IS NOT NULL THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c003_poz_supplier_sites_stg');
                dbms_output.put_line(' xxcnv_ap_c003_poz_supplier_sites_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppsites);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c003_poz_supplier_sites_ext',
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
                    column_list     => 'batch_id	VARCHAR2(200),
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

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c003_poz_supplier_sites_stg (
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
					FROM xxcnv_ap_c003_poz_supplier_sites_ext';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c003_poz_supplier_sites_stg: ' || SQL%rowcount);
            END IF;

--TABLE4

            IF gv_oci_file_name_suppcontacts IS NOT NULL THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c003_poz_sup_contacts_ext');
                dbms_output.put_line(' xxcnv_ap_c003_poz_sup_contacts_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppcontacts);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c003_poz_sup_contacts_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppcontacts,
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
						PREFIX	VARCHAR2(30)	,
						first_name	VARCHAR2(150)	,
						first_name_NEW	VARCHAR2(150)	,
						MIDDLE_NAME	VARCHAR2(60)	,
						LAST_NAME	VARCHAR2(150)	,
						LAST_NAME_NEW	VARCHAR2(150)	,
						TITLE	VARCHAR2(100)	,
						PRIMARY_ADMIN_CONTACT	VARCHAR2(1)	,
						EMAIL_ADDRESS	VARCHAR2(320)	,
						EMAIL_ADDRESS_NEW	VARCHAR2(320)	,
						PHONE_COUNTRY_CODE	VARCHAR2(10)	,
						AREA_CODE	VARCHAR2(10)	,
						PHONE	VARCHAR2(40)	,
						PHONE_EXTENSION	VARCHAR2(20)	,
						FAX_COUNTRY_CODE	VARCHAR2(10)	,
						FAX_AREA_CODE	VARCHAR2(10)	,
						FAX	VARCHAR2(40)	,
						MOBILE_COUNTRY_CODE	VARCHAR2(10)	,
						MOBILE_AREA_CODE	VARCHAR2(10)	,
						MOBILE_NUMBER	VARCHAR2(40)	,
						INACTIVE_DATE	DATE	,
						ATTRIBUTE_CATEGORY	VARCHAR2(30)	,
						ATTRIBUTE1	VARCHAR2(150)	,
						ATTRIBUTE2	VARCHAR2(150)	,
						ATTRIBUTE3	VARCHAR2(150)	,
						ATTRIBUTE4	VARCHAR2(150)	,
						ATTRIBUTE5	VARCHAR2(150)	,
						ATTRIBUTE6	VARCHAR2(150)	,
						ATTRIBUTE7	VARCHAR2(150)	,
						ATTRIBUTE8	VARCHAR2(150)	,
						ATTRIBUTE9	VARCHAR2(150)	,
						ATTRIBUTE10	VARCHAR2(150)	,
						ATTRIBUTE11	VARCHAR2(150)	,
						ATTRIBUTE12	VARCHAR2(150)	,
						ATTRIBUTE13	VARCHAR2(150)	,
						ATTRIBUTE14	VARCHAR2(150)	,
						ATTRIBUTE15	VARCHAR2(150)	,
						ATTRIBUTE16	VARCHAR2(150)	,
						ATTRIBUTE17	VARCHAR2(150)	,
						ATTRIBUTE18	VARCHAR2(150)	,
						ATTRIBUTE19	VARCHAR2(150)	,
						ATTRIBUTE20	VARCHAR2(150)	,
						ATTRIBUTE21	VARCHAR2(150)	,
						ATTRIBUTE22	VARCHAR2(150)	,
						ATTRIBUTE23	VARCHAR2(150)	,
						ATTRIBUTE24	VARCHAR2(150)	,
						ATTRIBUTE25	VARCHAR2(150)	,
						ATTRIBUTE26	VARCHAR2(150)	,
						ATTRIBUTE27	VARCHAR2(150)	,
						ATTRIBUTE28	VARCHAR2(150)	,
						ATTRIBUTE29	VARCHAR2(150)	,
						ATTRIBUTE30	VARCHAR2(150)	,
						ATTRIBUTE_NUMBER1	NUMBER	,
						ATTRIBUTE_NUMBER2	NUMBER	,
						ATTRIBUTE_NUMBER3	NUMBER	,
						ATTRIBUTE_NUMBER4	NUMBER	,
						ATTRIBUTE_NUMBER5	NUMBER	,
						ATTRIBUTE_NUMBER6	NUMBER	,
						ATTRIBUTE_NUMBER7	NUMBER	,
						ATTRIBUTE_NUMBER8	NUMBER	,
						ATTRIBUTE_NUMBER9	NUMBER	,
						ATTRIBUTE_NUMBER10	NUMBER	,
						ATTRIBUTE_NUMBER11	NUMBER	,
						ATTRIBUTE_NUMBER12	NUMBER	,
						ATTRIBUTE_DATE1	DATE	,
						ATTRIBUTE_DATE2	DATE	,
						ATTRIBUTE_DATE3	DATE	,
						ATTRIBUTE_DATE4	DATE	,
						ATTRIBUTE_DATE5	DATE	,
						ATTRIBUTE_DATE6	DATE	,
						ATTRIBUTE_DATE7	DATE	,
						ATTRIBUTE_DATE8	DATE	,
						ATTRIBUTE_DATE9	DATE	,
						ATTRIBUTE_DATE10	DATE	,
						ATTRIBUTE_DATE11	DATE	,
						ATTRIBUTE_DATE12	DATE	,
						USER_ACCOUNT_ACTION	VARCHAR2(100)	,
						ROLE1	VARCHAR2(4000)	,
						ROLE2	VARCHAR2(4000)	,
						ROLE3	VARCHAR2(4000)	,
						ROLE4	VARCHAR2(4000)	,
						ROLE5	VARCHAR2(4000)	,
						ROLE6	VARCHAR2(4000)	,
						ROLE7	VARCHAR2(4000)	,
						ROLE8	VARCHAR2(4000)	,
						ROLE9	VARCHAR2(4000)	,
						ROLE10	VARCHAR2(4000)'
                );

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c003_poz_sup_contacts_stg (
						IMPORT_ACTION ,
						vendor_name,
						PREFIX,
						first_name,
						first_name_NEW,
						MIDDLE_NAME,
						LAST_NAME,
						LAST_NAME_NEW,
						TITLE,
						PRIMARY_ADMIN_CONTACT,
						EMAIL_ADDRESS,
						EMAIL_ADDRESS_NEW,
						PHONE_COUNTRY_CODE,
						AREA_CODE,
						PHONE,
						PHONE_EXTENSION,
						FAX_COUNTRY_CODE,
						FAX_AREA_CODE,
						FAX,
						MOBILE_COUNTRY_CODE,
						MOBILE_AREA_CODE,
						MOBILE_NUMBER,
						INACTIVE_DATE,
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
						batch_id,
						USER_ACCOUNT_ACTION,
						ROLE1,
						ROLE2,
						ROLE3,
						ROLE4,
						ROLE5,
						ROLE6,
						ROLE7,
						ROLE8,
						ROLE9,
						ROLE10,
						FILE_NAME,	
						error_message,	
						IMPORT_STATUS,	
                        FILE_REFERENCE_IDENTIFIER,
						EXECUTION_ID,						
						SOURCE_SYSTEM
						 )
						SELECT 
						IMPORT_ACTION ,
						vendor_name,
						PREFIX,
						first_name,
						first_name_NEW,
						MIDDLE_NAME,
						LAST_NAME,
						LAST_NAME_NEW,
						TITLE,
						PRIMARY_ADMIN_CONTACT,
						EMAIL_ADDRESS,
						EMAIL_ADDRESS_NEW,
						PHONE_COUNTRY_CODE,
						AREA_CODE,
						PHONE,
						PHONE_EXTENSION,
						FAX_COUNTRY_CODE,
						FAX_AREA_CODE,
						FAX,
						MOBILE_COUNTRY_CODE,
						MOBILE_AREA_CODE,
						MOBILE_NUMBER,
						INACTIVE_DATE,
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
						batch_id,
						USER_ACCOUNT_ACTION,
						ROLE1,
						ROLE2,
						ROLE3,
						ROLE4,
						ROLE5,
						ROLE6,
						ROLE7,
						ROLE8,
						ROLE9,
						ROLE10,
						NULL,
						NULL,
						NULL,
						NULL
						,NULL	
                        ,NULL
						FROM xxcnv_ap_c003_poz_sup_contacts_ext';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c003_poz_sup_contacts_stg: ' || SQL%rowcount);
            END IF;

--TABLE5

            IF gv_oci_file_name_suppsitesassign IS NOT NULL THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c003_poz_sup_site_assign_ext');
                dbms_output.put_line(' xxcnv_ap_c003_poz_sup_site_assign_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppsitesassign);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c003_poz_sup_site_assign_ext',
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
                    column_list     => 'batch_id	VARCHAR2(200)
											,IMPORT_ACTION 	VARCHAR2(10)	,
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

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c003_poz_sup_site_assign_stg (
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
						FROM xxcnv_ap_c003_poz_sup_site_assign_ext';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c003_poz_sup_site_assign_stg: ' || SQL%rowcount);
            END IF;

--TABLE6

            IF gv_oci_file_name_suppclassstg IS NOT NULL THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c003_poz_sup_bus_class_ext');
                dbms_output.put_line(' xxcnv_ap_c003_poz_sup_bus_class_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppclassstg);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c003_poz_sup_bus_class_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppclassstg,
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
					CLASSIFICATION_LOOKUP_CODE	VARCHAR2(30)	,
					CLASSIFICATION_LOOKUP_CODE_NEW	VARCHAR2(30)	,
					SUB_CLASSIFICATION VARCHAR2(240)	,
					CERTIFYING_AGENCY_NAME	VARCHAR2(255)	,
					CERTIFYING_AGENCY_NAME_NEW	VARCHAR2(255)	,
					CREATE_CERTIFYING_AGENCY_FLAG	VARCHAR2(1)	,
					CERTIFICATE_NUMBER	VARCHAR2(80)	,
					CERTIFICATE_NUMBER_NAME	VARCHAR2(80)	,
					START_DATE	DATE	,
					EXPIRATION_DATE	DATE	,
					NOTES	VARCHAR2(1000)	,
					PROVIDED_BY_CONTACT_first_name VARCHAR2(150)	,
					PROVIDED_BY_CONTACT_LAST_NAME	VARCHAR2(150)	,
					PROVIDED_BY_CONTACT_EMAIL 		VARCHAR2(320)   ,
					CONFIRMED_ON				DATE'
                );

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c003_poz_sup_bus_class_stg (
						IMPORT_ACTION,
						vendor_name	,
						CLASSIFICATION_LOOKUP_CODE,
						CLASSIFICATION_LOOKUP_CODE_NEW,
						SUB_CLASSIFICATION ,
						CERTIFYING_AGENCY_NAME,
						CERTIFYING_AGENCY_NAME_NEW,
						CREATE_CERTIFYING_AGENCY_FLAG,
						CERTIFICATE_NUMBER,
						CERTIFICATE_NUMBER_NAME,
						START_DATE,
						EXPIRATION_DATE,
						NOTES,
						PROVIDED_BY_CONTACT_first_name,
						PROVIDED_BY_CONTACT_LAST_NAME,
						PROVIDED_BY_CONTACT_EMAIL,
						CONFIRMED_ON,
						batch_id,
						FILE_NAME,
						error_message,
						IMPORT_STATUS,
                        FILE_REFERENCE_IDENTIFIER,
						EXECUTION_ID,						
						SOURCE_SYSTEM)
					SELECT 
						IMPORT_ACTION,
						vendor_name,
						CLASSIFICATION_LOOKUP_CODE,
						CLASSIFICATION_LOOKUP_CODE_NEW,
						SUB_CLASSIFICATION,
						CERTIFYING_AGENCY_NAME,
						CERTIFYING_AGENCY_NAME_NEW,
						CREATE_CERTIFYING_AGENCY_FLAG,
						CERTIFICATE_NUMBER,
						CERTIFICATE_NUMBER_NAME,
						START_DATE,
						EXPIRATION_DATE,
						NOTES,
						PROVIDED_BY_CONTACT_first_name,
						PROVIDED_BY_CONTACT_LAST_NAME,
						PROVIDED_BY_CONTACT_EMAIL ,
						CONFIRMED_ON,
						batch_id,
						NULL,
						NULL,
						NULL,
						NULL						
                        ,NULL	
                        ,NULL
						FROM xxcnv_ap_c003_poz_sup_bus_class_ext';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c003_poz_sup_bus_class_stg: ' || SQL%rowcount);
            END IF;

--TABLE 7 

            IF gv_oci_file_name_suppcontactaddress IS NOT NULL THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c003_poz_sup_cont_addr_ext');
                dbms_output.put_line(' xxcnv_ap_c003_poz_sup_cont_addr_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppcontactaddress);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c003_poz_sup_cont_addr_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_suppcontactaddress,
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
				   IMPORT_ACTION             VARCHAR2(10),
					vendor_name               VARCHAR2(360),
					PARTY_SITE_NAME           VARCHAR2(240),
					first_name                VARCHAR2(150),
					LAST_NAME                 VARCHAR2(150),
					EMAIL_ADDRESS             VARCHAR2(320)'
                );

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c003_poz_sup_cont_addr_stg (
                    IMPORT_ACTION,
					vendor_name,
					PARTY_SITE_NAME,
					first_name,
					LAST_NAME,
					EMAIL_ADDRESS,
					batch_id,
					FILE_NAME,
					error_message,
					IMPORT_STATUS,
                    FILE_REFERENCE_IDENTIFIER,
					EXECUTION_ID,					
					SOURCE_SYSTEM)
				SELECT 
					IMPORT_ACTION,
					vendor_name,
					PARTY_SITE_NAME,
					first_name,
					LAST_NAME,
					EMAIL_ADDRESS,
					batch_id,
					NULL,
					NULL,
					NULL,
                    NULL
					,NULL	
                    ,NULL
					FROM xxcnv_ap_c003_poz_sup_cont_addr_ext';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c003_poz_sup_cont_addr_stg: ' || SQL%rowcount);
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
                    xxcnv_ap_c003_poz_suppliers_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c003_poz_suppliers_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;
--TABLE 2	
            IF gv_oci_file_name_suppaddress IS NOT NULL THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c003_poz_supplier_addresses_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c003_poz_supplier_addresses_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;
--TABLE 3		
            IF gv_oci_file_name_suppsites IS NOT NULL THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c003_poz_supplier_sites_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c003_poz_supplier_sites_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;
--TABLE 4
            IF gv_oci_file_name_suppcontacts IS NOT NULL THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c003_poz_sup_contacts_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c003_poz_sup_contacts_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF; 

--TABLE 5
            IF gv_oci_file_name_suppsitesassign IS NOT NULL THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c003_poz_sup_site_assign_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c003_poz_sup_site_assign_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF; 
--TABLE 6
            IF gv_oci_file_name_suppclassstg IS NOT NULL THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c003_poz_sup_bus_class_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c003_poz_sup_bus_class_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF; 
--TABLE 7
            IF gv_oci_file_name_suppcontactaddress IS NOT NULL THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c003_poz_sup_cont_addr_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c003_poz_sup_cont_addr_stg from OCI Source Folder: ' || lv_row_count
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
                xxcnv_ap_c003_poz_suppliers_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c003_poz_suppliers_stg from external table: ' || lv_row_count)
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
                dbms_output.put_line('Error counting rows in xxcnv_ap_c003_poz_suppliers_stg: ' || sqlerrm);
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
                xxcnv_ap_c003_poz_supplier_addresses_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c003_poz_supplier_addresses_stg from external table: ' || lv_row_count
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
                dbms_output.put_line('Error counting rows in xxcnv_ap_c003_poz_supplier_addresses_stg: ' || sqlerrm);
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
                xxcnv_ap_c003_poz_supplier_sites_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c003_poz_supplier_sites_stg from external table: ' || lv_row_count
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
                dbms_output.put_line('Error counting rows in xxcnv_ap_c003_poz_supplier_sites_stg: ' || sqlerrm);
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
                xxcnv_ap_c003_poz_sup_site_assign_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c003_poz_sup_site_assign_stg from external table: ' || lv_row_count
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
                dbms_output.put_line('Error counting rows in xxcnv_ap_c003_poz_sup_site_assign_stg: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

--table 5

        BEGIN
        -- Count the number of rows in the stage table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c003_poz_sup_contacts_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c003_poz_sup_contacts_stg from external table: ' || lv_row_count
            );
            IF lv_row_count > 0 THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_picked,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppcontacts,
                    p_attribute1        => NULL,
                    p_attribute2        => lv_row_count,
                    p_process_reference => NULL
                );
            END IF;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_ap_c003_poz_sup_contacts_stg: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

--table 6

        BEGIN
        -- Count the number of rows in the stage table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c003_poz_sup_cont_addr_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c003_poz_sup_cont_addr_stg from external table: ' || lv_row_count
            );
            IF lv_row_count > 0 THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_picked,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppcontactaddress,
                    p_attribute1        => NULL,
                    p_attribute2        => lv_row_count,
                    p_process_reference => NULL
                );
            END IF;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_ap_c003_poz_sup_cont_addr_stg: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

--table 7

        BEGIN
        -- Count the number of rows in the stage table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c003_poz_sup_bus_class_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c003_poz_sup_bus_class_stg from external table: ' || lv_row_count
            );
            IF lv_row_count > 0 THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_picked,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppclassstg,
                    p_attribute1        => NULL,
                    p_attribute2        => lv_row_count,
                    p_process_reference => NULL
                );
            END IF;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_ap_c003_poz_sup_bus_class_stg: ' || sqlerrm);
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
            to_char(sysdate, 'YYYYMMDDHHMMSS')
        INTO gv_batch_id
        FROM
            dual;

        BEGIN
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c003_poz_suppliers_stg;

            IF lv_row_count = 0 THEN
                dbms_output.put_line('No Data is found in the xxcnv_ap_c003_poz_suppliers_stg table');
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
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                execution_id = gv_execution_id,
                batch_id = gv_batch_id
            WHERE
                file_reference_identifier IS NULL;

        END;
        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = ''
            WHERE
                error_message IS NULL;

        END;

    ----------------------Supplier Validations-----------

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = error_message || '|Supplier Name should not be NULL'
            WHERE
                vendor_name IS NULL;

            dbms_output.put_line('Supplier Name is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = error_message || '|Supplier Number should not be NULL'
            WHERE
                segment1 IS NULL;

            dbms_output.put_line('Supplier Number is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = error_message || '|Duplicate Supplier Names'
            WHERE
                vendor_name IN (
                    SELECT
                        vendor_name
                    FROM
                        xxcnv_ap_c003_poz_suppliers_stg
                    WHERE
                        vendor_name IS NOT NULL
                    GROUP BY
                        vendor_name
                    HAVING
                        COUNT(1) > 1
                );

        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = error_message || '|Duplicate Supplier Numbers'
            WHERE
                segment1 IN (
                    SELECT
                        segment1
                    FROM
                        xxcnv_ap_c003_poz_suppliers_stg
                    WHERE
                        segment1 IS NOT NULL
                    GROUP BY
                        segment1
                    HAVING
                        COUNT(1) > 1
                );

        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = error_message || '|Taxpayer Country should not be NULL'
            WHERE
                tax_country_code IS NULL;

            dbms_output.put_line('Taxpayer Country is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = error_message || '|Duplicate Taxpayer_ID'
            WHERE
                num_1099 IS NOT NULL
                AND num_1099 IN (
                    SELECT
                        num_1099
                    FROM
                        xxcnv_ap_c003_poz_suppliers_stg
                    WHERE
                        num_1099 IS NOT NULL
                    GROUP BY
                        num_1099
                    HAVING
                        COUNT(1) > 1
                );

        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = error_message || '|Duplicate DUNS Number'
            WHERE
                duns_number IS NOT NULL
                AND duns_number IN (
                    SELECT
                        duns_number
                    FROM
                        xxcnv_ap_c003_poz_suppliers_stg
                    WHERE
                        duns_number IS NOT NULL
                    GROUP BY
                        duns_number
                    HAVING
                        COUNT(1) > 1
                );

        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = error_message || '|Duplicate Tax Registration Number'
            WHERE
                vat_registration_num IS NOT NULL
                AND vat_registration_num IN (
                    SELECT
                        vat_registration_num
                    FROM
                        xxcnv_ap_c003_poz_suppliers_stg
                    WHERE
                        vat_registration_num IS NOT NULL
                    GROUP BY
                        vat_registration_num
                    HAVING
                        COUNT(1) > 1
                );

        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = error_message || '|Tax Registration Number should not contain Comma'
            WHERE
                    regexp_count(vat_registration_num, ',') > 1
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Tax Registration Number is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = error_message || '|Federal Income Tax Type should be populated if Federal reportable is Y'
            WHERE
                    federal_reportable_flag = 'Y'
                AND type_1099 IS NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Federal Income Tax Type is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                oc_payment_method = (
                    SELECT
                        oc_value
                    FROM
                        xxcnv_ap_payment_method_mapping
                    WHERE
                        upper(ns_value) = upper(payment_method_lookup_code)
                )
            WHERE
                payment_method_lookup_code IS NOT NULL;

            dbms_output.put_line('Payment Method is updated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                remit_advice_delivery_method = 'EMAIL'
            WHERE
                    1 = 1
                AND remit_advice_email IS NOT NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Remittance E-mail is updated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                vendor_name = '"'
                              || vendor_name
                              || '"'
            WHERE
                vendor_name LIKE '%,%'
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('vendor_name With Comma is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                vendor_name_alt = '"'
                                  || vendor_name_alt
                                  || '"'
            WHERE
                vendor_name_alt LIKE '%,%'
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('vendor_name_ALT With Comma is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                alias = '"'
                        || alias
                        || '"'
            WHERE
                alias LIKE '%,%'
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Alias With Comma is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                error_message = error_message || '|At least one of the following fields must be filled: Taxpayer ID, Tax Registration Number, or DUNS Number'
            WHERE
                ( duns_number IS NULL
                  AND num_1099 IS NULL
                  AND vat_registration_num IS NULL )
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Remittance E-mail is validated');
        END;

     -- Updating constant values --

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                import_action = 'CREATE',
                organization_type_lookup_code = 'Corporation',
                vendor_type_lookup_code = 'Supplier',
                business_relationship = 'SPEND_AUTHORIZED',
                one_time_flag = 'N';

            dbms_output.put_line('Constant fields are updated');
        END;

  -- Update import_status based on error_message
        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
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
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
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
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
            SET
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure
            WHERE
                error_message IS NOT NULL
                AND file_reference_identifier IS NULL;

        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_suppliers_stg
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
            xxcnv_ap_c003_poz_suppliers_stg
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
                    xxcnv_ap_c003_poz_supplier_addresses_stg;

                IF lv_row_count = 0 THEN
                    dbms_output.put_line('No Data is found in the xxcnv_ap_c003_poz_supplier_addresses_stg table');
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
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    execution_id = gv_execution_id,
                    batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END;
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    party_site_name = replace(party_site_name, '"', '')
                WHERE
                    ( party_site_name LIKE '%,%"%'
                      OR party_site_name LIKE '%"%,%' );

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    address_line1 = replace(address_line1, '"', '')
                WHERE
                    ( address_line1 LIKE '%,%"%'
                      OR address_line1 LIKE '%"%,%' );

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    address_line2 = replace(address_line2, '"', '')
                WHERE
                    ( address_line2 LIKE '%,%"%'
                      OR address_line2 LIKE '%"%,%' );

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    vendor_name = '"'
                                  || vendor_name
                                  || '"'
                WHERE
                    vendor_name LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    party_site_name = '"'
                                      || party_site_name
                                      || '"'
                WHERE
                    party_site_name LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    address_line1 = '"'
                                    || address_line1
                                    || '"'
                WHERE
                    address_line1 LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    address_line2 = '"'
                                    || address_line2
                                    || '"'
                WHERE
                    address_line2 LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    address_line3 = '"'
                                    || address_line3
                                    || '"'
                WHERE
                    address_line3 LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    address_line4 = '"'
                                    || address_line4
                                    || '"'
                WHERE
                    address_line4 LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    city = '"'
                           || city
                           || '"'
                WHERE
                    city LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    state = '"'
                            || state
                            || '"'
                WHERE
                    state LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    postal_code = '"'
                                  || postal_code
                                  || '"'
                WHERE
                    postal_code LIKE '%,%';

            END;
  -- Initialize error_message to an empty string if it IS NULL
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
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
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Supplier Name not found in Supplier header table',
                    import_status = 'ERROR'
                WHERE
                    ( vendor_name NOT IN (
                        SELECT
                            vendor_name
                        FROM
                            xxcnv_ap_c003_poz_suppliers_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Child record failed because Parent failed',
                    import_status = 'ERROR'
                WHERE
                    ( vendor_name IN (
                        SELECT
                            vendor_name
                        FROM
                            xxcnv_ap_c003_poz_suppliers_stg
                        WHERE
                            import_status = 'ERROR'
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Supplier Name should not be NULL'
                WHERE
                    vendor_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Name is validated');
            END;
  ------------------------------PARTY_SITE_NAME------------------------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Address Name should not be NULL'
                WHERE
                    party_site_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Address Name is validated');
            END;
  --------COUNTRY -------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
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
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Address line1 should not be NULL'
                WHERE
                    address_line1 IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Address line1 is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Province should not be NULL'
                WHERE
                    province IS NULL
                    AND country IN ( 'CN', 'CA' )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Province is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|City should not be NULL'
                WHERE
                    city IS NULL
                    AND country IN ( 'AU', 'CH', 'DE', 'GB', 'IE',
                                     'IN', 'NL', 'NZ', 'US' )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('City is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Postal Code should not be NULL'
                WHERE
                    postal_code IS NULL
                    AND country IN ( 'CH', 'DE', 'IN', 'NL', 'NZ',
                                     'US' )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Postal Code is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|State should not be NULL'
                WHERE
                    state IS NULL
                    AND country IN ( 'IN', 'US' )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('State is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Atleast one of the RFQ,Ordering,Pay flags should be Y'
                WHERE
                    ( rfq_or_bidding_purpose_flag <> 'Y'
                      AND ordering_purpose_flag <> 'Y'
                      AND remit_to_purpose_flag <> 'Y' )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('RFQ,Ordering,Pay flags flags are validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    remit_advice_delivery_method = 'EMAIL'
                WHERE
                        1 = 1
                    AND remittance_email IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Remittance E-mail is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    error_message = error_message || '|Duplicate Address Name'
                WHERE
                    party_site_name IS NOT NULL
                    AND party_site_name IN (
                        SELECT
                            party_site_name
                        FROM
                            xxcnv_ap_c003_poz_supplier_addresses_stg
                        WHERE
                            party_site_name IS NOT NULL
                        GROUP BY
                            party_site_name
                        HAVING
                            COUNT(1) > 1
                    );

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_suppliers_stg sit1
                SET
                    sit1.error_message = sit1.error_message || '|At least one address record should be present for the supplier'
                WHERE
                        1 = 1
                    AND NOT EXISTS (
                        SELECT
                            1
                        FROM
                            xxcnv_ap_c003_poz_supplier_addresses_stg sit2
                        WHERE
                                1 = 1
                            AND sit2.vendor_name = sit1.vendor_name
                    );

                dbms_output.put_line('Address record is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_suppliers_stg
                SET
                    import_status =
                        CASE
                            WHEN error_message IS NOT NULL THEN
                                'ERROR'
                            ELSE
                                'PROCESSED'
                        END,
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    error_message IS NOT NULL;

            END;


    -- Updating constant values --

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    import_action = 'CREATE',
                    rfq_or_bidding_purpose_flag = 'Y',
                    ordering_purpose_flag = 'Y',
                    remit_to_purpose_flag = 'Y';

                dbms_output.put_line('Constant fields are updated');
            END;
  ---------------------Update import_status based on error_message----------------

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    file_name = gv_oci_file_name_suppaddress
                WHERE
                    file_reference_identifier IS NULL;

                dbms_output.put_line('File_name column is updated');
            END;

  -- Final update to set error_message AND import_status
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
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
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
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
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
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
                xxcnv_ap_c003_poz_supplier_addresses_stg
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
                    xxcnv_ap_c003_poz_supplier_sites_stg;

                IF lv_row_count = 0 THEN
                    dbms_output.put_line('No Data is found in the xxcnv_ap_c003_poz_supplier_sites_stg table');
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
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    execution_id = gv_execution_id,
                    batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END;
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    party_site_name = replace(party_site_name, '"', '')
                WHERE
                    ( party_site_name LIKE '%,%"%'
                      OR party_site_name LIKE '%"%,%' );

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    vendor_name = '"'
                                  || vendor_name
                                  || '"'
                WHERE
                    vendor_name LIKE '%,%';

            END;
			
			/* START added for v1.1 */
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    payment_reason_comments = '"'||payment_reason_comments||'"'
                WHERE
                    payment_reason_comments LIKE '%,%';

            END;
			/* END added for v1.1 */

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    party_site_name = '"'
                                      || party_site_name
                                      || '"'
                WHERE
                    party_site_name LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    vendor_site_code = '"'
                                       || vendor_site_code
                                       || '"'
                WHERE
                    vendor_site_code LIKE '%,%';

            END;
	-- Initialize error_message to an empty string if it IS NULL
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
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
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Child record failed because Parent failed'
                WHERE
                    ( vendor_name || party_site_name IN (
                        SELECT
                            vendor_name || party_site_name
                        FROM
                            xxcnv_ap_c003_poz_supplier_addresses_stg
                        WHERE
                                import_status = 'ERROR'
                            AND execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Supplier Name not found in Supplier header table'
                WHERE
                    ( vendor_name NOT IN (
                        SELECT
                            vendor_name
                        FROM
                            xxcnv_ap_c003_poz_suppliers_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Address Name not found in Supplier address table'
                WHERE
                    ( party_site_name NOT IN (
                        SELECT
                            party_site_name
                        FROM
                            xxcnv_ap_c003_poz_supplier_addresses_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

	-----VENDOR NAME--------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Supplier Name should not be NULL'
                WHERE
                    vendor_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Name is validated');
            END;

  -----PROCUREMENT_BUSINESS_UNIT_NAME------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Procurement BU should not be NULL'
                WHERE
                    procurement_business_unit_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Procurement BU is validated');
            END;

  -------PARTY_SITE_NAME------------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Address Name should not be NULL'
                WHERE
                    party_site_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Address Name  is validated');
            END;

  -----VENDOR_SITE_CODE------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Supplier Site should not be NULL'
                WHERE
                    vendor_site_code IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Site is validated');
            END;

 ----------------PURCHASING_SITE_FLAG -------------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Purchasing flag should not be NULL'
                WHERE
                    purchasing_site_flag IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Purchasing flag is validated');
            END;


 ---------------- PAY_SITE_FLAG -------------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Pay flag should not be NULL'
                WHERE
                    pay_site_flag IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Pay flag is validated');
            END;

   ---------EMAIL_ADDRESS---------------------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Email address is in incorrect format'
                WHERE
                        supplier_notif_method = 'EMAIL'
                    AND email_address IS NOT NULL
                    AND email_address NOT LIKE '%@%'
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Email address is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    supplier_notif_method = 'EMAIL'
                WHERE
                        1 = 1
                    AND email_address IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Notification Method is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Payment Method should not be NULL'
                WHERE
                    payment_method_lookup_code IS NULL;

                dbms_output.put_line('Payment Method is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    oc_payment_method = (
                        SELECT
                            oc_value
                        FROM
                            xxcnv_ap_payment_method_mapping
                        WHERE
                            upper(ns_value) = upper(payment_method_lookup_code)
                    )
                WHERE
                    payment_method_lookup_code IS NOT NULL;

                dbms_output.put_line('Payment Method is updated');
            END;

	-- To check whether the value is NULL after the transformation --

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Payment Method should not be NULL after the Transformation'
                WHERE
                    oc_payment_method IS NULL;

                dbms_output.put_line('Payment Method is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Payment Terms should not be NULL'
                WHERE
                    terms_name IS NULL;

                dbms_output.put_line('Payment Terms is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    oc_payment_terms = (
                        SELECT
                            oc_value
                        FROM
                            xxcnv_ap_payment_terms_mapping
                        WHERE
                            upper(ns_value) = upper(terms_name)
                    )
                WHERE
                    terms_name IS NOT NULL;

                dbms_output.put_line('Payment Terms is updated');
            END;

	-- To check whether the value is NULL after the transformation --
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Payment Terms should not be NULL after the Transformation'
                WHERE
                    oc_payment_terms IS NULL;

                dbms_output.put_line('Payment Terms is validated');
            END;


   ---------REMITTANCE_EMAIL---------------------

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    remit_advice_delivery_method = 'EMAIL'
                WHERE
                        1 = 1
                    AND remittance_email IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Remittance E-Mail is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    error_message = error_message || '|Attribute1 should be populated with NetSuite Vendor ID'
                WHERE
                        1 = 1
                    AND attribute1 IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Attribute1 is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
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

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg sit1
                SET
                    sit1.error_message = sit1.error_message || '|At least one site record should be present for the supplier address'
                WHERE
                        1 = 1
                    AND NOT EXISTS (
                        SELECT
                            1
                        FROM
                            xxcnv_ap_c003_poz_supplier_sites_stg sit2
                        WHERE
                                1 = 1
                            AND sit2.vendor_name = sit1.vendor_name
                    );

                dbms_output.put_line('Site record is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_addresses_stg
                SET
                    import_status =
                        CASE
                            WHEN error_message IS NOT NULL THEN
                                'ERROR'
                            ELSE
                                'PROCESSED'
                        END,
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    error_message IS NOT NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_suppliers_stg sit1
                SET
                    sit1.error_message = sit1.error_message || '|At least one of the site associated with Supplier with Federal reportable marked as “Y” should have Income Tax Reportable flag as “Y”'
                WHERE
                        sit1.federal_reportable_flag = 'Y'
                    AND NOT EXISTS (
                        SELECT
                            1
                        FROM
                            xxcnv_ap_c003_poz_supplier_sites_stg sit2
                        WHERE
                                sit2.tax_reporting_site_flag = 'Y'
                            AND sit2.vendor_name = sit1.vendor_name
                    );

                dbms_output.put_line('Income Tax Reportable flag is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_suppliers_stg
                SET
                    import_status =
                        CASE
                            WHEN error_message IS NOT NULL THEN
                                'ERROR'
                            ELSE
                                'PROCESSED'
                        END,
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    error_message IS NOT NULL;

            END;

    -- Updating constant values --

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    import_action = 'CREATE',
                    payment_priority = 99,
                    hold_all_payments_flag = 'N',
                    hold_unmatched_invoices_flag = 'N',
                    hold_future_payments_flag = 'N',
                    enforce_ship_to_location_code = 'NONE',
                    receiving_routing_id = 3,
                    pay_date_basis_lookup_code = 'DISCOUNT';

                dbms_output.put_line('Constant fields are updated');
            END;

  ---------------Final update to set error_message AND import_status
            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
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
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
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
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
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
                xxcnv_ap_c003_poz_supplier_sites_stg
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
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    execution_id = gv_execution_id,
                    batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END;
            BEGIN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c003_poz_sup_site_assign_stg;

                IF lv_row_count = 0 THEN
                    dbms_output.put_line('No Data is found in the xxcnv_ap_c003_poz_sup_site_assign_stg table');
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

  -- Initialize error_message to an empty string if it IS NULL
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
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
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    vendor_site_code = '"'
                                       || vendor_site_code
                                       || '"'
                WHERE
                    vendor_site_code LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    vendor_name = '"'
                                  || vendor_name
                                  || '"'
                WHERE
                    vendor_name LIKE '%,%';

            END;

      -----Vendor Site Validation--------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Child record failed because Parent failed'
                WHERE
                    ( vendor_name || vendor_site_code IN (
                        SELECT
                            vendor_name || vendor_site_code
                        FROM
                            xxcnv_ap_c003_poz_supplier_sites_stg
                        WHERE
                            import_status = 'ERROR'
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Supplier Name not found in Supplier header table'
                WHERE
                    ( vendor_name NOT IN (
                        SELECT
                            vendor_name
                        FROM
                            xxcnv_ap_c003_poz_suppliers_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Supplier Site not found in Supplier sites table'
                WHERE
                    ( vendor_site_code NOT IN (
                        SELECT
                            vendor_site_code
                        FROM
                            xxcnv_ap_c003_poz_supplier_sites_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

	-----VENDOR NAME--------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Supplier Name should not be NULL'
                WHERE
                    vendor_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Name is validated');
            END;
  -----VENDOR_SITE_CODE------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Supplier Site should not be NULL'
                WHERE
                    vendor_site_code IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Site is validated');
            END;
  -----PROCUREMENT_BUSINESS_UNIT_NAME------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Procurement BU should not be NULL'
                WHERE
                    procurement_business_unit_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Procurement BU is validated');
            END;
	-------BUSINESS_UNIT_NAME------------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Client BU should not be NULL'
                WHERE
                    business_unit_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Client BU is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Bill to BU should not be NULL'
                WHERE
                    bill_to_bu_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Bill to BU is validated');
            END;

	-------BUSINESS_UNIT_NAME------------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    oc_client_bu = (
                        SELECT
                            oc_business_unit_name
                        FROM
                            xxcnv_gl_le_bu_mapping
                        WHERE
                                1 = 1
                            AND upper(ns_legal_entity_name) = upper(business_unit_name)
                    )
                WHERE
                    business_unit_name IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Client BU is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Client BU should not be NULL after the transformation'
                WHERE
                    oc_client_bu IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Client BU is validated');
            END;

	-------bill_to_bu_name------------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    oc_bill_to_bu = (
                        SELECT
                            oc_business_unit_name
                        FROM
                            xxcnv_gl_le_bu_mapping
                        WHERE
                                1 = 1
                            AND upper(ns_legal_entity_name) = upper(bill_to_bu_name)
                    )
                WHERE
                    bill_to_bu_name IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Bill_to_bu_name is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    error_message = error_message || '|Bill to BU should not be NULL after the transformation'
                WHERE
                    oc_bill_to_bu IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Bill to BU is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    bill_to_location_code = (
                        SELECT
                            oc_bill_to_location_code
                        FROM
                            xxcnv_gl_le_bu_mapping
                        WHERE
                            upper(ns_legal_entity_name) = upper(business_unit_name)
                    )
                WHERE
                    business_unit_name IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Bill to location is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    bill_to_location_code = '"'
                                            || bill_to_location_code
                                            || '"'
                WHERE
                    bill_to_location_code LIKE '%,%';

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg sas
                SET
                    sas.distribution_set_name = (
                        SELECT
                            m.distribution_set_name
                        FROM
                            xxcnv_ap_distribution_mapping        m,
                            xxcnv_ap_c003_poz_supplier_sites_stg s
                        WHERE
                                1 = 1
                            AND upper(m.client_bu) = upper(sas.oc_client_bu)
                            AND upper(m.ns_vendor_num) = upper(s.attribute1)
                            AND upper(s.vendor_name) = upper(sas.vendor_name)
                            AND upper(s.vendor_site_code) = upper(sas.vendor_site_code)
                    )
                WHERE
                    sas.business_unit_name IS NOT NULL
                    AND sas.file_reference_identifier IS NULL;

                dbms_output.put_line('Distribution Set is updated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg sit1
                SET
                    sit1.error_message = sit1.error_message || '|At least one site assignment record should be present for the supplier site'
                WHERE
                        1 = 1
                    AND NOT EXISTS (
                        SELECT
                            1
                        FROM
                            xxcnv_ap_c003_poz_sup_site_assign_stg sit2
                        WHERE
                                1 = 1
                            AND sit2.vendor_name = sit1.vendor_name
                    );

                dbms_output.put_line('Site Assignment record is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_supplier_sites_stg
                SET
                    import_status =
                        CASE
                            WHEN error_message IS NOT NULL THEN
                                'ERROR'
                            ELSE
                                'PROCESSED'
                        END,
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    error_message IS NOT NULL;

            END;

    -- Updating constant values --

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
                SET
                    import_action = 'CREATE';

                dbms_output.put_line('Constant fields are updated');
            END;

  -- Final update to set error_message AND import_status
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
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
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
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
                UPDATE xxcnv_ap_c003_poz_sup_site_assign_stg
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
                xxcnv_ap_c003_poz_sup_site_assign_stg
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

  ---------------------------SUPPLIER CONTACT VALIDATIONS----------------------------
        BEGIN
            BEGIN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c003_poz_sup_contacts_stg;

                IF lv_row_count = 0 THEN
                    dbms_output.put_line('No Data is found in the xxcnv_ap_c003_poz_sup_contacts_stg table');
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

	-- Initialize error_message to an empty string if it IS NULL
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
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
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    execution_id = gv_execution_id,
                    batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END;
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    phone_country_code = replace(phone_country_code, '+', '') /*Remove for Mock1*/
                WHERE
                    phone_country_code LIKE '+%'
                    AND file_reference_identifier IS NULL
                    AND phone_country_code IS NOT NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    vendor_name = '"'
                                  || vendor_name
                                  || '"'
                WHERE
                    vendor_name LIKE '%,%'
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    error_message = error_message || '|Child record failed because Parent failed',
                    import_status = 'ERROR'
                WHERE
                    ( vendor_name IN (
                        SELECT
                            vendor_name
                        FROM
                            xxcnv_ap_c003_poz_suppliers_stg
                        WHERE
                            import_status = 'ERROR'
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    error_message = error_message || '|Supplier Name not found in Supplier header table',
                    import_status = 'ERROR'
                WHERE
                    ( vendor_name NOT IN (
                        SELECT
                            vendor_name
                        FROM
                            xxcnv_ap_c003_poz_suppliers_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

	---IMPORT ACTION--------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    error_message = error_message || '|Import Action should be CREATE'
                WHERE
                    nvl(
                        upper(import_action),
                        'CR'
                    ) <> 'CREATE';

                dbms_output.put_line('Import Action is validated');
            END;
	--------------------------------------------VENDOR NAME--------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    error_message = error_message || '|Supplier Name should not be NULL'
                WHERE
                    vendor_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Name is validated');
            END;

  -----first_name------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    first_name = '"'
                                 || first_name
                                 || '"'
                WHERE
                    first_name LIKE '%,%'
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    error_message = error_message || '|First Name should not be NULL'
                WHERE
                    first_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('First Name is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    first_name = TRIM(first_name)
                WHERE
                    first_name IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('first_name is Trimmed');
            END;
	-------------------------------------LAST_NAME------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    last_name = '"'
                                || last_name
                                || '"'
                WHERE
                    last_name LIKE '%,%'
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    error_message = error_message || '|Last Name should not be NULL'
                WHERE
                    last_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Last Name is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    last_name = TRIM(last_name)
                WHERE
                    last_name IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Last Name is Trimmed');
            END;

  -------PRIMARY_ADMIN_CONTACT------------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    error_message = error_message || '|Administrative Contact should not be other values than Y,N,Blank'
                WHERE
                    primary_admin_contact NOT IN ( 'Y', 'N', NULL )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Administrative Contact is validated');
            END;

	----------------EMAIL_ADDRESS-------------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    error_message = error_message || '|Duplicate E-Mail IDs'
                WHERE
                    email_address IN (
                        SELECT
                            email_address
                        FROM
                            xxcnv_ap_c003_poz_sup_contacts_stg
                        WHERE
                            email_address IS NOT NULL
                        GROUP BY
                            email_address
                        HAVING
                            COUNT(1) > 1
                    );

            END;

    -- Updating constant values --

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    import_action = 'CREATE';

                dbms_output.put_line('Constant fields are updated');
            END;
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    error_message = error_message || '|E-Mail is in incorrect format'
                WHERE
                    email_address IS NOT NULL
                    AND email_address NOT LIKE '%@%'
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('E-Mail is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    error_message = error_message || '|E-Mail should not contain ";"'
                WHERE
                    email_address IS NOT NULL
                    AND email_address LIKE '%;%'
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('E-Mail is validated');
            END;

	/* Remove for mock */

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
                SET
                    last_name = '.'
                WHERE
                    last_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Last Name is updated with dot');
            END;

   -- Final update to set error_message AND import_status
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
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
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
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
                UPDATE xxcnv_ap_c003_poz_sup_contacts_stg
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
                xxcnv_ap_c003_poz_sup_contacts_stg
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
                    p_file_name         => gv_oci_file_name_suppcontacts,
                    p_attribute1        => NULL,
                    p_attribute2        => gv_data_validated_failure,
                    p_process_reference => NULL
                );
            ELSIF gv_oci_file_name_suppcontacts IS NOT NULL THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_validated,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppcontacts,
                    p_attribute1        => NULL,
                    p_attribute2        => gv_data_validated_success,
                    p_process_reference => NULL
                );
            ELSE
                NULL;
            END IF;

        END;

--Table 6
        BEGIN
            BEGIN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c003_poz_sup_cont_addr_stg;

                IF lv_row_count = 0 THEN
                    dbms_output.put_line('No Data is found in the xxcnv_ap_c003_poz_sup_cont_addr_stg table');
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
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    execution_id = gv_execution_id,
                    batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END;

  -- Initialize error_message to an empty string if it IS NULL
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
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
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    party_site_name = replace(party_site_name, '"', '')
                WHERE
                    ( party_site_name LIKE '%,%"%'
                      OR party_site_name LIKE '%"%,%' );

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    vendor_name = '"'
                                  || vendor_name
                                  || '"'
                WHERE
                    vendor_name LIKE '%,%'
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    first_name = '"'
                                 || first_name
                                 || '"'
                WHERE
                    first_name LIKE '%,%'
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    last_name = '"'
                                || last_name
                                || '"'
                WHERE
                    last_name LIKE '%,%'
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    party_site_name = '"'
                                      || party_site_name
                                      || '"'
                WHERE
                    party_site_name LIKE '%,%'
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    last_name = '.'
                WHERE
                    last_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Last Name is updated with dot');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    error_message = error_message || '|Child record failed because Parent failed',
                    import_status = 'ERROR'
                WHERE
                    ( vendor_name
                      || first_name
                      || last_name ) IN (
                        SELECT
                            vendor_name
                            || first_name
                            || last_name
                        FROM
                            xxcnv_ap_c003_poz_suppliers_stg
                        WHERE
                            import_status = 'ERROR'
                    )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    error_message = error_message || '|Supplier Name not found in Supplier header table',
                    import_status = 'ERROR'
                WHERE
                    ( vendor_name NOT IN (
                        SELECT
                            vendor_name
                        FROM
                            xxcnv_ap_c003_poz_suppliers_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    error_message = error_message || '|Contact details not found in Supplier contacts table',
                    import_status = 'ERROR'
                WHERE
                    ( ( first_name
                        || '_'
                        || last_name ) NOT IN (
                        SELECT
                            first_name
                            || '_'
                            || last_name
                        FROM
                            xxcnv_ap_c003_poz_sup_contacts_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    error_message = error_message || '|Contact address not found in Supplier addresses table',
                    import_status = 'ERROR'
                WHERE
                    ( party_site_name NOT IN (
                        SELECT
                            party_site_name
                        FROM
                            xxcnv_ap_c003_poz_supplier_addresses_stg
                        WHERE
                            execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

	-----VENDOR NAME--------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    error_message = error_message || '|Supplier Name should not be NULL'
                WHERE
                    vendor_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier Name is validated');
            END;

   -------PARTY_SITE_NAME------------
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    error_message = error_message || '|Address Name should not be NULL'
                WHERE
                    party_site_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Address Name is validated');
            END;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    error_message = error_message || '|First Name should not be NULL'
                WHERE
                    first_name IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('First Name is validated');
            END;

    -- Updating constant values --

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
                SET
                    import_action = 'CREATE';

                dbms_output.put_line('Constant fields are updated');
            END;

	-- Final update to set error_message AND import_status
            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
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

	-- Check if there are any error messages
            SELECT
                COUNT(*)
            INTO lv_error_count
            FROM
                xxcnv_ap_c003_poz_sup_cont_addr_stg
            WHERE
                error_message IS NOT NULL;

            BEGIN
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
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
                UPDATE xxcnv_ap_c003_poz_sup_cont_addr_stg
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

            IF lv_error_count > 0 THEN
    -- Logging the message
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_failed_validation,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppcontactaddress,
                    p_attribute1        => NULL,
                    p_attribute2        => gv_data_validated_failure,
                    p_process_reference => NULL
                );
            ELSIF gv_oci_file_name_suppcontactaddress IS NOT NULL THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_validated,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_suppcontactaddress,
                    p_attribute1        => NULL,
                    p_attribute2        => gv_data_validated_success,
                    p_process_reference => NULL
                );
            ELSE
                NULL;
            END IF;

        END;

--table7
-------------------------------SUPPLIER BUSINESS CLASSIFICATION VALIDATIONS---------------------------
        BEGIN
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c003_poz_sup_bus_class_stg;

            IF lv_row_count = 0 THEN
                dbms_output.put_line('No Data is found in the xxcnv_ap_c003_poz_sup_bus_class_stg table');
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
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg
            SET
                execution_id = gv_execution_id,
                batch_id = gv_batch_id
            WHERE
                file_reference_identifier IS NULL;

        END;

	-- Initialize error_message to an empty string if it IS NULL
        BEGIN
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg
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
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg
            SET
                vendor_name = '"'
                              || vendor_name
                              || '"'
            WHERE
                vendor_name LIKE '%,%';

        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg
            SET
                notes = '"'
                        || notes
                        || '"'
            WHERE
                notes LIKE '%,%';

        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg
            SET
                error_message = error_message || '|Child record failed because Parent failed',
                import_status = 'ERROR'
            WHERE
                ( vendor_name IN (
                    SELECT
                        vendor_name
                    FROM
                        xxcnv_ap_c003_poz_suppliers_stg
                    WHERE
                        import_status = 'ERROR'
                ) )
                AND execution_id = gv_execution_id
                AND file_reference_identifier IS NULL;

        END;

	-----VENDOR NAME--------
        BEGIN
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg
            SET
                error_message = error_message || '|Supplier Name should not be NULL'
            WHERE
                vendor_name IS NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Supplier Name is validated');
        END;

  -----CLASSIFICATION_LOOKUP_CODE------
        BEGIN
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg
            SET
                error_message = error_message || '|Classification should not be NULL'
            WHERE
                classification_lookup_code IS NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Classification is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg bus
            SET
                error_message = error_message || '|Contact record not present in the Supplier Contacts table'
            WHERE
                bus.vendor_name IS NOT NULL
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        xxcnv_ap_c003_poz_sup_contacts_stg stg
                    WHERE
                            stg.vendor_name = bus.vendor_name
                        AND execution_id = gv_execution_id
                )
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Contact record is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg bus
            SET
                error_message = error_message || '|First_Name field in Business Classification table should match with Supplier Contacts table'
            WHERE
                bus.provided_by_contact_first_name IS NOT NULL
                AND bus.error_message NOT LIKE '%Contact record not present in the Supplier Contacts table%'
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        xxcnv_ap_c003_poz_sup_contacts_stg stg
                    WHERE
                            stg.vendor_name = bus.vendor_name
                        AND stg.first_name = bus.provided_by_contact_first_name
                        AND execution_id = gv_execution_id
                )
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('First_Name is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg bus
            SET
                error_message = error_message || '|Last_Name field in Business Classification table should match with Supplier Contacts table'
            WHERE
                bus.provided_by_contact_last_name IS NOT NULL
                AND bus.error_message NOT LIKE '%Contact record not present in the Supplier Contacts table%'
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        xxcnv_ap_c003_poz_sup_contacts_stg stg
                    WHERE
                            stg.vendor_name = bus.vendor_name
                        AND stg.last_name = bus.provided_by_contact_last_name
                        AND execution_id = gv_execution_id
                )
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Last_Name is validated');
        END;

        BEGIN
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg bus
            SET
                error_message = error_message || '|Email field in Business Classification table should match with Supplier Contacts table'
            WHERE
                bus.provided_by_contact_email IS NOT NULL
                AND bus.error_message NOT LIKE '%Contact record not present in the Supplier Contacts table%'
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        xxcnv_ap_c003_poz_sup_contacts_stg stg
                    WHERE
                            stg.vendor_name = bus.vendor_name
                        AND stg.email_address = bus.provided_by_contact_email
                        AND execution_id = gv_execution_id
                )
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Email is validated');
        END;

    -- Updating constant values --

        BEGIN
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg
            SET
                import_action = 'CREATE';

            dbms_output.put_line('Constant fields are updated');
        END;

	-- Final update to set error_message AND import_status
        BEGIN
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg
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
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg
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
            UPDATE xxcnv_ap_c003_poz_sup_bus_class_stg
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
            xxcnv_ap_c003_poz_sup_bus_class_stg
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
                p_file_name         => gv_oci_file_name_suppclassstg,
                p_attribute1        => NULL,
                p_attribute2        => gv_data_validated_failure,
                p_process_reference => NULL
            );
        ELSIF gv_oci_file_name_suppclassstg IS NOT NULL THEN
	-- Logging the message
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_validated,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_suppclassstg,
                p_attribute1        => NULL,
                p_attribute2        => gv_data_validated_success,
                p_process_reference => NULL
            );
        ELSE
            NULL;
        END IF;

    END data_validations_prc;

/*==============================================================================================================================
-- PROCEDURE : create_fbdi_file_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the FBDI CSV file by using the data in the staging tables after all validations.
================================================================================================================================= */
    PROCEDURE create_fbdi_file_prc IS
        lv_success_count NUMBER;
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
                    xxcnv_ap_c003_poz_suppliers_stg
                WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No batch_id is found for xxcnv_ap_c003_poz_suppliers_stg');
                    RETURN;
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking batch_id for xxcnv_ap_c003_poz_suppliers_stg ' || sqlerrm);
                    RETURN;
            END;

            BEGIN
                -- Count the number of rows which are validated successfully for the current batch_id
                SELECT
                    COUNT(1)
                INTO lv_success_count
                FROM
                    xxcnv_ap_c003_poz_suppliers_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Success record count for xxcnv_ap_c003_poz_suppliers_stg batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_success_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c003_poz_suppliers_stg batch_id: ' || lv_batch_id);
                    RETURN;
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking success record count for xxcnv_ap_c003_poz_suppliers_stg batch_id '
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
                      --  format          => JSON_OBJECT('type' VALUE 'csv', 'trimspaces' VALUE 'rtrim','quote' value '"'),
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
											,OC_PAYMENT_METHOD       -- transformed_field
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
                                            FROM xxcnv_ap_c003_poz_suppliers_stg
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

                    dbms_output.put_line('xxcnv_ap_c003_poz_suppliers_stg CSV file for batch_id '
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
                        dbms_output.put_line('Error exporting data to CSV for  xxcnv_ap_c003_poz_suppliers_stg batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for xxcnv_ap_c003_poz_suppliers_stg batch_id '
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
                    xxcnv_ap_c003_poz_supplier_addresses_stg
                WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No batch_id is found for xxcnv_ap_c003_poz_supplier_addresses_stg');
                    RETURN;
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking batch_id for xxcnv_ap_c003_poz_supplier_addresses_stg ' || sqlerrm);
                    RETURN;
            END;

            BEGIN
                -- Count the success record count for the current batch_id
                SELECT
                    COUNT(1)
                INTO lv_success_count
                FROM
                    xxcnv_ap_c003_poz_supplier_addresses_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Success record count for xxcnv_ap_c003_poz_supplier_addresses_stg batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_success_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c003_poz_supplier_addresses_stg batch_id: ' || lv_batch_id);
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
                       --format          => JSON_OBJECT('type' VALUE 'csv', 'trimspaces' VALUE 'rtrim','quote' value '"'),
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
                                            FROM xxcnv_ap_c003_poz_supplier_addresses_stg
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

                    dbms_output.put_line('xxcnv_ap_c003_poz_supplier_addresses_stg CSV file for batch_id '
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
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_supplier_addresses_stg batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for xxcnv_ap_c003_poz_supplier_addresses_stg batch_id '
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
                    xxcnv_ap_c003_poz_supplier_sites_stg
                WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No batch_id is found for xxcnv_ap_c003_poz_supplier_sites_stg');
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking batch_id for xxcnv_ap_c003_poz_supplier_sites_stg ' || sqlerrm);
            END;
             --dbms_output.put_line('DISTINCT batch for xxcnv_ap_c003_poz_supplier_sites_stg batch_id ' || lv_batch_id);
            BEGIN
                SELECT
                    COUNT(1)
                INTO lv_success_count
                FROM
                    xxcnv_ap_c003_poz_supplier_sites_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Success record count for xxcnv_ap_c003_poz_supplier_sites_stg batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_success_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c003_poz_supplier_sites_stg batch_id: ' || lv_batch_id);
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
											,OC_PAYMENT_TERMS 		-- transformed_field
											,TERMS_DATE_BASICS
											,PAY_DATE_BASIS_LOOKUP_CODE 
											,BANK_CHARGE_DEDUCTION_TYPE
											,ALWAYS_TAKE_DISC_FLAG
											,EXCLUDE_FREIGHT_FROM_DISCOUNT
											,EXCLUDE_TAX_FROM_DISCOUNT
											,AUTO_CALCULATE_INTEREST_FLAG
                                            ,NULL AS  NULL1
                                            ,NULL AS  NULL2
											,OC_PAYMENT_METHOD  -- transformed_field
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
                                            FROM xxcnv_ap_c003_poz_supplier_sites_stg
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

                    dbms_output.put_line('xxcnv_ap_c003_poz_supplier_sites_stg CSV file batch_id '
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
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_supplier_sites_stg batch_id '
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
                    xxcnv_ap_c003_poz_sup_site_assign_stg
                WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No batch_id is found for xxcnv_ap_c003_poz_sup_site_assign_stg');
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking batch_id for xxcnv_ap_c003_poz_sup_site_assign_stg ' || sqlerrm);
            END;

            BEGIN
                -- Count the success record count for the current batch_id
                SELECT
                    COUNT(1)
                INTO lv_success_count
                FROM
                    xxcnv_ap_c003_poz_sup_site_assign_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Success record count for xxcnv_ap_c003_poz_sup_site_assign_stg batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_success_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c003_poz_sup_site_assign_stg batch_id: ' || lv_batch_id);
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
											OC_CLIENT_BU,  -- transformed_field
											OC_BILL_TO_BU,  -- transformed_field
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
                                            FROM xxcnv_ap_c003_poz_sup_site_assign_stg
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

                    dbms_output.put_line('xxcnv_ap_c003_poz_sup_site_assign_stg CSV file for batch_id '
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
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_sup_site_assign_stg batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for xxcnv_ap_c003_poz_sup_site_assign_stg batch_id '
                                     || lv_batch_id
                                     || ': Error message columns contain data.');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
            --EXIT;
        END;

	--5th table
        BEGIN
            lv_success_count := 0;
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_ap_c003_poz_sup_contacts_stg
                WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No batch_id is found for xxcnv_ap_c003_poz_sup_contacts_stg');
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking batch_id for xxcnv_ap_c003_poz_sup_contacts_stg ' || sqlerrm);
            END;

            BEGIN
                -- Count the success record count for the current batch_id
                SELECT
                    COUNT(1)
                INTO lv_success_count
                FROM
                    xxcnv_ap_c003_poz_sup_contacts_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Success record count for xxcnv_ap_c003_poz_sup_contacts_stg batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_success_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c003_poz_sup_contacts_stg batch_id: ' || lv_batch_id);
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
                                         || gv_oci_file_name_suppcontacts,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
											IMPORT_ACTION ,
											vendor_name,
											PREFIX,
											first_name,
											first_name_NEW,
											MIDDLE_NAME,
											LAST_NAME,
											LAST_NAME_NEW,
											TITLE,
											PRIMARY_ADMIN_CONTACT,
											EMAIL_ADDRESS,
											EMAIL_ADDRESS_NEW,
											PHONE_COUNTRY_CODE,
											AREA_CODE,
											PHONE,
											PHONE_EXTENSION,
											FAX_COUNTRY_CODE,
											FAX_AREA_CODE,
											FAX,
											MOBILE_COUNTRY_CODE,
											MOBILE_AREA_CODE,
											MOBILE_NUMBER,
											INACTIVE_DATE,
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
                                            batch_id,
											USER_ACCOUNT_ACTION,
                                            ROLE1,
											ROLE2,
											ROLE3,                                            
											ROLE4,
											ROLE5,
											ROLE6,
											ROLE7,
											ROLE8,
											ROLE9,
											ROLE10
                                            FROM xxcnv_ap_c003_poz_sup_contacts_stg
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

                    dbms_output.put_line('xxcnv_ap_c003_poz_sup_contacts_stg CSV file for batch_id '
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
                                       || gv_oci_file_name_suppcontacts,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for EGP_MPN_SPN_REL_LINKAGE_INT batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for xxcnv_ap_c003_poz_sup_contacts_stg batch_id '
                                     || lv_batch_id
                                     || ': Error message columns contain data.');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
        END;


	--6th table
        BEGIN
            lv_success_count := 0;
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_ap_c003_poz_sup_cont_addr_stg
                WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No batch_id is found for xxcnv_ap_c003_poz_sup_cont_addr_stg');
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking batch_id for xxcnv_ap_c003_poz_sup_cont_addr_stg ' || sqlerrm);
            END;

            BEGIN
                -- Count the success record count for the current batch_id
                SELECT
                    COUNT(1)
                INTO lv_success_count
                FROM
                    xxcnv_ap_c003_poz_sup_cont_addr_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Success record count for xxcnv_ap_c003_poz_sup_cont_addr_stg batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_success_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c003_poz_sup_cont_addr_stg batch_id: ' || lv_batch_id);
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
                                         || gv_oci_file_name_suppcontactaddress,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
												import_action,
												vENDor_name,
												party_site_name,
												first_name,
												last_name,
												email_address,
												batch_id
                                            FROM xxcnv_ap_c003_poz_sup_cont_addr_stg
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

                    dbms_output.put_line('xxcnv_ap_c003_poz_sup_cont_addr_stg CSV file for batch_id '
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
                                       || gv_oci_file_name_suppcontactaddress,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_sup_cont_addr_stg batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for xxcnv_ap_c003_poz_sup_cont_addr_stg batch_id '
                                     || lv_batch_id
                                     || ': Error message columns contain data.');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
        END;

	--7th table
        BEGIN
            lv_success_count := 0;
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_ap_c003_poz_sup_bus_class_stg
                WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No batch_id is found for xxcnv_ap_c003_poz_sup_bus_class_stg');
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking batch_id for xxcnv_ap_c003_poz_sup_bus_class_stg ' || sqlerrm);
            END;

            BEGIN
                -- Count the success record count for the current batch_id
                SELECT
                    COUNT(1)
                INTO lv_success_count
                FROM
                    xxcnv_ap_c003_poz_sup_bus_class_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Success record count for xxcnv_ap_c003_poz_sup_bus_class_stg batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_success_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c003_poz_sup_bus_class_stg batch_id: ' || lv_batch_id);
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
                                         || gv_oci_file_name_suppclassstg,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
											IMPORT_ACTION,
											vendor_name	,
											CLASSIFICATION_LOOKUP_CODE,
											CLASSIFICATION_LOOKUP_CODE_NEW,
											SUB_CLASSIFICATION ,
											CERTIFYING_AGENCY_NAME,
											CERTIFYING_AGENCY_NAME_NEW,
											CREATE_CERTIFYING_AGENCY_FLAG,
											CERTIFICATE_NUMBER,
											CERTIFICATE_NUMBER_NAME,
											TO_CHAR(START_DATE, ''YYYY/MM/DD'') AS START_DATE,
											TO_CHAR(EXPIRATION_DATE, ''YYYY/MM/DD'') AS EXPIRATION_DATE,
											NOTES,
											PROVIDED_BY_CONTACT_first_name,
											PROVIDED_BY_CONTACT_LAST_NAME,
											PROVIDED_BY_CONTACT_EMAIL,
											CONFIRMED_ON,
                                            batch_id
                                            FROM xxcnv_ap_c003_poz_sup_bus_class_stg
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

                    dbms_output.put_line('xxcnv_ap_c003_poz_sup_bus_class_stg CSV file  batch_id '
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
                                       || gv_oci_file_name_suppclassstg,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_sup_bus_class_stg batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for xxcnv_ap_c003_poz_sup_bus_class_stg batch_id '
                                     || lv_batch_id
                                     || ': Error message columns contain data.');
            END IF;
        -- END LOOP;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
            --EXIT;
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
            xxcnv_ap_c003_poz_suppliers_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_sup_address IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c003_poz_supplier_addresses_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_sup_site IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c003_poz_supplier_sites_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_sup_site_assign IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c003_poz_sup_site_assign_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_sup_cont IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c003_poz_sup_contacts_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_sup_cont_address IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c003_poz_sup_cont_addr_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_sup_bus_class IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c003_poz_sup_bus_class_stg
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
                dbms_output.put_line('Processing recon report for xxcnv_ap_c003_poz_suppliers_stg for batch_id: '
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
                                    'header' VALUE TRUE
                                ),
                        query           => 'SELECT 
										 error_message
										 ,batch_id                      
										 ,import_action                 
										 ,vendor_name                   
										 ,vendor_name_new               
										 ,segment1                      
										 ,vendor_name_alt               
										 ,organization_type_lookup_code 
										 ,vendor_type_lookup_code       
										 ,end_date_active               
										 ,business_relationship         
										 ,parent_supplier_name          
										 ,alias                         
										 ,duns_number                   
										 ,one_time_flag                 
										 ,customer_num                  
										 ,standard_industry_class       
										 ,ni_number                     
										 ,corporate_website             
										 ,chief_executive_title         
										 ,chief_executive_name          
										 ,bc_not_applicable_flag        
										 ,tax_country_code              
										 ,num_1099                      
										 ,federal_reportable_flag       
										 ,type_1099                     
										 ,state_reportable_flag         
										 ,tax_reporting_name            
										 ,name_control                  
										 ,tax_verification_date         
										 ,allow_awt_flag                
										 ,awt_group_name                
										 ,vat_code                      
										 ,vat_registration_num          
										 ,auto_tax_calc_override        
										 ,payment_method_lookup_code    
										 ,delivery_channel_code         
										 ,bank_instruction1_code        
										 ,bank_instruction2_code        
										 ,bank_instruction_details      
										 ,settlement_priority           
										 ,payment_text_message1         
										 ,payment_text_message2         
										 ,payment_text_message3         
										 ,iby_bank_varchar2ge_bearer    
										 ,payment_reason_code           
										 ,payment_reason_comments       
										 ,payment_format_code           
										 ,attribute_category            
										 ,attribute1                    
										 ,attribute2                    
										 ,attribute3                    
										 ,attribute4                    
										 ,attribute5                    
										 ,attribute6                    
										 ,attribute7                    
										 ,attribute8                    
										 ,attribute9                    
										 ,attribute10                   
										 ,attribute11                   
										 ,attribute12                   
										 ,attribute13                   
										 ,attribute14                   
										 ,attribute15                   
										 ,attribute16                   
										 ,attribute17                   
										 ,attribute18                   
										 ,attribute19                   
										 ,attribute20                   
										 ,attribute_date1               
										 ,attribute_date2               
										 ,attribute_date3               
										 ,attribute_date4               
										 ,attribute_date5               
										 ,attribute_date6               
										 ,attribute_date7               
										 ,attribute_date8               
										 ,attribute_date9               
										 ,attribute_date10              
										 ,attribute_timestamp1          
										 ,attribute_timestamp2          
										 ,attribute_timestamp3          
										 ,attribute_timestamp4          
										 ,attribute_timestamp5          
										 ,attribute_timestamp6          
										 ,attribute_timestamp7          
										 ,attribute_timestamp8          
										 ,attribute_timestamp9          
										 ,attribute_timestamp10         
										 ,attribute_number1             
										 ,attribute_number2             
										 ,attribute_number3             
										 ,attribute_number4             
										 ,attribute_number5             
										 ,attribute_number6             
										 ,attribute_number7             
										 ,attribute_number8             
										 ,attribute_number9             
										 ,attribute_number10            
										 ,global_attribute_category     
										 ,global_attribute1             
										 ,global_attribute2             
										 ,global_attribute3             
										 ,global_attribute4             
										 ,global_attribute5             
										 ,global_attribute6             
										 ,global_attribute7             
										 ,global_attribute8             
										 ,global_attribute9             
										 ,global_attribute10            
										 ,global_attribute11            
										 ,global_attribute12            
										 ,global_attribute13            
										 ,global_attribute14            
										 ,global_attribute15            
										 ,global_attribute16            
										 ,global_attribute17            
										 ,global_attribute18            
										 ,global_attribute19            
										 ,global_attribute20            
										 ,global_attribute_date1        
										 ,global_attribute_date2        
										 ,global_attribute_date3        
										 ,global_attribute_date4        
										 ,global_attribute_date5        
										 ,global_attribute_date6        
										 ,global_attribute_date7        
										 ,global_attribute_date8        
										 ,global_attribute_date9        
										 ,global_attribute_date10       
										 ,global_attribute_timestamp1   
										 ,global_attribute_timestamp2   
										 ,global_attribute_timestamp3   
										 ,global_attribute_timestamp4   
										 ,global_attribute_timestamp5   
										 ,global_attribute_timestamp6   
										 ,global_attribute_timestamp7   
										 ,global_attribute_timestamp8   
										 ,global_attribute_timestamp9   
										 ,global_attribute_timestamp10  
										 ,global_attribute_number1      
										 ,global_attribute_number2      
										 ,global_attribute_number3      
										 ,global_attribute_number4      
										 ,global_attribute_number5      
										 ,global_attribute_number6      
										 ,global_attribute_number7      
										 ,global_attribute_number8      
										 ,global_attribute_number9      
										 ,global_attribute_number10     
										 ,party_number                  
										 ,service_level_code            
										 ,exclusive_payment_flag        
										 ,remit_advice_delivery_method  
										 ,remit_advice_email            
										 ,remit_advice_fax              
										 ,datafox_company_id            
										 ,file_name                                      
										 ,import_status                 
										 ,file_reference_identifier     
										 ,execution_id                  
										 ,source_system                 
									FROM xxcnv_ap_c003_poz_suppliers_stg
                                    where import_status = '''
                                 || 'ERROR'
                                 || '''
									and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c003_poz_suppliers_stg for batch_id '
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
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_suppliers_stg for batch_id '
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
                dbms_output.put_line('Processing recon report for xxcnv_ap_c003_poz_supplier_addresses_stg for batch_id: '
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
                                    'header' VALUE TRUE
                                ),
                        query           => 'SELECT 
											    error_message  
												,batch_id                    
												,import_action               
												,vendor_name                 
												,party_site_name             
												,party_site_name_NEW
												,country                     
												,address_line1               
												,address_line2               
												,address_line3               
												,address_line4               
												,address_lines_phonetic      
												,addr_element_attribute1     
												,addr_element_attribute2     
												,addr_element_attribute3     
												,addr_element_attribute4     
												,addr_element_attribute5     
												,building                    
												,floor_number                
												,city                        
												,state                       
												,province                    
												,county                      
												,postal_code                 
												,postal_plus4_code           
												,addressee                   
												,global_location_number      
												,party_site_language         
												,inactive_date               
												,phone_country_code          
												,phone_area_code             
												,phone                       
												,phone_extension             
												,fax_country_code            
												,fax_area_code               
												,fax                         
												,rfq_or_bidding_purpose_flag 
												,ordering_purpose_flag       
												,remit_to_purpose_flag       
												,attribute_category          
												,attribute1                  
												,attribute2                  
												,attribute3                  
												,attribute4                  
												,attribute5                  
												,attribute6                  
												,attribute7                  
												,attribute8                  
												,attribute9                  
												,attribute10                 
												,attribute11                 
												,attribute12                 
												,attribute13                 
												,attribute14                 
												,attribute15                 
												,attribute16                 
												,attribute17                 
												,attribute18                 
												,attribute19                 
												,attribute20                 
												,attribute21                 
												,attribute22                 
												,attribute23                 
												,attribute24                 
												,attribute25                 
												,attribute26                 
												,attribute27                 
												,attribute28                 
												,attribute29                 
												,attribute30                 
												,attribute_number1           
												,attribute_number2           
												,attribute_number3           
												,attribute_number4           
												,attribute_number5           
												,attribute_number6           
												,attribute_number7           
												,attribute_number8           
												,attribute_number9           
												,attribute_number10          
												,attribute_number11          
												,attribute_number12          
												,attribute_date1             
												,attribute_date2             
												,attribute_date3             
												,attribute_date4             
												,attribute_date5             
												,attribute_date6             
												,attribute_date7             
												,attribute_date8             
												,attribute_date9             
												,attribute_date10            
												,attribute_date11            
												,attribute_date12            
												,email_address               
												,delivery_channel_code       
												,bank_instruction1           
												,bank_instruction2           
												,bank_instruction            
												,settlement_priority         
												,payment_text_message1       
												,payment_text_message2       
												,payment_text_message3       
												,service_level_code          
												,exclusive_payment_flag      
												,iby_bank_charge_bearer      
												,payment_reason_code         
												,payment_reason_comments     
												,remit_advice_delivery_method
												,remittance_email            
												,remit_advice_fax            
												,file_name                                
												,import_status               
												,file_reference_identifier   
												,execution_id                
												,source_system               
									FROM xxcnv_ap_c003_poz_supplier_addresses_stg
                                    where import_status = '''
                                 || 'ERROR'
                                 || '''
									and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c003_poz_supplier_addresses_stg for batch_id '
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
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_supplier_addresses_stg for batch_id '
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
                dbms_output.put_line('Processing recon report for xxcnv_ap_c003_poz_supplier_sites_stg for batch_id: '
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
                                    'header' VALUE TRUE
                                ),
                        query           => 'SELECT 
											         error_message 
													,batch_id                      
													,import_action                 
													,vendor_name                   
													,procurement_business_unit_name
													,party_site_name               
													,vendor_site_code              
													,vendor_site_code_new          
													,inactive_date                 
													,rfq_only_site_flag            
													,purchasing_site_flag          
													,pcard_site_flag               
													,pay_site_flag                 
													,primary_pay_site_flag         
													,tax_reporting_site_flag       
													,vendor_site_code_alt          
													,customer_num                  
													,b2b_communication_method      
													,b2b_site_code                 
													,supplier_notif_method         
													,email_address                 
													,fax_country_code              
													,fax_area_code                 
													,fax                           
													,hold_flag                     
													,purchasing_hold_reason        
													,carrier                       
													,mode_of_transport_code        
													,service_level_code            
													,freight_terms_lookup_code     
													,pay_on_code                   
													,fob_lookup_code               
													,country_of_origin_code        
													,buyer_managed_transport_flag  
													,pay_on_use_flag               
													,aging_onset_point             
													,aging_period_days             
													,consumption_advice_frequency  
													,consumption_advice_summary    
													,default_pay_site_code         
													,pay_on_receipt_summary_code   
													,gapless_inv_num_flag          
													,selling_company_identifier    
													,create_debit_memo_flag        
													,enforce_ship_to_location_code 
													,receiving_routing_id          
													,qty_rcv_tolerance             
													,qty_rcv_exception_code        
													,days_early_receipt_allowed    
													,days_late_receipt_allowed     
													,allow_substitute_receipts_flag
													,allow_unordered_receipts_flag 
													,receipt_days_exception_code   
													,invoice_currency_code         
													,invoice_amount_limit          
													,match_option                  
													,match_approval_level          
													,payment_currency_code         
													,payment_priority              
													,pay_group_lookup_code         
													,tolerance_name                
													,services_tolerance            
													,hold_all_payments_flag        
													,hold_unmatched_invoices_flag  
													,hold_future_payments_flag     
													,hold_by                       
													,payment_hold_date             
													,hold_reason                   
													,terms_name                    
													,terms_date_basics             
													,pay_date_basis_lookup_code    
													,bank_charge_deduction_type    
													,always_take_disc_flag         
													,exclude_freight_from_discount 
													,exclude_tax_from_discount     
													,auto_calculate_interest_flag  
													,vat_code_obsoleted            
													,tax_registration_number_obsoleted
													,payment_method_lookup_code    
													,delivery_channel_code         
													,bank_instruction1_code        
													,bank_instruction2_code        
													,bank_instruction_details      
													,settlement_priority           
													,payment_text_message1         
													,payment_text_message2         
													,payment_text_message3         
													,iby_bank_varchar2ge_bearer    
													,payment_reason_code           
													,payment_reason_comments       
													,remit_advice_delivery_method  
													,remittance_email              
													,remit_advice_fax              
													,attribute_category            
													,attribute1                    
													,attribute2                    
													,attribute3                    
													,attribute4                    
													,attribute5                    
													,attribute6                    
													,attribute7                    
													,attribute8                    
													,attribute9                    
													,attribute10                   
													,attribute11                   
													,attribute12                   
													,attribute13                   
													,attribute14                   
													,attribute15                   
													,attribute16                   
													,attribute17                   
													,attribute18                   
													,attribute19                   
													,attribute20                   
													,attribute_date1               
													,attribute_date2               
													,attribute_date3               
													,attribute_date4               
													,attribute_date5               
													,attribute_date6               
													,attribute_date7               
													,attribute_date8               
													,attribute_date9               
													,attribute_date10              
													,attribute_timestamp1          
													,attribute_timestamp2          
													,attribute_timestamp3          
													,attribute_timestamp4          
													,attribute_timestamp5          
													,attribute_timestamp6          
													,attribute_timestamp7          
													,attribute_timestamp8          
													,attribute_timestamp9          
													,attribute_timestamp10         
													,attribute_number1             
													,attribute_number2             
													,attribute_number3             
													,attribute_number4             
													,attribute_number5             
													,attribute_number6             
													,attribute_number7             
													,attribute_number8             
													,attribute_number9             
													,attribute_number10            
													,global_attribute_category     
													,global_attribute1             
													,global_attribute2             
													,global_attribute3             
													,global_attribute4             
													,global_attribute5             
													,global_attribute6             
													,global_attribute7             
													,global_attribute8             
													,global_attribute9             
													,global_attribute10            
													,global_attribute11            
													,global_attribute12            
													,global_attribute13            
													,global_attribute14            
													,global_attribute15            
													,global_attribute16            
													,global_attribute17            
													,global_attribute18            
													,global_attribute19            
													,global_attribute20            
													,global_attribute_date1        
													,global_attribute_date2        
													,global_attribute_date3        
													,global_attribute_date4        
													,global_attribute_date5        
													,global_attribute_date6        
													,global_attribute_date7        
													,global_attribute_date8        
													,global_attribute_date9        
													,global_attribute_date10       
													,global_attribute_timestamp1   
													,global_attribute_timestamp2   
													,global_attribute_timestamp3   
													,global_attribute_timestamp4   
													,global_attribute_timestamp5   
													,global_attribute_timestamp6   
													,global_attribute_timestamp7   
													,global_attribute_timestamp8   
													,global_attribute_timestamp9   
													,global_attribute_timestamp10  
													,global_attribute_number1      
													,global_attribute_number2      
													,global_attribute_number3      
													,global_attribute_number4      
													,global_attribute_number5      
													,global_attribute_number6      
													,global_attribute_number7      
													,global_attribute_number8      
													,global_attribute_number9      
													,global_attribute_number10     
													,po_ack_reqd_code              
													,po_ack_reqd_days              
													,invoice_channel               
													,payee_service_level_code      
													,exclusive_parent_flag         
													,file_name                                     
													,import_status                 
													,file_reference_identifier     
													,execution_id                  
													,source_system                 
                                            FROM xxcnv_ap_c003_poz_supplier_sites_stg
											where import_status = '''
                                 || 'ERROR'
                                 || '''
											and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c003_poz_supplier_sites_stg for batch_id '
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
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_supplier_sites_stg for batch_id '
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
                dbms_output.put_line('Processing recon report for xxcnv_ap_c003_poz_sup_site_assign_stg for batch_id: '
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
                                    'header' VALUE TRUE
                                ),
                        query           => 'SELECT 
											 error_message  
											,batch_id                       
											,import_action                  
											,vendor_name                    
											,vendor_site_code               
											,procurement_business_unit_name 
											,business_unit_name             
											,bill_to_bu_name                
											,ship_to_location_code          
											,bill_to_location_code          
											,allow_awt_lag                  
											,awt_group_name                 
											,accts_pay_concatenated_segments
											,prepay_concat_segments         
											,future_dated_concat_segments   
											,distribution_set_name          
											,inactive_date                  
											,file_name                      
											,import_status                  
											,file_reference_identifier      
											,execution_id                   
											,source_system                  
                                            FROM xxcnv_ap_c003_poz_sup_site_assign_stg
											where import_status = '''
                                 || 'ERROR'
                                 || '''
											and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c003_poz_sup_site_assign_stg for batch_id '
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
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_sup_site_assign_stg for batch_id '
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

-- Table 5
        BEGIN
            FOR g_id IN batch_id_sup_cont LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing recon report for xxcnv_ap_c003_poz_sup_contacts_stg for batch_id: '
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
                                         || 'ATP_Recon_Supplier_Contacts'
                                         || '_'
                                         || sysdate,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600',
                                    'header' VALUE TRUE
                                ),
                        query           => 'SELECT 
												error_message  
												,batch_id                 
												,import_action            
												,vendor_name              
												,prefix                   
												,first_name               
												,first_name_new           
												,middle_name              
												,last_name                
												,last_name_new            
												,title                    
												,primary_admin_contact    
												,email_address            
												,email_address_new        
												,phone_country_code       
												,area_code                
												,phone                    
												,phone_extension          
												,fax_country_code         
												,fax_area_code            
												,fax                      
												,mobile_country_code      
												,mobile_area_code         
												,mobile_number            
												,inactive_date            
												,attribute_category       
												,attribute1               
												,attribute2               
												,attribute3               
												,attribute4               
												,attribute5               
												,attribute6               
												,attribute7               
												,attribute8               
												,attribute9               
												,attribute10              
												,attribute11              
												,attribute12              
												,attribute13              
												,attribute14              
												,attribute15              
												,attribute16              
												,attribute17              
												,attribute18              
												,attribute19              
												,attribute20              
												,attribute21              
												,attribute22              
												,attribute23              
												,attribute24              
												,attribute25              
												,attribute26              
												,attribute27              
												,attribute28              
												,attribute29              
												,attribute30              
												,attribute_number1        
												,attribute_number2        
												,attribute_number3        
												,attribute_number4        
												,attribute_number5        
												,attribute_number6        
												,attribute_number7        
												,attribute_number8        
												,attribute_number9        
												,attribute_number10       
												,attribute_number11       
												,attribute_number12       
												,attribute_date1          
												,attribute_date2          
												,attribute_date3          
												,attribute_date4          
												,attribute_date5          
												,attribute_date6          
												,attribute_date7          
												,attribute_date8          
												,attribute_date9          
												,attribute_date10         
												,attribute_date11         
												,attribute_date12         
												,user_account_action      
												,role1                    
												,role2                    
												,role3                    
												,role4                    
												,role5                    
												,role6                    
												,role7                    
												,role8                    
												,role9                    
												,role10                   
												,file_name                          
												,import_status            
												,file_reference_identifier
												,execution_id             
												,source_system            
                                            FROM xxcnv_ap_c003_poz_sup_contacts_stg
											where import_status = '''
                                 || 'ERROR'
                                 || '''
											and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c003_poz_sup_contacts_stg for batch_id '
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
                                       || gv_oci_file_name_suppcontacts,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_sup_contacts_stg for batch_id '
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

-- Table 5
        BEGIN
            FOR g_id IN batch_id_sup_cont_address LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing recon report for xxcnv_ap_c003_poz_sup_cont_addr_stg for batch_id: '
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
                                         || 'ATP_Recon_Supplier_Contact_Addresses'
                                         || '_'
                                         || sysdate,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600',
                                    'header' VALUE TRUE
                                ),
                        query           => 'SELECT 
												error_message 
												,batch_id                 
												,import_action            
												,vendor_name              
												,party_site_name          
												,first_name               
												,last_name                
												,email_address            
												,file_name                
												,import_status            
												,file_reference_identifier
												,execution_id             
												,source_system            
                                            FROM xxcnv_ap_c003_poz_sup_cont_addr_stg
											where import_status = '''
                                 || 'ERROR'
                                 || '''
											and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c003_poz_sup_cont_addr_stg for batch_id '
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
                                       || gv_oci_file_name_suppcontactaddress,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_sup_cont_addr_stg for batch_id '
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

-- Table 5
        BEGIN
            FOR g_id IN batch_id_sup_bus_class LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing recon report for xxcnv_ap_c003_poz_sup_bus_class_stg for batch_id: '
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
                                         || 'ATP_Recon_Supplier_Business_Classification'
                                         || '_'
                                         || sysdate,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600',
                                    'header' VALUE TRUE
                                ),
                        query           => 'SELECT 
												error_message  
												,batch_id                      
												,import_action                 
												,vendor_name                   
												,classification_lookup_code    
												,classification_lookup_code_new
												,sub_classification            
												,certifying_agency_name        
												,certifying_agency_name_new    
												,create_certifying_agency_flag 
												,certificate_number            
												,certificate_number_name       
												,start_date                    
												,expiration_date               
												,notes                         
												,provided_by_contact_first_name
												,provided_by_contact_last_name 
												,provided_by_contact_email     
												,confirmed_on                  
												,file_name                     	
												,import_status                 
												,file_reference_identifier     
												,execution_id                  
												,source_system                 
                                            FROM xxcnv_ap_c003_poz_sup_bus_class_stg
											where import_status = '''
                                 || 'ERROR'
                                 || '''
											and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c003_poz_sup_bus_class_stg for batch_id '
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
                                       || gv_oci_file_name_suppclassstg,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c003_poz_sup_bus_class_stg for batch_id '
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

END xxcnv_ap_c003_supplier_conversion_pkg;