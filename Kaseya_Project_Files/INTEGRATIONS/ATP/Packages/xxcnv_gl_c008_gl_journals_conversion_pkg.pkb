create or replace PACKAGE BODY  xxcnv.xxcnv_gl_c008_gl_journals_conversion_pkg IS
	/*************************************************************************************
    NAME              :     GL_Conversion_Package BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -----------------------------------------
	-- 	Priyanka S Kadam	  27-Feb-2024  	    1.0         Initial Development
	-- 	Priyanka Kadam	      29-Feb-2024  	    1.1         LTCI-6261_COA_Mapper_changes
	--  Satya Pavani          11-Aug-2025       1.2         LTCI-6984-CVR and FBDI file_name
    --  Satya Pavani          25-Aug-2025       1.3         LTCI-7741 - Handling file_names which are having commas and attribute8
	****************************************************************************************/

---Declaring global Variables

    gv_import_status             VARCHAR2(256) := NULL;
    gv_error_message             VARCHAR2(500) := NULL;
    gv_file_name                 VARCHAR2(256) := NULL;
    gv_oci_file_path             VARCHAR2(200) := NULL;
    gv_oci_file_name             VARCHAR2(100) := NULL;
    gv_execution_id              VARCHAR2(30) := NULL;
    gv_group_id                  NUMBER(18) := NULL;
    gv_credential_name           CONSTANT VARCHAR2(25) := 'OCI$RESOURCE_PRINCIPAL';
    gv_status_success            CONSTANT VARCHAR2(15) := 'Success';
    gv_status_failure            CONSTANT VARCHAR2(15) := 'Failure';
    gv_conversion_id             VARCHAR2(15) := NULL;
    gv_boundary_system           VARCHAR2(25) := NULL;
    gv_status_picked             CONSTANT VARCHAR2(100) := 'File_Picked_From_Oci_And_Loaded_To_Stg';
    gv_status_picked_for_tr      CONSTANT VARCHAR2(100) := 'Transformed_Data_From_Ext_To_Stg';
    gv_status_validated          CONSTANT VARCHAR2(50) := 'Validated';
    gv_status_failed             CONSTANT VARCHAR2(50) := 'Failed_At_Validation';
    gv_coa_transformation        CONSTANT VARCHAR2(50) := 'Coa_Transformation';
    gv_coa_transformation_failed CONSTANT VARCHAR2(50) := 'Coa_Transformation_Failed';
    gv_fbdi_export_status        CONSTANT VARCHAR2(50) := 'Exported_To_Fbdi';
    gv_status_staged             CONSTANT VARCHAR2(50) := 'Staged_For_Import';
    gv_transformed_folder        CONSTANT VARCHAR2(100) := 'Transformed_FBDI_Files';
    gv_source_folder             CONSTANT VARCHAR2(100) := 'Source_FBDI_Files';
    gv_properties                CONSTANT VARCHAR2(15) := 'properties';
    gv_file_picked               VARCHAR2(50) := 'File_Picked_From_OCI_Server';
    gv_data_validated_success    CONSTANT VARCHAR2(50) := 'Data_Validated';
    gv_data_validated_failure    CONSTANT VARCHAR2(50) := 'Data_Not_Validated';
    gv_external_table            CONSTANT VARCHAR2(30) := 'xxcnv_gl_c008_gl_interface_ext';
    gv_ledger_name               CONSTANT VARCHAR2(50) := 'US USGAAP USD'; -- check while running
--	gv_cloud_currency       CONSTANT    VARCHAR2(50)    := 'USD'
    gv_legacy_currency           CONSTANT VARCHAR2(50) := 'USD';
    gv_recon_folder              CONSTANT VARCHAR2(50) := 'ATP_Validation_Error_Files';
    gv_recon_report              CONSTANT VARCHAR2(50) := 'Recon_Report_Created';
    gv_src_file_cnt              NUMBER := 0;
    gv_trs_file_cnt              NUMBER := 0;
    gv_src_act_dr                NUMBER := 0;
    gv_src_act_cr                NUMBER := 0;
    gv_trs_act_dr                NUMBER := 0;
    gv_trs_act_cr                NUMBER := 0;

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
    BEGIN
        gv_conversion_id := p_rice_id;
        gv_execution_id := p_execution_id;
        gv_boundary_system := p_boundary_system;
        dbms_output.put_line('Conversion_id: ' || gv_conversion_id);
        dbms_output.put_line('Execution_id: ' || gv_execution_id);
        dbms_output.put_line('Boundary_system: ' || gv_boundary_system);
   /* BEGIN
        -- gv_oci_file_NAME := p_current_file_name;
        -- gv_execution_id := p_execution_id;

        BEGIN
            SELECT   
                cm.id 
                -- cm.obj_storage_path
            INTO    
                gv_conversion_id
                -- gv_oci_file_path
            FROM    
                conversion_metadata cm
            WHERE
                cm.id = gv_conversion_id;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error fetching conversion metadata: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
                -- RETURN;
        END;

    END; 
    */
    --  dbms_output.put_line('conversion_id: '|| gv_conversion_id);

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

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error fetching execution details: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    RETURN;
            END;

            dbms_output.put_line('File_name: ' || gv_oci_file_name);
            dbms_output.put_line('File_path: ' || gv_oci_file_path);
        END;

    -- Call to import data from OCI to external table
        BEGIN
            import_data_from_oci_to_stg_prc(p_loading_status);--PK
            IF p_loading_status = gv_status_failure THEN
                dbms_output.put_line('Error in IMPORT_DATA_FROM_OCI_TO_STG');
                RETURN;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling import_data_from_oci_to_stg_prc: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                RETURN;
        END;

    -- Call to perform data and business validations in staging table
        BEGIN
            data_validations_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling data_validations_prc: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                RETURN;
        END;

	   -- Call to perform COA transaction
        BEGIN
            coa_target_segments_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling coa_target_segments_prc: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                RETURN;
        END; 

	-- Call to load balancing lines
        BEGIN
            load_balancing_line_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling load_balancing_line_prc: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                RETURN;
        END;

	---- Call cvr_rule_check_prc()
        BEGIN
            cvr_rule_check_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling cvr_rule_check_prc: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                RETURN;
        END;

    -- Call to create a CSV file from xxcnv_gl_c008_gl_interface_stg after all validations
        BEGIN
            create_fbdi_file_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_fbdi_file_prc: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                RETURN;
        END;

        BEGIN
            create_atp_validation_recon_report_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_atp_validation_recon_report_prc: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                RETURN;
        END; 

	/*
	    -- Call to create a CSV file from xxcnv_gl_c008_gl_interface_stg after all validations
    BEGIN
        create_properties_file_prc;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling create_properties_file: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
            RETURN;
    END;
	*/

    END main_prc;

/*=================================================================================================================
-- PROCEDURE : IMPORT_DATA_FROM_OCI_TO_EXT
-- PARAMETERS: p_loading_status
-- COMMENT   : This procedure is used to create an external table and transfer that data from external to stg table.
===================================================================================================================*/

    PROCEDURE import_data_from_oci_to_stg_prc (
        p_loading_status OUT VARCHAR2
    ) IS

        lv_table_count   NUMBER := 0;
        lv_row_count     NUMBER := 0;
        lv_oci_file_name VARCHAR2(20) := 'GlInterface*.csv';
    BEGIN
        BEGIN
        -- Check if the external table exists and drop it if it does
            SELECT
                COUNT(*)
            INTO lv_table_count
            FROM
                all_objects
            WHERE
                    lower(object_name) = gv_external_table
                AND object_type = 'TABLE';

            IF lv_table_count > 0 THEN
                EXECUTE IMMEDIATE 'DROP TABLE xxcnv_gl_c008_gl_interface_ext';
                EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_gl_c008_gl_interface_stg';
                dbms_output.put_line('Table xxcnv_gl_c008_gl_interface_ext dropped');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error dropping table xxcnv_gl_c008_gl_interface_ext: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                p_loading_status := gv_status_failure;
                RETURN;
        END;

        BEGIN
            dbms_output.put_line('Creating an external table:'
                                 || gv_oci_file_path
                                 || '/'
                                 || lv_oci_file_name);

        -- Create the external table
            lv_oci_file_name := 'GlInterface*.csv';
            dbms_cloud.create_external_table(
                table_name      => 'xxcnv_gl_c008_gl_interface_ext',
                credential_name => 'OCI$RESOURCE_PRINCIPAL',
                file_uri_list   => gv_oci_file_path
                                 || '/'
                                 || lv_oci_file_name,
                format          =>
                        JSON_OBJECT(
                            'skipheaders' VALUE '1',
                            'type' VALUE 'csv',
                            'rejectlimit' VALUE 'UNLIMITED',
                            'dateformat' VALUE 'yyyy/mm/dd',
                            'ignoremissingcolumns' VALUE 'true',
                                    'blankasnull' VALUE 'true'
                        ),
                column_list     => 'status VARCHAR2(50),
						ledger_id NUMBER(18),
						accounting_date DATE,
						user_je_source_name VARCHAR2(25),
						user_je_category_name VARCHAR2(25),
						currency_code VARCHAR2(15),
						date_created DATE,
						actual_flag VARCHAR2(1),
						segment1 VARCHAR2(25),
						segment2 VARCHAR2(25),
						segment3 VARCHAR2(25),
						segment4 VARCHAR2(25),
						segment5 VARCHAR2(50),
						segment6 VARCHAR2(25),
						segment7 VARCHAR2(200),
						segment8 VARCHAR2(25),
						segment9 VARCHAR2(25),
						segment10 VARCHAR2(25),
						segment11 VARCHAR2(25),
						segment12 VARCHAR2(25),
						segment13 VARCHAR2(25),
						segment14 VARCHAR2(25),
						segment15 VARCHAR2(25),
						segment16 VARCHAR2(25),
						segment17 VARCHAR2(25),
						segment18 VARCHAR2(25),
						segment19 VARCHAR2(25),
						segment20 VARCHAR2(25),
						segment21 VARCHAR2(25),
						segment22 VARCHAR2(25),
						segment23 VARCHAR2(25),
						segment24 VARCHAR2(25),
						segment25 VARCHAR2(25),
						segment26 VARCHAR2(25),
						segment27 VARCHAR2(25),
						segment28 VARCHAR2(25),
						segment29 VARCHAR2(25),
						segment30 VARCHAR2(25),
						entered_dr NUMBER,
						entered_cr NUMBER,
						accounted_dr NUMBER,
						accounted_cr NUMBER,
						reference1 VARCHAR2(100),
						reference2 VARCHAR2(240),
						reference3 VARCHAR2(240),
						reference4 VARCHAR2(100),
						reference5 VARCHAR2(240),
						reference6 VARCHAR2(100),
						reference7 VARCHAR2(100),
						reference8 VARCHAR2(100),
						reference9 VARCHAR2(100),
						reference10 VARCHAR2(400),
						reference21 VARCHAR2(240),
						reference22 VARCHAR2(240),
						reference23 VARCHAR2(240),
						reference24 VARCHAR2(240),
						reference25 VARCHAR2(240),
						reference26 VARCHAR2(240),
						reference27 VARCHAR2(240),
						reference28 VARCHAR2(240),
						reference29 VARCHAR2(240),
						reference30 VARCHAR2(240),
						stat_amount NUMBER,
						user_currency_conversion_type VARCHAR2(30),
						currency_conversion_date DATE,
						currency_conversion_rate NUMBER,
						group_id NUMBER(18),
						attribute_category VARCHAR2(150),
						attribute1 VARCHAR2(150),
						attribute2 VARCHAR2(150),
						attribute3 VARCHAR2(150),
						attribute4 VARCHAR2(150),
						attribute5 VARCHAR2(150),
						attribute6 VARCHAR2(150),
						attribute7 VARCHAR2(200),
						attribute8 VARCHAR2(150),
						attribute9 VARCHAR2(150),
						attribute10 VARCHAR2(150),
						attribute11 VARCHAR2(150),
						attribute12 VARCHAR2(150),
						attribute13 VARCHAR2(150),
						attribute14 VARCHAR2(150),
						attribute15 VARCHAR2(150),
						attribute16 VARCHAR2(150),
						attribute17 VARCHAR2(150),
						attribute18 VARCHAR2(150),
						attribute19 VARCHAR2(150),
						attribute20 VARCHAR2(150),
						attribute_category3 VARCHAR2(150),
						average_journal_flag VARCHAR2(1),
						originating_bal_seg_value VARCHAR2(25),
						ledger_name VARCHAR2(30),
						encumbrance_type_id NUMBER,				
						jgzz_recon_ref VARCHAR2(240),
						period_name VARCHAR2(15),
						reference18 VARCHAR2(100),
						reference19 VARCHAR2(100),
						reference20 VARCHAR2(100),
						attribute_date1 DATE,
						attribute_date2 DATE,
						attribute_date3 DATE,
						attribute_date4 DATE,
						attribute_date5 DATE,
						attribute_date6 DATE,
						attribute_date7 DATE,
						attribute_date8 DATE,
						attribute_date9 DATE,
						attribute_date10 DATE,
						attribute_number1 NUMBER,
						attribute_number2 NUMBER,
						attribute_number3 NUMBER,
						attribute_number4 NUMBER,
						attribute_number5 NUMBER,
						attribute_number6 NUMBER,
						attribute_number7 NUMBER,
						attribute_number8 NUMBER,
						attribute_number9 NUMBER,
						attribute_number10 NUMBER,
						global_attribute_category VARCHAR2(30),
						global_attribute1 VARCHAR2(150),
						global_attribute2 VARCHAR2(150),
						global_attribute3 VARCHAR2(150),
						global_attribute4 VARCHAR2(150),
						global_attribute5 VARCHAR2(150),
						global_attribute6 VARCHAR2(150),
						global_attribute7 VARCHAR2(150),
						global_attribute8 VARCHAR2(150),
						global_attribute9 VARCHAR2(150),
						global_attribute10 VARCHAR2(150),
						global_attribute11 VARCHAR2(150),
						global_attribute12 VARCHAR2(150),
						global_attribute13 VARCHAR2(150),
						global_attribute14 VARCHAR2(150),
						global_attribute15 VARCHAR2(150),
						global_attribute16 VARCHAR2(150),
						global_attribute17 VARCHAR2(150),
						global_attribute18 VARCHAR2(150),
						global_attribute19 VARCHAR2(150),
						global_attribute20 VARCHAR2(150),
						global_attribute_date1 DATE,
						global_attribute_date2 DATE,
						global_attribute_date3 DATE,
						global_attribute_date4 DATE,
						global_attribute_date5 DATE,
						global_attribute_number1 NUMBER,
						global_attribute_number2 NUMBER,
						global_attribute_number3 NUMBER,
						global_attribute_number4 NUMBER,
						global_attribute_number5 NUMBER'
            );

            EXECUTE IMMEDIATE 'INSERT INTO xxcnv_gl_c008_gl_interface_stg (
						status,                        
						ledger_id, 
						accounting_date,
						user_je_source_name, 
						user_je_category_name,
						currency_code,
						date_created,
						actual_flag,
						segment1,                  
						segment2,
						segment3,
						segment4,
						segment5,
						segment6,
						segment7,
						segment8,
						segment9,
						segment10, 
						segment11, 
						segment12,
						segment13,
						segment14,
						segment15,
						segment16,
						segment17,
						segment18,
						segment19,
						segment20,
						segment21,
						segment22,
						segment23,
						segment24,
						segment25,
						segment26,
						segment27,
						segment28,
						segment29,
						segment30,
						entered_dr, 
						entered_cr,
						accounted_dr,
						accounted_cr,
						reference1, 
						reference2, 
						reference3, 
						reference4, 
						reference5, 
						reference6, 
						reference7, 
						reference8, 
						reference9, 
						reference10,  
						reference21, 
						reference22, 
						reference23, 
						reference24, 
						reference25, 
						reference26, 
						reference27, 
						reference28, 
						reference29, 
						reference30, 
						stat_amount, 
						user_currency_conversion_type, 
						currency_conversion_date,   
						currency_conversion_rate,  
						group_id,
						attribute_category,
						attribute1, 
						attribute2, 
						attribute3, 
						attribute4, 
						attribute5, 
						attribute6, 
						attribute7, 
						attribute8, 
						attribute9, 
						attribute10, 
						attribute11, 
						attribute12, 
						attribute13, 
						attribute14, 
						attribute15, 
						attribute16, 
						attribute17, 
						attribute18, 
						attribute19, 
						attribute20, 
						attribute_category3,  
						average_journal_flag,
						originating_bal_seg_value,
						ledger_name,
						encumbrance_type_id,
						jgzz_recon_ref,
						period_name,              
						reference18,       
						reference19,
						reference20, 
						attribute_date1,
						attribute_date2,
						attribute_date3,
						attribute_date4,
						attribute_date5,
						attribute_date6,
						attribute_date7,
						attribute_date8,
						attribute_date9,
						attribute_date10,
						attribute_number1,
						attribute_number2,
						attribute_number3,
						attribute_number4,
						attribute_number5,
						attribute_number6,
						attribute_number7,
						attribute_number8,
						attribute_number9,
						attribute_number10,
						global_attribute_category, 
						global_attribute1, 
						global_attribute2, 
						global_attribute3, 
						global_attribute4, 
						global_attribute5, 
						global_attribute6, 
						global_attribute7, 
						global_attribute8, 
						global_attribute9, 
						global_attribute10, 
						global_attribute11, 
						global_attribute12, 
						global_attribute13, 
						global_attribute14, 
						global_attribute15, 
						global_attribute16, 
						global_attribute17, 
						global_attribute18, 
						global_attribute19, 
						global_attribute20, 
						global_attribute_date1,
						global_attribute_date2,
						global_attribute_date3,
						global_attribute_date4,
						global_attribute_date5,
						global_attribute_number1,
						global_attribute_number2,
						global_attribute_number3,
						global_attribute_number4,
						global_attribute_number5,
						file_name,
						error_message,
						import_status,
						target_segment1,
						target_segment2,
						target_segment3,
						target_segment4,
						target_segment5,
						target_segment6,
						target_segment7,
						target_segment8,
						target_segment9,
						target_segment10,
						file_reference_identifier,
						source_system,
						execution_id
					) SELECT 
						status,                        
						ledger_id, 
						accounting_date,
						user_je_source_name, 
						user_je_category_name,
						currency_code,
						date_created,
						actual_flag,
						segment1,                  
						segment2,
						segment3,
						segment4,
						segment5,
						segment6,
						segment7,
						segment8,
						segment9,
						segment10, 
						segment11, 
						segment12,
						segment13,
						segment14,
						segment15,
						segment16,
						segment17,
						segment18,
						segment19,
						segment20,
						segment21,
						segment22,
						segment23,
						segment24,
						segment25,
						segment26,
						segment27,
						segment28,
						segment29,
						segment30,
						entered_dr, 
						entered_cr,
						accounted_dr,
						accounted_cr,
						reference1, 
						reference2, 
						reference3, 
						reference4, 
						reference5, 
						reference6, 
						reference7, 
						reference8, 
						reference9, 
						reference10,  
						reference21, 
						reference22, 
						reference23, 
						reference24, 
						reference25, 
						reference26, 
						reference27, 
						reference28, 
						reference29, 
						reference30, 
						stat_amount, 
						user_currency_conversion_type, 
						currency_conversion_date,   
						currency_conversion_rate,  
						group_id,
						attribute_category,
						attribute1, 
						attribute2, 
						attribute3, 
						attribute4, 
						attribute5, 
						attribute6, 
						attribute7, 
						attribute8, 
						attribute9, 
						attribute10, 
						attribute11, 
						attribute12, 
						attribute13, 
						attribute14, 
						attribute15, 
						attribute16, 
						attribute17, 
						attribute18, 
						attribute19, 
						attribute20, 
						attribute_category3,  
						average_journal_flag,
						originating_bal_seg_value,
						ledger_name,
						encumbrance_type_id,
						jgzz_recon_ref,
						period_name,              
						reference18,       
						reference19,
						reference20, 
						attribute_date1,
						attribute_date2,
						attribute_date3,
						attribute_date4,
						attribute_date5,
						attribute_date6,
						attribute_date7,
						attribute_date8,
						attribute_date9,
						attribute_date10,
						attribute_number1,
						attribute_number2,
						attribute_number3,
						attribute_number4,
						attribute_number5,
						attribute_number6,
						attribute_number7,
						attribute_number8,
						attribute_number9,
						attribute_number10,
						global_attribute_category, 
						global_attribute1, 
						global_attribute2, 
						global_attribute3, 
						global_attribute4, 
						global_attribute5, 
						global_attribute6, 
						global_attribute7, 
						global_attribute8, 
						global_attribute9, 
						global_attribute10, 
						global_attribute11, 
						global_attribute12, 
						global_attribute13, 
						global_attribute14, 
						global_attribute15, 
						global_attribute16, 
						global_attribute17, 
						global_attribute18, 
						global_attribute19, 
						global_attribute20, 
						global_attribute_date1,
						global_attribute_date2,
						global_attribute_date3,
						global_attribute_date4,
						global_attribute_date5,
						global_attribute_number1,
						global_attribute_number2,
						global_attribute_number3,
						global_attribute_number4,
						global_attribute_number5,
						null,
						null,
						null,			
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
                        null,
						null
						FROM xxcnv_gl_c008_gl_interface_ext';
            dbms_output.put_line('Inserted records in the xxcnv_gl_c008_gl_interface_stg from OCI Source Folder: ' || SQL%rowcount);
            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                p_loading_status := gv_status_failure;
                dbms_output.put_line('Error in load_staging_table: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                p_loading_status := gv_status_failure;
                RETURN;
        END;

        BEGIN
        -- Count the number of rows in the external table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_gl_c008_gl_interface_stg;

        -- Use an implicit cursor in the FOR LOOP to iterate over distinct group_ids
     --FOR rec IN (SELECT DISTINCT group_id FROM xxcnv_gl_c008_gl_interface_stg where file_reference_identifier = gv_execution_id) LOOP
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_picked,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name,
                p_attribute1        => NULL,
                p_attribute2        => NULL,
                p_process_reference => NULL
            );
      -- END LOOP;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_gl_c008_gl_interface_stg: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                p_loading_status := gv_status_failure;
                RETURN;
        END;

    END import_data_from_oci_to_stg_prc;

/*=================================================================================================================
-- PROCEDURE : data_validations_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for validating the mandatory columns and business validations as per lean spec
===================================================================================================================*/
    PROCEDURE data_validations_prc IS

  -- Declaring Local Variables for validation.
        lv_total_debit      NUMBER;
        lv_total_credit     NUMBER;
        lv_row_count        NUMBER;
        lv_count            NUMBER;
        lv_error_count      NUMBER;
        ln_sum_entered_cr   NUMBER;
        ln_sum_entered_dr   NUMBER;
        ln_sum_accounted_cr NUMBER;
        ln_sum_accounted_dr NUMBER;
        lv_ledger_name      VARCHAR2(50);
        lv_ledger_currency  VARCHAR2(15);
        lv_ledger_1         VARCHAR2(15);
  /*CURSOR cur_amount_check IS
		SELECT ledger_name,period_name,accounting_date,reference1,
			reference2,reference4,reference5,segment1,
			sum(entered_cr)sum_entered_cr,sum(entered_dr) sum_entered_dr,sum(accounted_cr) sum_accounted_cr,sum(accounted_dr) sum_accounted_dr
		FROM xxcnv_gl_c008_gl_interface_stg
		WHERE execution_id = gv_execution_id
		GROUP BY ledger_name,period_name,accounting_date,reference1,reference2,reference4,reference5,segment1;*/

    BEGIN
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                execution_id = gv_execution_id
            WHERE
                file_reference_identifier IS NULL;

            dbms_output.put_line('Execution_id column is updated');
        END;

        BEGIN
            SELECT
                nvl(
                    count(*),
                    0
                )
            INTO gv_src_file_cnt
            FROM
                xxcnv_gl_c008_gl_interface_stg
            WHERE
                upper(ledger_name) = upper('NONE');

            SELECT DISTINCT
                nvl(ledger_name, 0)
            INTO lv_ledger_1
            FROM
                xxcnv_gl_c008_gl_interface_stg;

            IF
                gv_src_file_cnt = 0
                AND ( upper(lv_ledger_1) = upper('NONE') )
            THEN
                dbms_output.put_line('No Data is found in the xxcnv_gl_c008_gl_interface_stg Table');
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
            UPDATE xxcnv_gl_c008_gl_interface_stg
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

  -- Calculate the sum of Accounted Credit and Debit
        BEGIN
            SELECT
                nvl(
                    sum(accounted_cr),
                    0
                )
            INTO gv_src_act_cr
            FROM
                xxcnv_gl_c008_gl_interface_stg
            WHERE
                    execution_id = gv_execution_id
                AND upper(ledger_name) = upper('NONE');

            SELECT
                nvl(
                    sum(accounted_dr),
                    0
                )
            INTO gv_src_act_dr
            FROM
                xxcnv_gl_c008_gl_interface_stg
            WHERE
                    execution_id = gv_execution_id
                AND upper(ledger_name) = upper('NONE');

    -- Check if the sums are equal 
            IF ( abs(gv_src_act_dr - gv_src_act_cr) ) = 0 THEN
                dbms_output.put_line('The Sum of Accounted Debit is equal to Sum of Accounted Credit');
            ELSE
                dbms_output.put_line('The Sum of Accounted Debit is not equal to Sum of Accounted Credit' ||(gv_src_act_dr - gv_src_act_cr
                ));
                UPDATE xxcnv_gl_c008_gl_interface_stg
                SET
                    error_message = error_message
                                    || 'Accounted_CR and Accounted_DR should be equal'
                                    || ( gv_src_act_dr - gv_src_act_cr ),
                    import_status = 'ERROR'
                WHERE
                    file_reference_identifier IS NULL;
	  --and UPPER(ledger_name) = upper('NONE');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred while calculating sums: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);
        END;

   -- Calculate the sum of Entered Credit and Debit

        BEGIN
            lv_total_credit := 0;
            lv_total_debit := 0;
            SELECT
                nvl(
                    sum(entered_cr),
                    0
                )
            INTO lv_total_credit
            FROM
                xxcnv_gl_c008_gl_interface_stg
            WHERE
                execution_id = gv_execution_id;

            SELECT
                nvl(
                    sum(entered_dr),
                    0
                )
            INTO lv_total_debit
            FROM
                xxcnv_gl_c008_gl_interface_stg
            WHERE
                execution_id = gv_execution_id;

    -- Check if the sums are equal 
            IF ( abs(lv_total_debit - lv_total_credit) > 1 ) THEN
                dbms_output.put_line('The Sum of Entered Debit is not equal to Sum of Entered Credit' ||(lv_total_debit - lv_total_credit
                ));
                UPDATE xxcnv_gl_c008_gl_interface_stg
                SET
                    error_message = error_message
                                    || 'Entered_CR and Entered_DR should be equal'
                                    || ( lv_total_debit - lv_total_credit ),
                    import_status = 'ERROR'
                WHERE
                    file_reference_identifier IS NULL;
	  --and UPPER(ledger_name) = upper('NONE');

            ELSE
                dbms_output.put_line('The Sum of Entered Debit is equal to Sum of Entered Credit');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred while calculating sums: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = error_message || '|EFFECTIVE_DATE_OF_TRANSACTION should not be NULL'
            WHERE
                accounting_date IS NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('EFFECTIVE_DATE_OF_TRANSACTION is validated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = error_message || '|USER_JE_CATEGORY_NAME should not be NULL'
            WHERE
                user_je_category_name IS NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('USER_JE_CATEGORY_NAME is validated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = error_message || '|CURRENCY_CODE should not be NULL'
            WHERE
                currency_code IS NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('CURRENCY_CODE is validated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = error_message || '|DATE_CREATED should not be NULL'
            WHERE
                date_created IS NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('date_created is validated');
        END;

	/*--ENTERED_DR-- and --ENTERED_CR--
	BEGIN
    UPDATE xxcnv_gl_c008_gl_interface_stg 
    SET error_message = error_message || '|Atleast one value ENTERED_DR/ENTERED_CR should not be NULL'
    WHERE (entered_dr IS NULL AND entered_cr IS NULL)
    AND file_reference_identifier IS NULL;
    dbms_output.put_line('ENTERED_DR and ENTERED CR are validated');
	END;*/

 ---Accounting Dr
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = error_message || '|Atleast one value ACCOUNTED_DR/ACCOUNTED_CR should not be NULL'
            WHERE
                ( accounted_dr IS NULL
                  AND accounted_cr IS NULL )
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('ACCOUNTED_DR and ACCOUNTED_CR are validated');
        END;

---REFERENCE1 (Batch Name)
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = error_message || '|REFERENCE1(Batch Name) should not be NULL'
            WHERE
                reference1 IS NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('REFERENCE1(Batch Name) is validated');
        END;

---REFERENCE2 (Batch Description)
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = error_message || '|REFERENCE2(Batch Description) should not be NULL'
            WHERE
                reference2 IS NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('REFERENCE2(Batch Description) is validated');
        END;

 ---REFERENCE4 (Journal Entry Name)
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = error_message || '|REFERENCE4(Journal Entry Name) should not be NULL'
            WHERE
                reference4 IS NULL
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('REFERENCE4 (Journal Entry Name) is validated');
        END;

  ---REFERENCE5 (Journal Entry Description)
	/*BEGIN
    UPDATE xxcnv_gl_c008_gl_interface_stg 
    SET error_message = error_message || '|REFERENCE5(Journal Entry Description) should not be NULL'
    WHERE reference5 IS NULL
    AND file_reference_identifier IS NULL;
    dbms_output.put_line('REFERENCE5(Journal Entry Description) is validated');
	END;
*/
-- Group_Id --
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                group_id = (
                    SELECT
                        to_char(sysdate, 'YYYYMMDDHHMMSS')
                    FROM
                        dual
                )
            WHERE
                file_reference_identifier IS NULL;

            dbms_output.put_line('GROUP_ID column is updated');
        END;

  ---ATTRIBUTE1
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                attribute_category = 'NETSUITE CONVERSION'
            WHERE
                    1 = 1
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('attribute_category is updated');
        END;

  ---PERIOD_NAME
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                period_name = to_char(accounting_date, 'MON-YY')
            WHERE
                file_reference_identifier IS NULL
                AND accounting_date IS NOT NULL;

            dbms_output.put_line('PERIOD_NAME column is updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                source_system = gv_boundary_system;

            dbms_output.put_line('SOURCE_SYSTEM is updated');
        END;

    /* Ledger Name logic */
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg gis
            SET
                ledger_name = (
                    SELECT
                        CASE
                            WHEN gis.ledger_name = 'NONE'                      THEN
                                (
                                    SELECT
                                        glm.primary_ledger_name
                                    FROM
                                        xxcnv_gl_ledger_mapping glm
                                    WHERE
                                            glm.consolidated_exchange_rate = 'NONE'
                                        AND glm.subsidiary_internal_id = gis.segment1
                                )
                            WHEN upper(gis.ledger_name) = upper('PER-ACCOUNT') THEN
                                (
                                    SELECT
                                        glm.primary_ledger_name
                                    FROM
                                        xxcnv_gl_ledger_mapping glm
                                    WHERE
                                            upper(glm.consolidated_exchange_rate) = upper('Per-Account')
                                        AND glm.subsidiary_internal_id = gis.segment1
                                )
                        END
                    FROM
                        dual
                )
            WHERE
                execution_id = gv_execution_id;

            dbms_output.put_line('Ledger_name is updated');
        END;    

	--- ledger currency --
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg stg
            SET
                ledger_currency = (
                    SELECT
                        currency_code
                    FROM
                        xxcnv_gl_currency_mapping
                    WHERE
                        ledger_name = stg.ledger_name
                );

            dbms_output.put_line('ledger_currency is updated');
        END;

	-- Update amount fields 
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                entered_cr =
                    CASE
                        WHEN currency_code = ledger_currency THEN
                            accounted_cr
                        ELSE
                            entered_cr
                    END,
                entered_dr =
                    CASE
                        WHEN currency_code = ledger_currency THEN
                            accounted_dr
                        ELSE
                            entered_dr
                    END,
		    ---accounted_cr = CASE WHEN currency_code = ledger_currency THEN NULL ELSE accounted_cr END,
			---accounted_dr = CASE WHEN currency_code = ledger_currency THEN NULL ELSE accounted_dr END,
                currency_conversion_rate =
                    CASE
                        WHEN currency_code = ledger_currency THEN
                            NULL
                        ELSE
                            currency_conversion_rate
                    END,
                user_currency_conversion_type =
                    CASE
                        WHEN currency_code != ledger_currency THEN
                            'User'
                        ELSE
                            user_currency_conversion_type
                    END;

            dbms_output.put_line('Amount fields are updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                currency_conversion_rate =
                    CASE
                        WHEN currency_code != ledger_currency
                             AND currency_conversion_rate IS NULL
                             AND user_currency_conversion_type = 'User' THEN
                            1
                        ELSE
                            currency_conversion_rate
                    END;

            dbms_output.put_line('CURRENCY_CONVERSION_RATE are updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                entered_cr =
                    CASE
                        WHEN currency_code != ledger_currency
                             AND accounted_cr IS NOT NULL
                             AND entered_cr IS NULL
                             AND entered_dr IS NULL THEN
                            0
                        ELSE
                            entered_cr
                    END,
                entered_dr =
                    CASE
                        WHEN currency_code != ledger_currency
                             AND accounted_dr IS NOT NULL
                             AND entered_cr IS NULL
                             AND entered_dr IS NULL THEN
                            0
                        ELSE
                            entered_dr
                    END;
   --WHERE upper(USER_JE_CATEGORY_NAME) = UPPER('Currency Revaluation');

            dbms_output.put_line('USER_JE_CATEGORY_NAME fields are updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                entered_dr =
                    CASE
                        WHEN currency_code != ledger_currency
                             AND entered_cr > 0
                             AND ( accounted_dr > 0
                                   OR accounted_dr IS NOT NULL ) THEN
                            - ( entered_cr )
                        ELSE
                            entered_dr
                    END,
                accounted_dr =
                    CASE
                        WHEN currency_code != ledger_currency
                             AND entered_dr > 0
                             AND accounted_cr > 0 THEN
                            ( accounted_cr )
                        ELSE
                            accounted_dr
                    END;

            dbms_output.put_line('ENTERED_DR AND ACCOUNTED_DR fields are updated when amounts fields are not coming properly');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                entered_cr =
                    CASE
                        WHEN currency_code != ledger_currency
                             AND entered_cr > 0
                             AND ( accounted_dr > 0
                                   OR accounted_dr IS NOT NULL ) THEN
                            NULL
                        ELSE
                            entered_cr
                    END,
                accounted_cr =
                    CASE
                        WHEN currency_code != ledger_currency
                             AND entered_dr > 0
                             AND accounted_cr > 0 THEN
                            NULL
                        ELSE
                            accounted_cr
                    END;

            dbms_output.put_line('ENTERED_DR AND ACCOUNTED_DR fields are updated when amounts fields are not coming properly');
        END;


	-- Amount check --
/*	BEGIN

	FOR c_id IN cur_amount_check 
		LOOP
			IF (ABS(c_id.sum_entered_cr - c_id.sum_entered_dr)) > 1
			THEN
				UPDATE xxcnv_gl_c008_gl_interface_stg
				SET error_message = error_message || '|Entered_CR and Entered_DR are not equal and amount difference:'||(c_id.sum_entered_cr - c_id.sum_entered_dr)
				WHERE ledger_name = c_id.ledger_name
				AND   period_name = c_id.period_name
				AND   accounting_date = c_id.accounting_date
				AND   reference1 = c_id.reference1
				AND   reference2 = c_id.reference2
				AND   reference4 = c_id.reference4
				AND   reference5 = c_id.reference5
				AND   segment1 = c_id.segment1
				AND   execution_id = gv_execution_id;
			END IF;

			IF (ABS(c_id.sum_accounted_cr - c_id.sum_accounted_dr))  > 1
			THEN
				UPDATE xxcnv_gl_c008_gl_interface_stg
				SET error_message = error_message || '|Accounted_CR and Accounted_DR are not equal and amount difference:'||(c_id.sum_accounted_cr - c_id.sum_accounted_dr)
				WHERE ledger_name = c_id.ledger_name
				AND   period_name = c_id.period_name
				AND   accounting_date = c_id.accounting_date
				AND   reference1 = c_id.reference1
				AND   reference2 = c_id.reference2
				AND   reference4 = c_id.reference4
				AND   reference5 = c_id.reference5
				AND   segment1 = c_id.segment1
				AND   execution_id = gv_execution_id;
			END IF;

		END LOOP;

    EXCEPTION
    WHEN OTHERS THEN 
      dbms_output.put_line('An error occurred while validating amount fields: '|| '->'|| SUBSTR(SQLERRM, 1, 3000) || '->'|| DBMS_UTILITY.format_error_backtrace);
    END;
*/

      -- Updating constant values --

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                status = 'NEW',
                user_je_source_name = 'NETSUITE CONVERSION',
                actual_flag = 'A',
                reference7 = 'N';

            dbms_output.put_line('Constant fields are updated');
        END;

-- IF One or More Lines Errored Out. So marking all lines as error
/*
	BEGIN
		-- Check if there are any rows with a non-null error_message
		SELECT COUNT(*)
		INTO lv_count
		FROM xxcnv_gl_c008_gl_interface_stg
		WHERE error_message IS NOT NULL
		AND execution_id = gv_execution_id;

		-- If such rows exist, update import_status and error_message for all rows
		IF lv_count > 0 THEN
			UPDATE xxcnv_gl_c008_gl_interface_stg
			SET 
				import_status = 'ERROR',
				error_message = COALESCE(error_message, '') || '|One or more lines errored out. So marking all lines as error'
			WHERE execution_id = gv_execution_id;
		END IF;

		dbms_output.put_line('import_status and error_message columns are updated');
	END;
*/


        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                reference5 = replace(reference5, '"', ''),
                reference10 = replace(reference10, '"', ''),
                attribute2 = replace(attribute2, '"', ''),
                attribute7 = replace(attribute7, '"', ''),
				attribute8 = replace(attribute8, '"', ''); -- Added as per v1.3

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                attribute7 = substr(attribute7, 1, 148),
                attribute8 = substr(attribute8, 1, 148),
                reference10 = substr(reference10, 1, 236)
            WHERE
                file_reference_identifier IS NULL;

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
    --error_message = error_message || '|Reference5 length exceeded',
                reference5 = substr(
                    regexp_replace(reference5, '[^a-zA-Z0-9]', ''),
                    1,
                    238
                )
            WHERE
                    lengthb(reference5) > 240
                AND file_reference_identifier IS NULL
                AND execution_id = gv_execution_id;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error validation of Reference5 length');
        END;
/*
Begin
   update xxcnv_gl_c008_gl_interface_stg
   set reference10 = substr(reference10,1,238)
   where file_reference_identifier IS NULL
    AND execution_id = gv_execution_id;

    dbms_output.put_line('Error validation of Reference10 length');
end;
*/

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
   -- error_message = error_message || '|Reference10 length exceeded',
                reference10 = substr(
                    regexp_replace(reference10, '[^a-zA-Z0-9]', ''),
                    1,
                    238
                )
            WHERE
                    lengthb(reference10) > 240
                AND file_reference_identifier IS NULL
                AND execution_id = gv_execution_id;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error validation of Reference10 length');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                reference4 = '"'
                             || reference4
                             || '"'
            WHERE
                reference4 LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Journal Entry description is updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                reference5 = '"'
                             || reference5
                             || '"'
            WHERE
                reference5 LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Journal Entry description is updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                reference10 = '"'
                              || reference10
                              || '"'
            WHERE
                reference10 LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Journal Entry Line description is updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                attribute2 = '"'
                             || attribute2
                             || '"'
            WHERE
                attribute2 LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Attribute2 is updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                attribute7 = '"'
                             || attribute7
                             || '"'
            WHERE
                attribute7 LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Attribute7 is updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                attribute10 = '"'
                              || attribute10
                              || '"'
            WHERE
                attribute10 LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('Attribute10 is updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                reference2 = '"'
                             || reference2
                             || '"'
            WHERE
                reference2 LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('reference2 is updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                attribute8 = '"'
                             || attribute8
                             || '"'
            WHERE
                attribute8 LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
                AND file_reference_identifier IS NULL;

            dbms_output.put_line('attribute8 is updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = error_message || 'Some journal line failed validation so erroring out all the journal lines'
            WHERE
                error_message IS NULL
                AND reference4 IN (
                    SELECT DISTINCT
                        reference4
                    FROM
                        xxcnv_gl_c008_gl_interface_stg
                    WHERE
                        error_message IS NOT NULL
                        AND file_reference_identifier IS NULL
                )
                AND file_reference_identifier IS NULL;

        END;

  -- Update import_status based on error_message
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                import_status =
                    CASE
                        WHEN error_message IS NOT NULL THEN
                            'ERROR'
                        ELSE
                            'PROCESSED'
                    END;

            dbms_output.put_line('import_status is validated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                file_name = gv_oci_file_name
            WHERE
                file_reference_identifier IS NULL;

            dbms_output.put_line('file_name column is updated');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                file_name = gv_oci_file_name
            WHERE
                file_reference_identifier IS NULL;

            dbms_output.put_line('file_name column is updated');
        END;

  -- Check if there are any error messages
        SELECT
            COUNT(*)
        INTO lv_error_count
        FROM
            xxcnv_gl_c008_gl_interface_stg
        WHERE
            error_message IS NOT NULL
            AND file_reference_identifier IS NULL;

        UPDATE xxcnv_gl_c008_gl_interface_stg
        SET
            file_reference_identifier = gv_execution_id
                                        || '_'
                                        || gv_status_failure
        WHERE
            file_reference_identifier IS NULL
            AND error_message IS NOT NULL;

        dbms_output.put_line('File_reference_identifier column is updated');
        UPDATE xxcnv_gl_c008_gl_interface_stg
        SET
            file_reference_identifier = gv_execution_id
                                        || '_'
                                        || gv_status_success
        WHERE
            file_reference_identifier IS NULL
            AND error_message IS NULL;

        dbms_output.put_line('File_reference_identifier column is updated');
        IF lv_error_count > 0 THEN

    -- Logging the message
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_failed,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name,
                p_attribute1        => NULL,
                p_attribute2        => gv_data_validated_failure,
                p_process_reference => NULL
            );
        END IF;

  -- Logging the message
        IF lv_error_count = 0 THEN
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_validated,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name,
                p_attribute1        => NULL,
                p_attribute2        => gv_data_validated_success,
                p_process_reference => NULL
            );
        END IF;

        COMMIT;
    END data_validations_prc;

/*==============================================================================================================================
-- PROCEDURE : coa_target_segments_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used .
================================================================================================================================= */
    PROCEDURE coa_target_segments_prc IS

        lv_status           VARCHAR2(50);
        lv_message          VARCHAR2(500);
        lv_target_segment   VARCHAR2(200);
        lv_error_message    VARCHAR2(500);
        lv_target_segment1  VARCHAR2(25);
        lv_target_segment2  VARCHAR2(25);
        lv_target_segment3  VARCHAR2(25);
        lv_target_segment4  VARCHAR2(25);
        lv_target_segment5  VARCHAR2(50);
        lv_target_segment6  VARCHAR2(25);
        lv_target_segment7  VARCHAR2(25);
        lv_target_segment8  VARCHAR2(25);
        lv_target_segment9  VARCHAR2(25);
        lv_target_segment10 VARCHAR2(25);
        lv_pkg_name         VARCHAR2(10) := 'GL';
        CURSOR gl_line_cur IS
        SELECT
            ROWID AS identifier,
            x.*
        FROM
            xxcnv_gl_c008_gl_interface_stg x
        WHERE
            execution_id = gv_execution_id;

        TYPE gl_tab_type IS
            TABLE OF gl_line_cur%rowtype;
        gl_tab              gl_tab_type;
        v_limit             CONSTANT PLS_INTEGER := 2000;
    BEGIN
        OPEN gl_line_cur;
        LOOP
            FETCH gl_line_cur
            BULK COLLECT INTO gl_tab LIMIT v_limit;
            EXIT WHEN gl_tab.count = 0;
            FOR i IN 1..gl_tab.count LOOP
                BEGIN
            -- Call the COA_TRANSFORMATION_PKG for each row
                    xxcnv.xxcnv_gl_coa_transformation_pkg.coa_segment_mapping_prc(
                        p_in_segment1       => gl_tab(i).segment1,
                        p_in_segment2       => gl_tab(i).segment2,
                        p_in_segment3       => gl_tab(i).segment3,
                        p_in_segment4       => gl_tab(i).segment4,
                        p_in_segment5       => gl_tab(i).segment5,
                        p_in_segment6       => replace(gl_tab(i).segment6,
                                                 '|',
                                                 ''),   -- update this once we have a clear alignment from functional
                        p_in_segment7       => gl_tab(i).segment7,
                        p_in_segment8       => gl_tab(i).segment8,
                        p_in_segment9       => gl_tab(i).segment9,
                        p_in_segment10      => gl_tab(i).segment10,
                        p_out_target_system => lv_target_segment,
                        p_out_status        => lv_status,
                        p_out_message       => lv_message,
                        p_in_pkg_name       => lv_pkg_name
                    );

                    IF lv_status = 'SUCCESS' THEN

              --  DBMS_OUTPUT.PUT_LINE('Mapping Target Segments: '|| lv_target_segment);

                        SELECT
                            substr(lv_target_segment,
                                   1,
                                   instr(lv_target_segment, '|', 1, 1) - 1)                                       target_segment1,
                            substr(lv_target_segment,
                                   instr(lv_target_segment, '|', 1, 1) + 1,
                                   instr(lv_target_segment, '|', 1, 2) - instr(lv_target_segment, '|', 1, 1) - 1) target_segment2,
                            substr(lv_target_segment,
                                   instr(lv_target_segment, '|', 1, 2) + 1,
                                   instr(lv_target_segment, '|', 1, 3) - instr(lv_target_segment, '|', 1, 2) - 1) target_segment3,
                            substr(lv_target_segment,
                                   instr(lv_target_segment, '|', 1, 3) + 1,
                                   instr(lv_target_segment, '|', 1, 4) - instr(lv_target_segment, '|', 1, 3) - 1) target_segment4,
                            substr(lv_target_segment,
                                   instr(lv_target_segment, '|', 1, 4) + 1,
                                   instr(lv_target_segment, '|', 1, 5) - instr(lv_target_segment, '|', 1, 4) - 1) target_segment5,
                            substr(lv_target_segment,
                                   instr(lv_target_segment, '|', 1, 5) + 1,
                                   instr(lv_target_segment, '|', 1, 6) - instr(lv_target_segment, '|', 1, 5) - 1) target_segment6,
                            substr(lv_target_segment,
                                   instr(lv_target_segment, '|', 1, 6) + 1,
                                   instr(lv_target_segment, '|', 1, 7) - instr(lv_target_segment, '|', 1, 6) - 1) target_segment7,
                            substr(lv_target_segment,
                                   instr(lv_target_segment, '|', 1, 7) + 1,
                                   instr(lv_target_segment, '|', 1, 8) - instr(lv_target_segment, '|', 1, 7) - 1) target_segment8,
                            substr(lv_target_segment,
                                   instr(lv_target_segment, '|', 1, 8) + 1,
                                   instr(lv_target_segment, '|', 1, 9) - instr(lv_target_segment, '|', 1, 8) - 1) target_segment9,
                            substr(lv_target_segment,
                                   instr(lv_target_segment, '|', 1, 9) + 1)                                       target_segment10
                        INTO
                            lv_target_segment1,
                            lv_target_segment2,
                            lv_target_segment3,
                            lv_target_segment4,
                            lv_target_segment5,
                            lv_target_segment6,
                            lv_target_segment7,
                            lv_target_segment8,
                            lv_target_segment9,
                            lv_target_segment10
                        FROM
                            dual;

                        UPDATE xxcnv_gl_c008_gl_interface_stg
                        SET
                            target_segment1 = lv_target_segment1,
                            target_segment2 = lv_target_segment2,
                            target_segment3 = lv_target_segment3,
                            target_segment4 = lv_target_segment4,
                            target_segment5 = lv_target_segment5,
                            target_segment6 = lv_target_segment6,
                            target_segment7 = lv_target_segment7,
                            target_segment8 = lv_target_segment8,
                            target_segment9 = lv_target_segment9,
                            target_segment10 = lv_target_segment10
                        WHERE
                            ROWID = gl_tab(i).identifier;


                --DBMS_OUTPUT.PUT_LINE('Successfully transformed segments for record group_id: '|| rec.group_id);


                    ELSE
                        dbms_output.put_line('Source segments are not valid values, so we cannot map the target segments');
				/*
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc
							(
							p_conversion_id 	=> gv_conversion_id,
							p_execution_id		=> gv_execution_id,
							p_execution_step 	=> gv_coa_transformation_failed,
							p_boundary_system 	=> gv_boundary_system,
							p_file_path 		=> gv_oci_file_path,
							p_file_name 		=> gv_file_name,
							P_attribute1        => gv_group_id,
							P_attribute2        => lv_message,
							p_process_reference => NULL
							);

				*/
				--RETURN;
                        UPDATE xxcnv_gl_c008_gl_interface_stg
                        SET
                            error_message = error_message || lv_message,
                            import_status = 'ERROR',
                            file_reference_identifier = gv_execution_id
                                                        || '_'
                                                        || gv_status_failure
                        WHERE
                            ROWID = gl_tab(i).identifier;

                        BEGIN
                            UPDATE xxcnv_gl_c008_gl_interface_stg
                            SET
                                import_status =
                                    CASE
                                        WHEN error_message IS NOT NULL THEN
                                            'ERROR'
                                        ELSE
                                            'PROCESSED'
                                    END
                            WHERE
                                ROWID = gl_tab(i).identifier;
				--dbms_output.put_line('import_status is validated');
                        END;

                    END IF;

                EXCEPTION
                    WHEN OTHERS THEN
                        lv_error_message := '->'
                                            || substr(sqlerrm, 1, 3000)
                                            || '->'
                                            || dbms_utility.format_error_backtrace;

                        dbms_output.put_line('Completed with error: ' || lv_error_message);
                        dbms_output.put_line('Error transforming segments for record group_id: '
                                             || gl_tab(i).group_id
                                             || '- '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);

                        RETURN;
                END;
            END LOOP;

        END LOOP;

        CLOSE gl_line_cur;
        COMMIT;
        dbms_output.put_line('Completed mapping target segments');
        UPDATE xxcnv_gl_c008_gl_interface_stg
        SET
            reference1 = reference1
                         || '_'
                         || target_segment1
        WHERE
            target_segment1 IS NOT NULL;

        dbms_output.put_line('Reference1 Mapping updated');

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                reference1 = '"'
                             || reference1
                             || '"'
            WHERE
                reference1 LIKE '%,%'
                AND execution_id = gv_execution_id;

            dbms_output.put_line('Reference1 is updated');
        END;

				--Attribute 11
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                attribute11 = segment1
                              || '-'
                              || segment2
                              || '-'
                              || segment3
                              || '-'
                              || segment4
                              || '-'
                              || segment5
                              || '-'
                              || replace(segment6, '|', '')
                              || '-'
                              || segment7
                              || '-'
                              || segment8
                              || '-'
                              || segment9
                              || '-'
                              || segment10
            WHERE
                    1 = 1
                AND execution_id = gv_execution_id;

            dbms_output.put_line('attribute11 is updated');

            BEGIN
                UPDATE xxcnv_gl_c008_gl_interface_stg
                SET
                    attribute11 = substr(attribute11, 1, 148)
                WHERE
                    execution_id = gv_execution_id;

                UPDATE xxcnv_gl_c008_gl_interface_stg
                SET
                    attribute11 = '"'
                                  || attribute11
                                  || '"'
                WHERE
                    attribute11 LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('attribute11 3rd time updated');
            END;

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = error_message || 'Some journal line failed validation so erroring out all the journal lines in COA prc'
                ,
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure,
                import_status = 'ERROR'
            WHERE
                error_message IS NULL
                AND reference4 IN (
                    SELECT DISTINCT
                        reference4
                    FROM
                        xxcnv_gl_c008_gl_interface_stg
                    WHERE
                        error_message IS NOT NULL
                        AND execution_id = gv_execution_id
                )
                AND execution_id = gv_execution_id;

        END;

        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
            p_conversion_id     => gv_conversion_id,
            p_execution_id      => gv_execution_id,
            p_execution_step    => gv_coa_transformation,
            p_boundary_system   => gv_boundary_system,
            p_file_path         => gv_oci_file_path,
            p_file_name         => gv_file_name,
            p_attribute1        => gv_group_id,
            p_attribute2        => NULL,
            p_process_reference => NULL
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('An unexpected error occurred in coa_target_segments_prc: '
                                 || '->'
                                 || substr(sqlerrm, 1, 3000)
                                 || '->'
                                 || dbms_utility.format_error_backtrace);

            RETURN;
    END coa_target_segments_prc;

/*==============================================================================================================================
-- PROCEDURE : cvr_rule_check_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used to Check Cross Validation Rules
================================================================================================================================= */

    PROCEDURE cvr_rule_check_prc IS
    BEGIN

--GL-1--
	/* Start added for v1.2 */
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-1.1 Account range 100000-131004 can only use 99999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 100000 AND 131004
                AND target_segment3 <> 99999;

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-1.2 Account range 131006-151000 can only use 99999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 131006 AND 151000
                AND target_segment3 <> 99999;

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-1.3 Account range 151004-151007 can only use 99999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 151004 AND 151007
                AND target_segment3 <> 99999;

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-1.4 Account range 151010-151100 can only use 99999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 151010 AND 151100
                AND target_segment3 <> 99999;

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-1.4.3 Account range 151111-151302 can only use 99999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 151111 AND 151302
                AND target_segment3 <> 99999;

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-1.4.4 Account range 151304-151307 can only use 99999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 151304 AND 151307
                AND target_segment3 <> 99999;

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-1.4.5 Account range 151310-196200 can only use 99999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 151310 AND 196200
                AND target_segment3 <> 99999;

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-1.5 Account range 196202-196210 can only use 99999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 196202 AND 196210
                AND target_segment3 <> 99999;

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-1.5.1 Account range 196212-196301 can only use 99999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 196212 AND 196301
                AND target_segment3 <> 99999;

        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-1.5.2 Account range 196303-399999 can only use 99999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 196303 AND 399999
                AND target_segment3 <> 99999;

        END;

	--GL-4--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-4 Account range 411001-767003 can not use default Cost Center value',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 411001 AND 767003
                AND target_segment3 = 99999;

        END;

	/* end added for v1.2 */

	--GL-10--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-10 Account range 700000-799999 can only use 99970 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 700000 AND 799999
                AND target_segment3 <> 99970;

        END;

	--GL-11--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-11 Account range 800000-899999 can only use 99980 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 800000 AND 899999
                AND target_segment3 <> 99980;

        END;

	--GL-12--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-12 Account range 621201-621299 can only use Cost Centers 20101-39999 is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 621201 AND 621299
                AND ( target_segment3 < 20101
                      OR target_segment3 > 39999 );

        END;

	--GL-13--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-13 Account range 621301-621399 can only use Cost Centers 20101-39999 is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 621301 AND 621399
                AND ( target_segment3 < 20101
                      OR target_segment3 > 39999 );

        END;

	--GL-14--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-14 Account range 621001-621014 can only use 30202 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 621001 AND 621014
                AND target_segment3 <> 30202;

        END;

	--GL-2--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-2 Account range 400000-499999 can only use 99990 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 400000 AND 499999
                AND target_segment3 <> 99990;

        END;

	--GL-3--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-3 Account range 500000-599999 can only use Cost Centers 10001-19999 is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 BETWEEN 500000 AND 599999
                AND ( target_segment3 < 10001
                      OR target_segment3 > 19999 );

        END;

	--GL-6--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-6 Acount 632003 can only use 19999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 = 632003
                AND target_segment3 <> 19999;

        END;

	--GL-7--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-7 Acount 632001 can only use 29999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 = 632001
                AND target_segment3 <> 29999;

        END;

	--GL-8--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-8 Acount 632002 can only use 29999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 = 632002
                AND target_segment3 <> 29999;

        END;

	--GL-9--
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = 'CVR Rule GL-9 Acount 629001 can only use 49999 for Cost Center is violated',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND target_segment4 = 629001
                AND target_segment3 <> 49999;

        END;

        BEGIN
            INSERT INTO xxcnv_gl_cvr_violation_tbl   --v1.1 Updated the CVR table insert script
             (
                segment3,
                segment4,
                segment5,
                target_segment3,
                target_segment4,
                target_segment5,
                error_message,
                segment1,
                period_name,
                file_name,
                error_date
            )
                SELECT DISTINCT
                    segment3,
                    segment4,
                    segment5,
                    target_segment3,
                    target_segment4,
                    target_segment5,
                    error_message,
                    segment1,
                    period_name,
                    file_name,
                    sysdate
                FROM
                    xxcnv_gl_c008_gl_interface_stg
                WHERE
                    error_message LIKE '%CVR%'
                    AND target_segment1 IS NOT NULL;

            COMMIT;
        END;

        BEGIN
            SELECT
                COUNT(*)
            INTO gv_trs_file_cnt
            FROM
                xxcnv_gl_c008_gl_interface_stg
            WHERE
                upper(ledger_name) NOT LIKE upper('%Reporting%');

            IF gv_src_file_cnt <> gv_trs_file_cnt THEN
                UPDATE xxcnv_gl_c008_gl_interface_stg
                SET
                    error_message = error_message || '|Source file count is not matching with the Transformed file count',
                    import_status = 'ERROR',
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure;

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
            SELECT
                nvl(
                    sum(accounted_cr),
                    0
                )
            INTO gv_trs_act_cr
            FROM
                xxcnv_gl_c008_gl_interface_stg
            WHERE
                    execution_id = gv_execution_id
                AND upper(ledger_name) NOT LIKE upper('%Reporting%');

            SELECT
                nvl(
                    sum(accounted_dr),
                    0
                )
            INTO gv_trs_act_dr
            FROM
                xxcnv_gl_c008_gl_interface_stg
            WHERE
                    execution_id = gv_execution_id
                AND upper(ledger_name) NOT LIKE upper('%Reporting%');

    -- Check if the sums are equal 
            IF ( gv_src_act_dr - gv_trs_act_dr ) = 0 THEN
                dbms_output.put_line('Source accounted debit amount is equal to Transformed accounted debit amount');
            ELSE
                dbms_output.put_line('Source accounted debit amount is not equal to Transformed accounted debit amount' ||(gv_src_act_dr - gv_trs_act_dr
                ));
                UPDATE xxcnv_gl_c008_gl_interface_stg
                SET
                    error_message = error_message
                                    || '|Source accounted debit amount is not equal to Transformed accounted debit amount'
                                    || ( gv_src_act_dr - gv_trs_act_dr ),
                    import_status = 'ERROR',
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure;
	  --and UPPER(ledger_name) = upper('NONE');
            END IF;

    -- Check if the sums are equal 
            IF ( gv_src_act_cr - gv_trs_act_cr ) = 0 THEN
                dbms_output.put_line('Source accounted credit amount is equal to Transformed accounted credit amount');
            ELSE
                dbms_output.put_line('Source accounted credit amount is not equal to Transformed accounted credit amount' ||(gv_src_act_cr - gv_trs_act_cr
                ));
                UPDATE xxcnv_gl_c008_gl_interface_stg
                SET
                    error_message = error_message
                                    || '|Source accounted credit amount is not equal to Transformed accounted credit amount'
                                    || ( gv_src_act_cr - gv_trs_act_cr ),
                    import_status = 'ERROR',
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure;

            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred while calculating sums: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                error_message = error_message || '|Some journal line failed validation so erroring out all the journal lines in CVR prc'
                ,
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure,
                import_status = 'ERROR'
            WHERE
                error_message IS NULL
                AND reference4 IN (
                    SELECT DISTINCT
                        reference4
                    FROM
                        xxcnv_gl_c008_gl_interface_stg
                    WHERE
                        error_message IS NOT NULL
                        AND execution_id = gv_execution_id
                )
                AND execution_id = gv_execution_id;

        END;

        COMMIT;
    END cvr_rule_check_prc;

/*==============================================================================================================================
-- PROCEDURE : load_balancing_line_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the balancing line when accounted amounts are not matching
================================================================================================================================= */

    PROCEDURE load_balancing_line_prc IS

        CURSOR line_cursor IS
        SELECT
            status,
            accounting_date,
            user_je_source_name,
            user_je_category_name,
            currency_code,
            date_created,
            actual_flag,
            target_segment1,
            reference1,
            reference2,
            reference4,
            reference5,
            reference7,
            group_id,
            attribute_category,
            ledger_name,
            period_name,
            user_currency_conversion_type,
            ledger_currency,
            execution_id,
            import_status,
            file_reference_identifier,
            nvl(
                sum(accounted_dr),
                0
            ) AS sum_accounted_dr,
            nvl(
                sum(accounted_cr),
                0
            ) AS sum_accounted_cr
        FROM
            xxcnv_gl_c008_gl_interface_stg
        WHERE
            ledger_name LIKE '%Reporting%'
            AND import_status = 'PROCESSED'
        GROUP BY
            status,
            accounting_date,
            user_je_source_name,
            user_je_category_name,
            currency_code,
            date_created,
            actual_flag,
            target_segment1,
            reference1,
            reference2,
            reference4,
            reference5,
            reference7,
            group_id,
            attribute_category,
            ledger_name,
            period_name,
            user_currency_conversion_type,
            ledger_currency,
            execution_id,
            import_status,
            file_reference_identifier;

        v_reference4        xxcnv_gl_c008_gl_interface_stg.reference4%TYPE;
        v_diff_accounted_cr xxcnv_gl_c008_gl_interface_stg.accounted_cr%TYPE;
        v_diff_accounted_dr xxcnv_gl_c008_gl_interface_stg.accounted_dr%TYPE;
        lv_diff_amt         NUMBER;
        lv_balance_field    VARCHAR2(100) := 'Oracle Conversion – CTA Balancing Line';
        lv_segment1         VARCHAR2(25);
        lv_ns_account       VARCHAR2(25) := 'CTA';
        lv_oc_account       VARCHAR2(25);
        CURSOR cur_amount_check IS
        SELECT
            ledger_name,
            period_name,
            accounting_date,
            reference2,
            reference4,
            reference5,
            segment1,
            nvl(
                sum(entered_cr),
                0
            ) sum_entered_cr,
            nvl(
                sum(entered_dr),
                0
            ) sum_entered_dr,
            nvl(
                sum(accounted_cr),
                0
            ) sum_accounted_cr,
            nvl(
                sum(accounted_dr),
                0
            ) sum_accounted_dr
        FROM
            xxcnv_gl_c008_gl_interface_stg
        WHERE
            execution_id = gv_execution_id
        GROUP BY
            ledger_name,
            period_name,
            accounting_date,
            reference2,
            reference4,
            reference5,
            segment1;

        CURSOR cur_accounted_amount_check IS
        SELECT
            ledger_name,
            period_name,
            accounting_date,
            reference2,
            reference4,
            reference5,
            segment1,
            nvl(
                sum(entered_cr),
                0
            ) sum_entered_cr,
            nvl(
                sum(entered_dr),
                0
            ) sum_entered_dr,
            nvl(
                sum(accounted_cr),
                0
            ) sum_accounted_cr,
            nvl(
                sum(accounted_dr),
                0
            ) sum_accounted_dr
        FROM
            xxcnv_gl_c008_gl_interface_stg
        WHERE
            execution_id = gv_execution_id
        GROUP BY
            ledger_name,
            period_name,
            accounting_date,
            reference2,
            reference4,
            reference5,
            segment1;

    BEGIN
        dbms_output.put_line('Entered into load_balancing_line_prc procedure');
        BEGIN
            SELECT
                erp_coa_value
            INTO lv_oc_account
            FROM
                xxmap.xxmap_gl_e001_kaseya_ns_account
            WHERE
                    ns_account_attribute_1 = lv_ns_account
                AND last_update_date = (
                    SELECT
                        MAX(last_update_date)
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_account
                    WHERE
                        ns_account_attribute_1 = lv_ns_account
                );

        END;

        FOR g_ln IN line_cursor LOOP
            lv_diff_amt := 0;
            IF g_ln.sum_accounted_cr <> g_ln.sum_accounted_dr THEN
                lv_diff_amt := abs(g_ln.sum_accounted_cr - g_ln.sum_accounted_dr);
                IF ( g_ln.sum_accounted_cr > g_ln.sum_accounted_dr ) THEN
                    INSERT INTO xxcnv_gl_c008_gl_interface_stg (
                        status,
                        accounting_date,
                        user_je_source_name,
                        user_je_category_name,
                        currency_code,
                        date_created,
                        actual_flag,
                        target_segment1,
                        target_segment2,
                        target_segment3,
                        target_segment4,
                        target_segment5,
                        target_segment6,
                        target_segment7,
                        target_segment8,
                        target_segment9,
                        target_segment10,
                        entered_dr,
                        entered_cr,
                        accounted_dr,
                        accounted_cr,
                        reference1,
                        reference2,
                        reference4,
                        reference5,
                        reference7,
                        reference10,
                        group_id,
                        attribute_category,
                        ledger_name,
                        period_name,
                        user_currency_conversion_type,
                        ledger_currency,
                        currency_conversion_rate,
                        execution_id,
                        import_status,
                        file_reference_identifier,
                        error_message
                    ) VALUES ( g_ln.status,
                               g_ln.accounting_date,
                               g_ln.user_je_source_name,
                               g_ln.user_je_category_name,
                               g_ln.currency_code,
                               g_ln.date_created,
                               g_ln.actual_flag,
                               g_ln.target_segment1,
                               '999',
                               '99999',
                               lv_oc_account,
                               '9999',
                               '999999',
                               '9999',
                               '9999',
                               '9999',
                               '999999',
                               NULL,
                               NULL,
                               lv_diff_amt,
                               NULL,
                               g_ln.reference1,
                               g_ln.reference2,
                               g_ln.reference4,
                               g_ln.reference5,
                               g_ln.reference7,
                               lv_balance_field,
                               g_ln.group_id,
                               g_ln.attribute_category,
                               g_ln.ledger_name,
                               g_ln.period_name,
                               g_ln.user_currency_conversion_type,
                               g_ln.ledger_currency,
                               1,
                               g_ln.execution_id,
                               g_ln.import_status,
                               g_ln.file_reference_identifier,
                               NULL );

                    dbms_output.put_line('Inserted debit balancing line for journal: ' || g_ln.reference4);
                    COMMIT;
                ELSE
                    INSERT INTO xxcnv_gl_c008_gl_interface_stg (
                        status,
                        accounting_date,
                        user_je_source_name,
                        user_je_category_name,
                        currency_code,
                        date_created,
                        actual_flag,
                        target_segment1,
                        target_segment2,
                        target_segment3,
                        target_segment4,
                        target_segment5,
                        target_segment6,
                        target_segment7,
                        target_segment8,
                        target_segment9,
                        target_segment10,
                        entered_dr,
                        entered_cr,
                        accounted_dr,
                        accounted_cr,
                        reference1,
                        reference2,
                        reference4,
                        reference5,
                        reference7,
                        reference10,
                        group_id,
                        attribute_category,
                        ledger_name,
                        period_name,
                        user_currency_conversion_type,
                        ledger_currency,
                        currency_conversion_rate,
                        execution_id,
                        import_status,
                        file_reference_identifier,
                        error_message
                    ) VALUES ( g_ln.status,
                               g_ln.accounting_date,
                               g_ln.user_je_source_name,
                               g_ln.user_je_category_name,
                               g_ln.currency_code,
                               g_ln.date_created,
                               g_ln.actual_flag,
                               g_ln.target_segment1,
                               '999',
                               '99999',
                               lv_oc_account,
                               '9999',
                               '999999',
                               '9999',
                               '9999',
                               '9999',
                               '999999',
                               NULL,
                               NULL,
                               NULL,
                               lv_diff_amt,
                               g_ln.reference1,
                               g_ln.reference2,
                               g_ln.reference4,
                               g_ln.reference5,
                               g_ln.reference7,
                               lv_balance_field,
                               g_ln.group_id,
                               g_ln.attribute_category,
                               g_ln.ledger_name,
                               g_ln.period_name,
                               g_ln.user_currency_conversion_type,
                               g_ln.ledger_currency,
                               1,
                               g_ln.execution_id,
                               g_ln.import_status,
                               g_ln.file_reference_identifier,
                               NULL );

                    dbms_output.put_line('Inserted credit balancing line for journal: ' || g_ln.reference4);
                END IF;

            END IF;

        END LOOP;

        COMMIT;
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                entered_cr =
                    CASE
                        WHEN currency_code != ledger_currency
                             AND accounted_cr IS NOT NULL
                             AND entered_cr IS NULL
                             AND entered_dr IS NULL THEN
                            0
                        ELSE
                            entered_cr
                    END,
                entered_dr =
                    CASE
                        WHEN currency_code != ledger_currency
                             AND accounted_dr IS NOT NULL
                             AND entered_cr IS NULL
                             AND entered_dr IS NULL THEN
                            0
                        ELSE
                            entered_dr
                    END
            WHERE
                execution_id = gv_execution_id;

            dbms_output.put_line('Amounts are shifted');
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                entered_cr =
                    CASE
                        WHEN currency_code = ledger_currency
                             AND accounted_cr IS NOT NULL
                             AND entered_cr IS NULL
                             AND entered_dr IS NULL THEN
                            accounted_cr
                        ELSE
                            entered_cr
                    END,
                entered_dr =
                    CASE
                        WHEN currency_code = ledger_currency
                             AND accounted_dr IS NOT NULL
                             AND entered_cr IS NULL
                             AND entered_dr IS NULL THEN
                            accounted_dr
                        ELSE
                            entered_dr
                    END
            WHERE
                execution_id = gv_execution_id;

            dbms_output.put_line('Amounts are shifted');
        END;

	-- Amount check --
        BEGIN
            FOR c_id IN cur_amount_check LOOP
                IF ( abs(c_id.sum_entered_cr - c_id.sum_entered_dr) ) > 1 THEN
                    UPDATE xxcnv_gl_c008_gl_interface_stg
                    SET
                        error_message = error_message
                                        || '|Entered_CR and Entered_DR are not equal and amount difference after transformation:'
                                        || ( c_id.sum_entered_cr - c_id.sum_entered_dr ),
                        import_status = 'ERROR',
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                            ledger_name = c_id.ledger_name
                        AND period_name = c_id.period_name
                        AND accounting_date = c_id.accounting_date
                        AND reference2 = c_id.reference2
                        AND reference4 = c_id.reference4
                        AND reference5 = c_id.reference5
                        AND segment1 = c_id.segment1
                        AND execution_id = gv_execution_id;

                END IF;

			/*IF (ABS(c_id.sum_accounted_cr - c_id.sum_accounted_dr))  > 1
			THEN
				UPDATE xxcnv_gl_c008_gl_interface_stg
				SET error_message = error_message || '|Accounted_CR and Accounted_DR are not equal and amount difference:'||(c_id.sum_accounted_cr - c_id.sum_accounted_dr)
				WHERE ledger_name = c_id.ledger_name
				AND   period_name = c_id.period_name
				AND   accounting_date = c_id.accounting_date
				AND   reference1 = c_id.reference1
				AND   reference2 = c_id.reference2
				AND   reference4 = c_id.reference4
				AND   reference5 = c_id.reference5
				AND   segment1 = c_id.segment1
				AND   execution_id = gv_execution_id;
			END IF;*/

            END LOOP;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred while validating amount fields: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);
        END;

--Check Accounted Amount

        BEGIN
            FOR c_acc_id IN cur_accounted_amount_check LOOP
                IF ( abs(c_acc_id.sum_accounted_dr - c_acc_id.sum_accounted_cr) ) > 0 THEN
                    UPDATE xxcnv_gl_c008_gl_interface_stg
                    SET
                        error_message = error_message
                                        || '|Accounted_CR and Accounted_DR are not equal and amount difference after tansformation:'
                                        || ( c_acc_id.sum_accounted_cr - c_acc_id.sum_accounted_dr ),
                        import_status = 'ERROR',
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                            ledger_name = c_acc_id.ledger_name
                        AND period_name = c_acc_id.period_name
                        AND accounting_date = c_acc_id.accounting_date
                        AND reference2 = c_acc_id.reference2
                        AND reference4 = c_acc_id.reference4
                        AND reference5 = c_acc_id.reference5
                        AND segment1 = c_acc_id.segment1
                        AND execution_id = gv_execution_id;

                END IF;
            END LOOP;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred while validating amount fields: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);
        END;

        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                group_id = group_id
                           || TO_NUMBER(to_char(accounting_date, 'YYMM'))
            WHERE
                execution_id = gv_execution_id;

            dbms_output.put_line('Updating group_id based on the month');
        END;

		/* Commented as per v1.3
        BEGIN
            UPDATE xxcnv_gl_c008_gl_interface_stg
            SET
                reference1 = '"'
                             || reference1
                             || '"'
            WHERE
                reference1 LIKE '%,%'
                AND execution_id = gv_execution_id;

            dbms_output.put_line('reference1 is updated');
        END;
		*/

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error while executing load_balancing_line_prc procedure :'
                                 || substr(sqlerrm, 1, 3000)
                                 || '->'
                                 || dbms_utility.format_error_backtrace);
    END load_balancing_line_prc;

/*==============================================================================================================================
-- PROCEDURE : create_fbdi_file_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the FBDI CSV file by using the data in the GL_inteface Table after all validations.
================================================================================================================================= */
    PROCEDURE create_fbdi_file_prc IS

        CURSOR group_id_cursor IS
        SELECT DISTINCT
            group_id
        FROM
            xxcnv_gl_c008_gl_interface_stg
        WHERE
            file_reference_identifier = gv_execution_id
                                        || '_'
                                        || gv_status_success;

        lv_error_count  NUMBER;
        lv_group_id     NUMBER;
        lv_period_name  VARCHAR2(20);
        lv_ledger       VARCHAR2(100);
        lv_subs_name    VARCHAR2(150);
        lv_file         VARCHAR2(2000);
        ln_category_cnt NUMBER;
        lv_file_type    VARCHAR2(20);
    BEGIN
        FOR g_id IN group_id_cursor LOOP
            lv_group_id := g_id.group_id;
            ln_category_cnt := 0;
            lv_error_count := 1;
            dbms_output.put_line('Processing group_id: ' || lv_group_id);
            BEGIN
            -- Count the success record count for the current GROUP_ID
                SELECT
                    COUNT(1)
                INTO lv_error_count
                FROM
                    xxcnv_gl_c008_gl_interface_stg
                WHERE
                        1 = 1
                    AND group_id = lv_group_id
                    AND error_message IS NOT NULL;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for group_id: ' || lv_group_id);
                    RETURN;
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking error record count for group_id '
                                         || lv_group_id
                                         || ': '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    RETURN;
            END;

            IF lv_error_count = 0 THEN
                BEGIN

			/* START added for v1.2*/

                    SELECT
                        initcap(period_name),
                        ledger_name,
						--TRIM(reference2) -- commented for v1.3
                        TRIM(replace(replace(replace(reference2,'"',''),',',''),' ','')) -- added for v1.3
						--target_segment1
                    INTO
                        lv_period_name,
                        lv_ledger,
                        lv_subs_name
                    FROM
                        xxcnv_gl_c008_gl_interface_stg
                    WHERE
                            1 = 1
                        AND group_id = lv_group_id
                        AND file_reference_identifier = gv_execution_id
                                                        || '_'
                                                        || gv_status_success
                    GROUP BY
                        period_name,
                        ledger_name,
                        reference2;

                    dbms_output.put_line('lv_period_name ' || lv_period_name);
                    SELECT
                        COUNT(1)
                    INTO ln_category_cnt
                    FROM
                        xxcnv_gl_c008_gl_interface_stg
                    WHERE
                            1 = 1
                        AND group_id = lv_group_id
                        AND upper(user_je_category_name) = upper('RevRec Journal')
                        AND file_reference_identifier = gv_execution_id
                                                        || '_'
                                                        || gv_status_success;

                    dbms_output.put_line('ln_category_cnt: ' || ln_category_cnt);
                    IF
                        lv_ledger LIKE '%USGAAP%'
                        AND ln_category_cnt = 0
                    THEN
                        lv_file_type := 'NN';
                    ELSIF
                        lv_ledger LIKE '%USGAAP%'
                        AND ln_category_cnt > 0
                    THEN
                        lv_file_type := 'RR-NN';
                    ELSIF
                        lv_ledger LIKE '%Reporting%'
                        AND ln_category_cnt > 0
                    THEN
                        lv_file_type := 'RR-PA';
                    ELSE
                        NULL;
                    END IF;

                    lv_file := 'GlInterface_'
                               || lv_ledger
                               || '_'
                               || lv_group_id
                               || '_'
                               || lv_period_name
                               || '_'
                               || lv_subs_name
                               || '_'
                               || lv_file_type;

                    dbms_output.put_line('lv_file: ' || lv_file);

		    /* END added for v1.2*/

                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                    --FILE_URI_LIST   => REPLACE(gv_oci_file_path, gv_source_folder, gv_transformed_folder) || '/'||lv_period_name||'_'||lv_group_id,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                         || '/'
                                         || lv_file,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
                                        STATUS,                        
                                        LEDGER_ID, 	
										TO_CHAR(ACCOUNTING_DATE, ''YYYY/MM/DD'') AS ACCOUNTING_DATE,
                                        USER_JE_SOURCE_NAME, 
                                        USER_JE_CATEGORY_NAME,
                                        CURRENCY_CODE,
										 TO_CHAR(DATE_CREATED, ''YYYY/MM/DD'') AS DATE_CREATED,
                                        ACTUAL_FLAG,
                                        target_segment1 AS segment1,                  
                                        target_segment2 AS segment2,	 
                                        target_segment3 AS segment3,	 
                                        target_segment4 AS segment4,	 
                                        target_segment5 AS segment5,	 
                                        target_segment6 AS segment6,	 
                                        target_segment7 AS segment7,	 
                                        target_segment8 AS segment8,	 
                                        target_segment9 AS segment9,	 
                                        target_segment10 AS segment10, 	 
                                        SEGMENT11, 	 
                                        SEGMENT12,	 
                                        SEGMENT13,	 
                                        SEGMENT14,	 
                                        SEGMENT15,	 
                                        SEGMENT16,	 
                                        SEGMENT17,	 
                                        SEGMENT18,	 
                                        SEGMENT19,	 
                                        SEGMENT20,	 
                                        SEGMENT21,	 
                                        SEGMENT22,	 
                                        SEGMENT23,	 
                                        SEGMENT24,	 
                                        SEGMENT25,	 
                                        SEGMENT26,	 
                                        SEGMENT27,	 
                                        SEGMENT28,	 
                                        SEGMENT29,	 
                                        SEGMENT30,	 
                                        ENTERED_DR, 	
                                        ENTERED_CR,		
                                        ACCOUNTED_DR,
                                        ACCOUNTED_CR,
                                        trim(REFERENCE1),
                                        trim(REFERENCE2), 
                                        REFERENCE3, 
                                        REFERENCE4, 
                                        REFERENCE5, 
                                        REFERENCE6, 
                                        REFERENCE7, 
                                        REFERENCE8, 
                                        REFERENCE9, 
                                        REFERENCE10,  
                                        REFERENCE21, 
                                        REFERENCE22, 
                                        REFERENCE23, 
                                        REFERENCE24, 
                                        REFERENCE25, 
                                        REFERENCE26, 
                                        REFERENCE27, 
                                        REFERENCE28, 
                                        REFERENCE29, 
                                        REFERENCE30, 
                                        STAT_AMOUNT, 
                                        USER_CURRENCY_CONVERSION_TYPE, 
                                        CURRENCY_CONVERSION_DATE,   
                                        CURRENCY_CONVERSION_RATE,  
                                        GROUP_ID,	
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
                                        ATTRIBUTE_CATEGORY3, 
                                        AVERAGE_JOURNAL_FLAG,										
                                        ORIGINATING_BAL_SEG_VALUE,
                                        LEDGER_NAME,
										ENCUMBRANCE_TYPE_ID,  
                                        JGZZ_RECON_REF,
                                        PERIOD_NAME, 
                                        reference18,
										REFERENCE19,
										REFERENCE20,
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
                                        GLOBAL_ATTRIBUTE_CATEGORY, 
                                        GLOBAL_ATTRIBUTE1, 
                                        GLOBAL_ATTRIBUTE2, 
                                        GLOBAL_ATTRIBUTE3, 
                                        GLOBAL_ATTRIBUTE4, 
                                        GLOBAL_ATTRIBUTE5, 
                                        GLOBAL_ATTRIBUTE6, 
                                        GLOBAL_ATTRIBUTE7, 
                                        GLOBAL_ATTRIBUTE8, 
                                        GLOBAL_ATTRIBUTE9, 
                                        GLOBAL_ATTRIBUTE10, 
                                        GLOBAL_ATTRIBUTE11, 
                                        GLOBAL_ATTRIBUTE12, 
                                        GLOBAL_ATTRIBUTE13, 
                                        GLOBAL_ATTRIBUTE14, 
                                        GLOBAL_ATTRIBUTE15, 
                                        GLOBAL_ATTRIBUTE16, 
                                        GLOBAL_ATTRIBUTE17, 
                                        GLOBAL_ATTRIBUTE18, 
                                        GLOBAL_ATTRIBUTE19, 
                                        GLOBAL_ATTRIBUTE20, 
                                        GLOBAL_ATTRIBUTE_DATE1,
                                        GLOBAL_ATTRIBUTE_DATE2,
                                        GLOBAL_ATTRIBUTE_DATE3,
                                        GLOBAL_ATTRIBUTE_DATE4,
                                        GLOBAL_ATTRIBUTE_DATE5,
                                        GLOBAL_ATTRIBUTE_NUMBER1,
                                        GLOBAL_ATTRIBUTE_NUMBER2,
                                        GLOBAL_ATTRIBUTE_NUMBER3,
                                        GLOBAL_ATTRIBUTE_NUMBER4,
                                        GLOBAL_ATTRIBUTE_NUMBER5 
                                    FROM xxcnv_gl_c008_gl_interface_stg
                                    WHERE import_status = '''
                                 || 'PROCESSED'
                                 || '''
									AND execution_id  = '''
                                 || gv_execution_id
                                 || '''
									AND group_id ='''
                                 || lv_group_id
                                 || '''
									AND file_reference_identifier= '''
                                 || gv_execution_id
                                 || '_'
                                 || gv_status_success
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for group_id '
                                         || lv_group_id
                                         || 'exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_fbdi_export_status,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name,
                        p_attribute1        => NULL,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for group_id '
                                             || lv_group_id
                                             || ': '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);

                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for group_id '
                                     || lv_group_id
                                     || ': Error message columns contain data');
                RETURN;
            END IF;

        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('An error occurred: '
                                 || '->'
                                 || substr(sqlerrm, 1, 3000)
                                 || '->'
                                 || dbms_utility.format_error_backtrace);

            RETURN;
    END create_fbdi_file_prc;

/*==============================================================================================================================
-- PROCEDURE : create_atp_validation_recon_report_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating properties file.
================================================================================================================================= */
    PROCEDURE create_atp_validation_recon_report_prc IS

        CURSOR group_id_cursor IS
        SELECT DISTINCT
            group_id
        FROM
            xxcnv_gl_c008_gl_interface_stg
        WHERE
            file_reference_identifier = gv_execution_id
                                        || '_'
                                        || gv_status_failure;

        lv_error_count NUMBER;
        lv_group_id    NUMBER := 0;
        lv_period_name VARCHAR2(20);
        lv_ledger      VARCHAR2(100);
        lv_subs_name   VARCHAR2(150);
        lv_file        VARCHAR2(2000);
    BEGIN
        FOR g_id IN group_id_cursor LOOP
            lv_group_id := g_id.group_id;
            dbms_output.put_line('Processing group_id: ' || lv_group_id);
            BEGIN
            -- Count the Error record count for the current GROUP_ID
			/*
            SELECT period_name
            INTO   lv_period_name
            FROM xxcnv_gl_c008_gl_interface_stg
            WHERE 1=1
			AND group_id = lv_group_id
            AND file_reference_identifier = gv_execution_id||'_'||gv_status_failure
			group by period_name;
			*/

                SELECT
                    initcap(period_name),
                    ledger_name,
                    TRIM(reference2),
                    COUNT(1)
                INTO
                    lv_period_name,
                    lv_ledger,
                    lv_subs_name,
                    lv_error_count
                FROM
                    xxcnv_gl_c008_gl_interface_stg
                WHERE
                        1 = 1
                    AND group_id = lv_group_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                GROUP BY
                    period_name,
                    ledger_name,
                    reference2;

            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for group_id: ' || lv_group_id);
                    RETURN;
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking record count for group_id '
                                         || lv_group_id
                                         || ': '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    RETURN;
            END;

            IF lv_group_id > 0 THEN
                BEGIN

		--lv_period_name:= 'GlInterface_'||lv_period_name;
                    lv_file := 'GlInterface_'
                               || lv_ledger
                               || '_'
                               || lv_group_id
                               || '_'
                               || lv_period_name
                               || '_'
                               || lv_subs_name;

                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                --FILE_URI_LIST   => REPLACE(gv_oci_file_path, gv_source_folder, gv_recon_folder) || '/'|| lv_group_id || lv_period_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                         || '/'
                                         || lv_file,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600',
                                    'header' VALUE TRUE
                                ),
                        query           => '   SELECT
										status as STATUS,                        
                                        ledger_id AS LEDGER_ID , 
                                        accounting_date AS ACCOUNTING_DATE,
                                        user_je_source_name AS SOURCE_NAME, 
                                        user_je_category_name AS CATEGORY_NAME,
                                        currency_code,
                                        date_created,
                                        actual_flag,
                                        CAST(segment1 AS VARCHAR2(100)) AS SOURCE_SEGMENT1,
										CAST(segment2 AS VARCHAR2(100)) AS SOURCE_SEGMENT2,
										CAST(segment3 AS VARCHAR2(100)) AS SOURCE_SEGMENT3,
										CAST(segment4 AS VARCHAR2(100)) AS SOURCE_SEGMENT4,
										CAST(segment5 AS VARCHAR2(100)) AS SOURCE_SEGMENT5,
										CAST(segment6 AS VARCHAR2(100)) AS SOURCE_SEGMENT6,
										CAST(segment7 AS VARCHAR2(100)) AS SOURCE_SEGMENT7,
										CAST(segment8 AS VARCHAR2(100)) AS SOURCE_SEGMENT8,
										CAST(segment9 AS VARCHAR2(100)) AS SOURCE_SEGMENT9,
										CAST(segment10 AS VARCHAR2(100)) AS SOURCE_SEGMENT10,
										to_number(entered_dr) as entered_dr, 
                                        to_number(entered_cr) as entered_cr ,
                                        to_number(accounted_dr) as accounted_dr,
                                        to_number(accounted_cr) as accounted_cr,
                                        reference1 as batch_name, 
                                        reference2 as batch_description, 
                                        reference3, 
                                        reference4 as journal_entry_name, 
                                        reference5 as journal_entry_description, 
                                        reference6 as journal_entry_reference, 
                                        reference7 as journal_entry_reversal_flag, 
                                        reference8 as journal_entry_reversal_period, 
                                        reference9 as journal_reversal_method, 
                                        reference10 as journal_entry_line_description,   
                                        group_id,
                                        ledger_name AS LEDGER_NAME,
                                        period_name AS PERIOD_NAME, 
                                        file_name AS FILE_NAME,
                                        error_message AS error_message,
                                        import_status AS IMPORT_STATUS,
                                        CAST(target_segment1 AS VARCHAR2(100)) AS SEGMENT1,
										CAST(target_segment2 AS VARCHAR2(100)) AS SEGMENT2,
										CAST(target_segment3 AS VARCHAR2(100)) AS SEGMENT3,
										CAST(target_segment4 AS VARCHAR2(100)) AS SEGMENT4,
										CAST(target_segment5 AS VARCHAR2(100)) AS SEGMENT5,
										CAST(target_segment6 AS VARCHAR2(100)) AS SEGMENT6,
										CAST(target_segment7 AS VARCHAR2(100)) AS SEGMENT7,
										CAST(target_segment8 AS VARCHAR2(100)) AS SEGMENT8,
										CAST(target_segment9 AS VARCHAR2(100)) AS SEGMENT9,
										CAST(target_segment10 AS VARCHAR2(100)) AS SEGMENT10,
										source_system
										FROM xxcnv_gl_c008_gl_interface_stg
										where execution_id  = '''
                                 || gv_execution_id
                                 || '''
                                        AND import_status = '''
                                 || 'ERROR'
                                 || '''
										AND group_id ='''
                                 || lv_group_id
                                 || '''
										AND file_reference_identifier= '''
                                 || gv_execution_id
                                 || '_'
                                 || gv_status_failure
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for group_id '
                                         || lv_group_id
                                         || 'exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_recon_report,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => lv_group_id
                                       || '_'
                                       || gv_oci_file_name,
                        p_attribute1        => lv_group_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for group_id '
                                             || lv_group_id
                                             || ': '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);

                        RETURN;
                END;
            END IF;

        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('An error occurred: '
                                 || '->'
                                 || substr(sqlerrm, 1, 3000)
                                 || '->'
                                 || dbms_utility.format_error_backtrace);
    END create_atp_validation_recon_report_prc;

/*==============================================================================================================================
-- PROCEDURE : create_properties_file_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating properties file.
================================================================================================================================= */
    PROCEDURE create_properties_file_prc IS
        CURSOR group_id_cursor IS
        SELECT DISTINCT
            group_id
        FROM
            xxcnv_gl_c008_gl_interface_stg
        WHERE
            execution_id = gv_execution_id;

        lv_error_count NUMBER;
        lv_group_id    NUMBER;
    BEGIN
        FOR g_id IN group_id_cursor LOOP
            lv_group_id := g_id.group_id;
            dbms_output.put_line('Processing group_id: ' || lv_group_id);
            BEGIN
            -- Count the number of rows with non-null, non-empty error_message for the current group_id
                SELECT
                    COUNT(error_message)
                INTO lv_error_count
                FROM
                    xxcnv_gl_c008_gl_interface_stg
                WHERE
                        group_id = lv_group_id
                    AND error_message IS NOT NULL
                    AND TRIM(error_message) != '';

                dbms_output.put_line('Error count for group_id '
                                     || lv_group_id
                                     || ': '
                                     || lv_error_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for group_id: ' || lv_group_id);
                    CONTINUE;
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking error_message column for group_id '
                                         || lv_group_id
                                         || ': '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    CONTINUE;
            END;

            IF lv_error_count = 0 THEN
                BEGIN
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                         || '/'
                                         || lv_group_id
                                         || gv_oci_file_name
                                         || '.properties',
                        format          =>
                                JSON_OBJECT(
                                    'trimspaces' VALUE 'rtrim'
                                ),
                        query           => 'SELECT ''oracle/apps/ess/financials/generalLedger/programs/common/,JournalImportLauncher,GlInterface,300000001891582,300000001892392,300000001891564,'
                                 || lv_group_id
                                 || ',N,N,Y''as column1 from dual'
                    );

                    dbms_output.put_line('Properties file for GROUP_ID '
                                         || lv_group_id
                                         || 'exported successfully to OCI Object Storage.');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_staged,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                        p_file_name         => lv_group_id
                                       || '_'
                                       || 'properties',
                        p_attribute1        => lv_group_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to properties for group_id '
                                             || lv_group_id
                                             || ': '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);

                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for group_id '
                                     || lv_group_id
                                     || ': Error message columns contain data.');
            END IF;

        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('An error occurred: '
                                 || '->'
                                 || substr(sqlerrm, 1, 3000)
                                 || '->'
                                 || dbms_utility.format_error_backtrace);
    END create_properties_file_prc;

END xxcnv_gl_c008_gl_journals_conversion_pkg;