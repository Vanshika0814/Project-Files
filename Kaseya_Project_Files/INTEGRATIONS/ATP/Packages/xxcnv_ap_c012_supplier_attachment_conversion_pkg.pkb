CREATE OR REPLACE PACKAGE BODY xxcnv.xxcnv_ap_c012_supplier_attachment_conversion_pkg IS
	/*************************************************************************************
    NAME              :     SUPPLIER_ATTACHMENT_CONVERSION_PKG BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------         ----------  -----------------------------------------
	-- Phanindra 	   03-Mar-2025  	  1.0         Initial Development
    -- Phanindra       26-Jul-2025        1.1          Removed commented code
	-- Phanindra       28-Aug-2025        1.2         Made Changes as per the Jira LTCI-8094
	****************************************************************************************/

---Declaring global Variables

    gv_import_status            VARCHAR2(256) := NULL;
    gv_error_message            VARCHAR2(500) := NULL;
    gv_file_name                VARCHAR2(256) := NULL;
    gv_oci_file_name            VARCHAR2(4000) := NULL;
    gv_oci_file_path            VARCHAR2(200) := NULL;
    gv_oci_file_name_bus_class  VARCHAR2(100) := NULL;
    gv_execution_id             VARCHAR2(100) := NULL;
    gv_batch_id                 VARCHAR2(200) := NULL;
    gv_credential_name          CONSTANT VARCHAR2(30) := 'OCI$RESOURCE_PRINCIPAL';
    gv_status_success           CONSTANT VARCHAR2(100) := 'Success';
    gv_status_failure           CONSTANT VARCHAR2(100) := 'Failure';
    gv_conversion_id            VARCHAR2(100) := NULL;
    gv_boundary_system          VARCHAR2(100) := NULL;
    gv_status_picked            CONSTANT VARCHAR2(100) := 'File_Picked_From_Oci_And_Loaded_To_Stg';
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
    gv_recon_folder             CONSTANT VARCHAR2(100) := 'ATP_Validation_Recon_Report_Files';
    gv_recon_report             CONSTANT VARCHAR2(100) := 'Recon_Report_Created';
    gv_file_not_found           CONSTANT VARCHAR2(100) := 'File_Not_Found';

/*===========================================================================================================
-- PROCEDURE : MAIN_PRC
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
     /*  -- Fetch conversion metadata 
        BEGIN
            SELECT   cm.id               
            INTO     gv_conversion_id               
            FROM     conversion_metadata cm
            WHERE    cm.id = gv_conversion_id;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error fetching conversion metadata: ' || SQLERRM);
        END;*/

        dbms_output.put_line('conversion_id: ' || gv_conversion_id);

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

            dbms_output.put_line('Execution ID: ' || gv_execution_id);
            dbms_output.put_line('File Path: ' || gv_oci_file_path);
            dbms_output.put_line('File Name: ' || gv_oci_file_name);
            dbms_output.put_line('Fetched execution details:');


        -- Split the concatenated file names and assign to global variables
            LOOP
                lv_end_pos := instr(gv_oci_file_name, '.csv', lv_start_pos) + 3;
                EXIT WHEN lv_end_pos = 3; -- Exit loop if no more '.csv' found

                lv_file_name := substr(gv_oci_file_name, lv_start_pos, lv_end_pos - lv_start_pos + 1);
                dbms_output.put_line('Processing file name: ' || lv_file_name); -- Debugging output

                CASE
                    WHEN lv_file_name LIKE '%SupBusClassAttachments%.csv' THEN
                        gv_oci_file_name_bus_class := lv_file_name;
                    ELSE
                        dbms_output.put_line('No match found for file name: ' || lv_file_name); -- Debugging output
                END CASE;

                lv_start_pos := lv_end_pos + 1;
            END LOOP;

        -- Output the results for debugging
            dbms_output.put_line('lv_file_name: ' || lv_file_name);
            dbms_output.put_line('Business classification File Name: ' || gv_oci_file_name_bus_class);
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error fetching execution details: ' || sqlerrm);
		--RETURN;
        END;	

    -- Call to import data from OCI to Stage table
        BEGIN
            import_data_from_oci_to_stg_prc(p_loading_status);
            IF p_loading_status = gv_status_failure THEN
                dbms_output.put_line('Error in import_data_from_oci_to_stg_prc');
                RETURN;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling IMPORT_DATA_FROM_OCI_TO_STG_PRC: ' || sqlerrm);
            --RETURN;
        END;

    -- Call to perform data and business validations in staging table
        BEGIN
            data_validations_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling data_validations: ' || sqlerrm);
            --RETURN;
        END;

    -- Call to create a CSV file after all validations
        BEGIN
            create_fbdi_file_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_fbdi_file: ' || sqlerrm);
            --RETURN;
        END;

	--CREATE RECON REPORT 

        BEGIN
            create_recon_report_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_recon_report: ' || sqlerrm);
            -- RETURN;
        END;

    END main_prc;
 /*=================================================================================================================
-- PROCEDURE : IMPORT_DATA_FROM_OCI_TO_STG_PRC
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
                        upper(object_name) = 'XXCNV_AP_C012_SUPPLIER_BUS_CLASS_ATTACHMENTS_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c012_supplier_bus_class_attachments_ext';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c012_supplier_bus_class_attachments_stg';
                    dbms_output.put_line('table xxcnv_ap_c012_supplier_bus_class_attachments_ext dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_ap_c012_supplier_bus_class_attachments_ext: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    p_loading_status := gv_status_failure;
			--RETURN;
            END;

            IF gv_oci_file_name_bus_class LIKE '%SupBusClassAttachments%' THEN
                dbms_output.put_line('Creating external table xxcnv_ap_c012_supplier_bus_class_attachments_ext');
                dbms_output.put_line(' xxcnv_ap_c012_supplier_bus_class_attachments_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_bus_class);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_ap_c012_supplier_bus_class_attachments_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_bus_class,
                    format          =>
                            JSON_OBJECT(
                                'type' VALUE 'csv',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'skipheaders' VALUE '1',
                                'dateformat' VALUE 'yyyy/mm/dd',
                                'ignoremissingcolumns' VALUE 'true',
                                        'blankasnull' VALUE 'true'
                            ),
                    column_list     => '
					 BATCH_ID                      		  VARCHAR2(200 CHAR),
                     IMPORT_ACTION                        VARCHAR2(10 CHAR),
                     VENDOR_NAME                          VARCHAR2(360 CHAR),
                     CLASSIFICATION                       VARCHAR2(240 CHAR),
                     SUBCLASSIFICATION                    VARCHAR2(15 CHAR),
					 CERTIFYING_AGENCY                    VARCHAR2(200 CHAR),
					 CERTIFICATE_NUMBER                   VARCHAR2(200 CHAR),
                     ATTACHMENT_CATEGORY                  VARCHAR2(30 CHAR),
                     ATTACHMENT_TYPE                      VARCHAR2(30 CHAR),
                     FILE_TEXT_URL                        CLOB,
                     FILE_ATTACHMENTS_ZIP                 VARCHAR2(2000 CHAR),
                     ATTACHMENT_TITLE                     VARCHAR2(200 CHAR),
                     ATTACHMENT_DESCRIPTION               VARCHAR2(225 CHAR)
					'
                );

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_ap_c012_supplier_bus_class_attachments_stg (
			            BATCH_ID,
                        IMPORT_ACTION,
                        VENDOR_NAME,
                        CLASSIFICATION,
                        SUBCLASSIFICATION,
					    CERTIFYING_AGENCY,
					    CERTIFICATE_NUMBER,
                        ATTACHMENT_CATEGORY,
                        ATTACHMENT_TYPE,
                        FILE_TEXT_URL,
                        FILE_ATTACHMENTS_ZIP,
                        ATTACHMENT_TITLE,
                        ATTACHMENT_DESCRIPTION,
						file_name,
						import_status,
						error_message,
						file_reference_identifier,
						source_system) 
					SELECT 
						BATCH_ID,
                        IMPORT_ACTION,
                        VENDOR_NAME,
                        CLASSIFICATION,
                        SUBCLASSIFICATION,
					    CERTIFYING_AGENCY,
					    CERTIFICATE_NUMBER,
                        ATTACHMENT_CATEGORY,
                        ATTACHMENT_TYPE,
                        FILE_TEXT_URL,
                        FILE_ATTACHMENTS_ZIP,
                        ATTACHMENT_TITLE,
                        ATTACHMENT_DESCRIPTION, 
						null,
						null,
						null,
						null,
						null
						FROM xxcnv_ap_c012_supplier_bus_class_attachments_ext';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_ap_c012_supplier_bus_class_attachments_ext: ' || SQL%rowcount);
				--commit;
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error creating external table: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

    -- Count the number of rows in the external table
        BEGIN
            IF gv_oci_file_name = '%SupBusClassAttachments%' THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_ap_c012_supplier_bus_class_attachments_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_ap_c012_supplier_bus_class_attachments_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in the external table: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

    END import_data_from_oci_to_stg_prc;
/*=================================================================================================================
-- PROCEDURE : DATA_VALIDATIONS_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for the validating the mandatory columns and business validations as per lean spec
===================================================================================================================*/
    PROCEDURE data_validations_prc IS

  -- Declaring Local Variables for validation.     
        lv_row_count   NUMBER;
        lv_error_count NUMBER;
    BEGIN
       -- BEGIN --table1

     -- Initializing batch_id to current time stamp --

        SELECT
            to_char(sysdate, 'YYYYMMDDHHMM')
        INTO gv_batch_id
        FROM
            dual;

        BEGIN
            BEGIN
                UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                SET
                    execution_id = gv_execution_id,
                    batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;
            -- dbms_output.put_line('source_system is updated');
            END;
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_ap_c012_supplier_bus_class_attachments_stg
            WHERE
                execution_id = gv_execution_id;

            IF lv_row_count <> 0 THEN 

		  -- Initialize ERROR_MESSAGE to an empty string if it is NULL
                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        error_message = ''
                    WHERE
                        error_message IS NULL
			--and execution_id = gv_execution_id
                        ;

                END;
				-- Code changes for the Jira LTCI - 8094 starts
                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        ns_vendor_num = vendor_name
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;
				
				-- Set Vendor name to null
                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        vendor_name = ''
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Updating vendor name to null');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg stg
                    SET
                        vendor_name = (
                            SELECT
                                vt.oc_vendor_name
                            FROM
                                xxcnv_ap_c012_sup_bus_class_mapping vt
                            WHERE
                                vt.ns_vendor_num = stg.ns_vendor_num
                            GROUP BY
                                vt.oc_vendor_name
                        )
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;
            -- code change for the jira LTCI-8094 Ends
			
    -- Validate Supplier Name
                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        error_message = error_message || '|Supplier Name is required. '
                    WHERE
                        vendor_name IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error validating Supplier Name.');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        error_message = error_message || '|Classification is required. '
                    WHERE
                        classification IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error setting default value for Classification.');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg stg
                    SET
                        error_message = error_message || '|Classification should match between Supplier Business Classification table and Business Classification Attachment table'
                    WHERE
                        classification IS NOT NULL
                        AND error_message IS NULL
                        AND NOT EXISTS (
                            SELECT
                                1
                            FROM
                                xxcnv_ap_c012_sup_bus_class_mapping map
                            WHERE
                                    map.oc_vendor_name = stg.vendor_name
                                AND map.classification_lookup_code = stg.classification
                        )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error setting default value for Classification.');
                END;

    -- Validate File/Text/URL
                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        error_message = error_message || '|File/Text/URL is required. '
                    WHERE
                        file_text_url IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error validating File/Text/URL.');
                END;

   -- Validate File/Text/URL --DEFAULT FILE IS REQUIRED
                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        error_message = error_message || '|File/Text/URL contains Invalid characters'
                    WHERE
                        REGEXP_LIKE ( to_char(file_text_url),
                                      '[<>*?|;":\%$()]' )
                        AND file_reference_identifier IS NULL;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('File/Text/URL contains Invalid characters.');
                END;

    -- Validate File Attachments .ZIP
                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        error_message = error_message || '|File Attachments.ZIP is required '
                    WHERE
                        file_attachments_zip IS NULL
                        AND file_reference_identifier IS NULL;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error validating File Attachments .ZIP.');
                END;
        -- Validate attachment title
                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        error_message = error_message || '|Title length exceeded'
                    WHERE
                            lengthb(attachment_title) > 80
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error validation of title length');
                END;

					-- Update constant values
                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        attachment_category = 'FROM_SUPPLIER',
                        attachment_type = 'FILE',
                        import_action = 'CREATE'
                    WHERE
                        file_reference_identifier IS NULL;

                END;

        -- Update import_status based on error_message
                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
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
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        error_message = ltrim(error_message, ','),
                        import_status =
                            CASE
                                WHEN error_message IS NOT NULL THEN
                                    'ERROR'
                                ELSE
                                    'PROCESSED'
                            END;
		     --where execution_id = gv_execution_id ;
                    dbms_output.put_line('import_status column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        file_name = gv_oci_file_name_bus_class
                    WHERE --execution_id = gv_execution_id ;
                        file_reference_identifier IS NULL;

                    dbms_output.put_line('file_name column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    SET
                        source_system = gv_boundary_system
                    WHERE
                        file_reference_identifier IS NULL;

                    dbms_output.put_line('source_system is updated');
                END;

-- Check if there are any error messages
                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_ap_c012_supplier_bus_class_attachments_stg
                WHERE
                    error_message IS NOT NULL; 
		 --and execution_id = gv_execution_id ;

                UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    error_message IS NOT NULL
                    AND file_reference_identifier IS NULL;
		    --and execution_id = gv_execution_id ;
                dbms_output.put_line('file_reference_identifier column is updated');
                UPDATE xxcnv_ap_c012_supplier_bus_class_attachments_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success
                WHERE
                    error_message IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('file_reference_identifier column is updated');
                IF lv_error_count > 0 THEN

	       -- Logging the message If data is not validated
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_failed,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_bus_class,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                ELSIF gv_oci_file_name_bus_class IS NOT NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_validated,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_bus_class,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                ELSE
                    NULL;
                END IF;

                IF gv_oci_file_name_bus_class IS NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_file_not_found,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_bus_class,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

            ELSE
                dbms_output.put_line('No Data is found in interface tables. Data is not loaded from ext to stg ');
            END IF;

        END;

    END data_validations_prc;
/*==============================================================================================================================
-- PROCEDURE : CREATE_FBDI_FILE_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the FBDI CSV file by using the data in the supplier attachment stage tables  after all validations.
================================================================================================================================= */
    PROCEDURE create_fbdi_file_prc IS

        CURSOR batch_id_cursor_bus_class IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c012_supplier_bus_class_attachments_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_success;

        lv_success_count NUMBER := 0;
        lv_batch_id      NUMBER := 0;
    BEGIN      

--TABLE 2

        BEGIN
            lv_batch_id := 0;
            FOR g_id IN batch_id_cursor_bus_class LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing batch_id: ' || lv_batch_id);
                BEGIN
                    lv_success_count := 0;

                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                    SELECT
                        COUNT(*)
                    INTO lv_success_count
                    FROM
                        xxcnv_ap_c012_supplier_bus_class_attachments_stg
                    WHERE
                            batch_id = lv_batch_id
                --AND error_message IS NOT NULL
                        AND file_reference_identifier = gv_execution_id
                                                        || '_'
                                                        || gv_status_success;
                --AND TRIM(error_message) != '';

                    dbms_output.put_line('Success record count for XXCNV_AP_C012_SUPPLIER_BUS_CLASS_ATTACHMENTS_STG batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || lv_success_count);
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for XXCNV_AP_C012_SUPPLIER_BUS_CLASS_ATTACHMENTS_STG batch_id: ' || lv_batch_id
                        );
                        RETURN;
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error checking success record count for batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN;
                END;

                IF lv_success_count > 0 THEN
                    BEGIN
                        dbms_cloud.export_data(
                            credential_name => gv_credential_name,
                            file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                             || '/'
                                             || lv_batch_id
                                             || gv_oci_file_name_bus_class,
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv',
                                        'trimspaces' VALUE 'rtrim',
                                        'header' VALUE FALSE
                                    ),
                            query           => 'SELECT 
                                           IMPORT_ACTION,
                                           VENDOR_NAME,
                                           CLASSIFICATION,
                                           SUBCLASSIFICATION,
					                       CERTIFYING_AGENCY,
					                       CERTIFICATE_NUMBER,
                                           ATTACHMENT_CATEGORY,
                                           ATTACHMENT_TYPE,
                                           FILE_TEXT_URL,
                                           FILE_ATTACHMENTS_ZIP,
                                           ATTACHMENT_TITLE,
                                           ATTACHMENT_DESCRIPTION,
										   BATCH_ID
                                           FROM XXCNV_AP_C012_SUPPLIER_BUS_CLASS_ATTACHMENTS_STG
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

                        dbms_output.put_line('CSV file for batch_id '
                                             || lv_batch_id
                                             || ' exported successfully to SupplierBusClass OCI Object Storage.');
                        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                            p_conversion_id     => gv_conversion_id,
                            p_execution_id      => gv_execution_id,
                            p_execution_step    => gv_fbdi_export_status,
                            p_boundary_system   => gv_boundary_system,
                            p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                            p_file_name         => lv_batch_id
                                           || '_'
                                           || gv_oci_file_name_bus_class,
                            p_attribute1        => lv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );

                    EXCEPTION
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error exporting data to CSV for XXCNV_AP_C012_SUPPLIER_BUS_CLASS_ATTACHMENTS_STG batch_id '
                                                 || lv_batch_id
                                                 || ': '
                                                 || sqlerrm);
                            RETURN;
                    END;
                ELSE
                    dbms_output.put_line('Process Stopped for XXCNV_AP_C012_SUPPLIER_BUS_CLASS_ATTACHMENTS_STG batch_id '
                                         || lv_batch_id
                                         || ': Error message columns contain data.');
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
-- PROCEDURE : CREATE_PROPERTIES_FILE_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating properties file.
================================================================================================================================= */
    PROCEDURE create_properties_file_prc IS

        CURSOR batch_id_cursor IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c012_supplier_bus_class_attachments_stg
        WHERE
            execution_id = gv_execution_id;

        lv_error_count NUMBER := 0;
        lv_batch_id    NUMBER := 0;
    BEGIN
        FOR g_id IN batch_id_cursor LOOP
            lv_batch_id := g_id.batch_id;
            dbms_output.put_line('Processing batch_id: ' || lv_batch_id);
            BEGIN
                dbms_cloud.export_data(
                    credential_name => gv_credential_name,
                    file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                     || '/'
                                     || lv_batch_id
                                     || 'supplierattachmentsimport.properties',
                    format          =>
                            JSON_OBJECT(
                                'trimspaces' VALUE 'rtrim'
                            ),
                    query           => 'SELECT ''/oracle/apps/ess/financials/assets/additions/,PostMassAdditions,supplierattachmentsimport,'
                             || lv_batch_id
                             || ',null,NORMAL,null,null,null,null,null''as column1 from dual'
                );

                dbms_output.put_line('Properties file for book_type_code '
                                     || lv_batch_id
                                     || ' exported successfully to OCI Object Storage.');
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_staged,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                    p_file_name         => lv_batch_id
                                   || '_'
                                   || 'supplierattachmentsimport.properties',
                    p_attribute1        => lv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error exporting data to properties for batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
                    --RETURN;
            END;
       /* ELSE
            dbms_output.put_line('Process Stopped for batch_id ' || lv_batch_id || ': Error message columns contain data.');
        END IF;*/
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('An error occurred: ' || sqlerrm);
    END create_properties_file_prc;

/*==============================================================================================================================
-- PROCEDURE : CREATE_RECON_REPORT_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating properties file.
================================================================================================================================= */

    PROCEDURE create_recon_report_prc IS

        CURSOR batch_id_bus_class_recon IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_ap_c012_supplier_bus_class_attachments_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        lv_batch_id NUMBER := 0;

--table2
    BEGIN
        FOR g_id IN batch_id_bus_class_recon LOOP
            lv_batch_id := g_id.batch_id;
            BEGIN
                dbms_cloud.export_data(
                    credential_name => gv_credential_name,
                    file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                     || '/'
                                     || lv_batch_id
                                     || 'ATP_Recon_Supplier_bus_class_Attachments'
                                     || sysdate,
                    format          =>
                            JSON_OBJECT(
                                'type' VALUE 'csv',
                                'trimspaces' VALUE 'rtrim',
                                'header' VALUE TRUE
                            ),
                    query           => '
                                       SELECT 
                                           BATCH_ID,
                                           IMPORT_ACTION,
                                           VENDOR_NAME,
                                           CLASSIFICATION,
                                           SUBCLASSIFICATION,
					                       CERTIFYING_AGENCY,
					                       CERTIFICATE_NUMBER,
                                           ATTACHMENT_CATEGORY,
                                           ATTACHMENT_TYPE,
                                           FILE_TEXT_URL,
                                           FILE_ATTACHMENTS_ZIP,
                                           ATTACHMENT_TITLE,
                                           ATTACHMENT_DESCRIPTION,
                                           file_name,
                                           error_message,
                                           import_status,
                                           source_system
                                           FROM XXCNV_AP_C012_SUPPLIER_BUS_CLASS_ATTACHMENTS_STG
                                            where import_status = '''
                             || 'ERROR'
                             || ''' AND
											execution_id  =  '''
                             || gv_execution_id
                             || ''''
                );

                dbms_output.put_line('CSV file for batch_id '
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
                                   || gv_oci_file_name_bus_class,
                    p_attribute1        => lv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error exporting data to CSV for batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
                        --RETURN;
            END;

        END LOOP;
    END create_recon_report_prc;

END xxcnv_ap_c012_supplier_attachment_conversion_pkg;