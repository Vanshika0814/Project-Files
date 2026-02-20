create or replace PACKAGE BODY XXCNV.XXCNV_AP_C039_AP_Invoices_Attachments_CONVERSION_PKG IS
    /*************************************************************************************
    NAME              :     XXCNV.XXCNV_AP_C039_AP_Invoices_Attachments_CONVERSION_PKG  BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
    -- Modification History
    -- Developer          Date         Version     Comments and changes made
    -- -------------   ------       ----------  -----------------------------------------
    --  Bhargavi.K   28-May-2025       1.0         Initial Development
    --  Bhargavi.K   26-Jul-2025       1.1         Removed XXCNV. at line 289
    ****************************************************************************************/

    -- Declaring global Variables
    gv_import_status                    VARCHAR2(256)    := NULL;
    gv_error_message                    VARCHAR2(500)    := NULL;
	gv_file_name            			VARCHAR2(256)   := NULL;
    gv_oci_file_path                    VARCHAR2(256)    := NULL;
    gv_oci_file_name                    VARCHAR2(4000)   := NULL; 
    gv_oci_file_name_AP_Attachments             VARCHAR2(100)    := NULL;
    gv_execution_id                     VARCHAR2(100)    := NULL;
    gv_batch_id                         NUMBER(38)       := NULL;
    gv_credential_name      CONSTANT    VARCHAR2(100)    := 'OCI$RESOURCE_PRINCIPAL';                
    gv_status_success       CONSTANT    VARCHAR2(100)    := 'Success';
    gv_status_failure       CONSTANT    VARCHAR2(100)    := 'Failure';
    gv_conversion_id                    VARCHAR2(100)    := NULL;
	gv_boundary_system	            	VARCHAR2(100)	:=  NULL;
    gv_status_picked            CONSTANT VARCHAR2(100) := 'File_Picked_From_OCI_And_Loaded_To_Stg';
    gv_status_picked_for_tr     CONSTANT VARCHAR2(100) := 'Transformed_Data_From_Ext_To_Stg';

    gv_status_validated     CONSTANT    VARCHAR2(100)    := 'VALIDATED';
	gv_status_failed   	    CONSTANT 	VARCHAR2(100)	:= 'FAILED_AT_VALIDATION';
	gv_status_failed_validation CONSTANT VARCHAR2(100)   := 'NOT_VALIDATED';
    gv_fbdi_export_status   CONSTANT    VARCHAR2(100)    := 'EXPORTED_TO_FBDI';
    gv_status_staged        CONSTANT    VARCHAR2(100)    := 'STAGED_FOR_IMPORT';    
        gv_transformed_folder       CONSTANT VARCHAR2(100) := 'Transformed_FBDI_Files';
    gv_source_folder            CONSTANT VARCHAR2(100) := 'Source_FBDI_Files';
    gv_properties           CONSTANT    VARCHAR2(100)    := 'properties';
    gv_file_picked                      VARCHAR2(100)    := 'File_Picked_From_OCI_Server';
	gv_recon_folder             CONSTANT VARCHAR2(50) := 'ATP_Validation_Error_Files';
	gv_recon_report         CONSTANT    VARCHAR2(100)    := 'Recon_Report_Created';
	gv_file_not_found       CONSTANT    VARCHAR2(100)    := 'File_not_found';

    /*===========================================================================================================
    -- PROCEDURE : MAIN_PRC
    -- PARAMETERS:
    -- COMMENT   : This procedure is used to call all the procedures under a single procedure
    ==============================================================================================================*/
    PROCEDURE MAIN_PRC ( p_RICE_ID 	            IN  		VARCHAR2,
                     p_execution_id 		IN  	    VARCHAR2,
                     p_boundary_system      IN  		VARCHAR2,
			         p_file_name 		    IN  		VARCHAR2)AS
    p_loading_status VARCHAR2(30) := NULL;
    lv_start_pos NUMBER := 1;
    lv_end_pos NUMBER;
    lv_file_name VARCHAR2(4000);
    BEGIN
        gv_conversion_id := p_rice_id;
        gv_execution_id  := p_execution_id ;
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
					AND ce.STATUS = gv_file_picked
					AND ce.last_update_date = (
						SELECT MAX(ce1.last_update_date) 
						FROM xxcnv_cmn_conversion_execution ce1
						WHERE ce1.conversion_id = gv_conversion_id
						AND ce1.STATUS = gv_file_picked 
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
					lv_end_pos := INSTR(gv_oci_file_name, '.csv', lv_start_pos) + 3;
					EXIT WHEN lv_end_pos = 3; -- Exit loop if no more '.csv' found

					lv_file_name := SUBSTR(gv_oci_file_name, lv_start_pos, lv_end_pos - lv_start_pos + 1);
					dbms_output.put_line('Processing file name: ' || lv_file_name); -- Debugging output

					CASE
						WHEN lv_file_name LIKE '%APAttachment%.csv' THEN gv_oci_file_name_AP_Attachments := lv_file_name;

						ELSE
							dbms_output.put_line('No match found for file name: ' || lv_file_name); -- Debugging output
					END CASE;

					lv_start_pos := lv_end_pos + 1;
				END LOOP;

				-- Output the results for debugging
				dbms_output.put_line('lv_File Name: ' || lv_file_name);
				dbms_output.put_line('AP Attachment File Name: ' || gv_oci_file_name_AP_Attachments);


			EXCEPTION
				WHEN OTHERS THEN
					dbms_output.put_line('Error fetching execution details: ' || SQLERRM);
					--RETURN;
			END;

			-- Call to import data from OCI to external table
			BEGIN
				IMPORT_DATA_FROM_OCI_TO_STG_PRC(p_loading_status);
				IF p_loading_status = gv_status_failure THEN
					dbms_output.put_line('Error in IMPORT_DATA_FROM_OCI_TO_STG_PRC');
					RETURN;
				END IF;
			EXCEPTION
				WHEN OTHERS THEN
					dbms_output.put_line('Error calling IMPORT_DATA_FROM_OCI_TO_STG_PRC: ' || SQLERRM);
					-- RETURN;
			END;





END MAIN_PRC;

/*=================================================================================================================
-- PROCEDURE : IMPORT_DATA_FROM_OCI_TO_STG_PRC
-- PARAMETERS: p_loading_status
-- COMMENT   : This procedure is used to create an external table and transfer that data from external to stg table.
===================================================================================================================*/
PROCEDURE IMPORT_DATA_FROM_OCI_TO_STG_PRC (p_loading_status OUT VARCHAR2) IS
    lv_table_count NUMBER := 0;
    lv_row_count   NUMBER := 0;
BEGIN

	BEGIN
	lv_table_count := 0;
	SELECT COUNT(*)
            INTO lv_table_count
            FROM all_objects
            WHERE UPPER(object_name) = 'XXCNV_AP_C039_AP_Attachments_EXT'
            AND object_type = 'TABLE';

            IF lv_table_count > 0 THEN
			    EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_AP_C039_InvoiceAttachments_Mapping';
                EXECUTE IMMEDIATE 'DROP TABLE XXCNV_AP_C039_AP_Attachments_EXT';
                dbms_output.put_line('Table XXCNV_AP_C039_AP_Attachments_EXT  dropped');
            END IF;
			EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error dropping table XXCNV_AP_C039_AP_Attachments_EXT : ' ||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
            p_loading_status := gv_status_failure;
			--RETURN;
	END;

    -- Create the external table
    BEGIN

        IF gv_oci_file_name_AP_Attachments LIKE '%APAttachment%' THEN

            dbms_output.put_line('Creating external table XXCNV_AP_C039_AP_Attachments_EXT');
					dbms_output.put_line(' XXCNV_AP_C039_AP_Attachments_EXT  : '|| gv_oci_file_path||'/'||gv_oci_file_name_AP_Attachments);


	DBMS_CLOUD.CREATE_EXTERNAL_TABLE(

		 table_name => 'XXCNV_AP_C039_AP_Attachments_EXT',
		 credential_name => gv_credential_name,
		 file_uri_list   =>  gv_oci_file_path||'/'||gv_oci_file_name_AP_Attachments,
		 format => json_object('skipheaders' VALUE '1','type' VALUE 'csv','rejectlimit' value 'UNLIMITED','ignoremissingcolumns' value 'true','blankasnull' value 'true','dateformat' VALUE 'mm/dd/yyyy','conversionerrors' VALUE 'store_null'), 
		 column_list => 
				'
                 InvoiceNumber Varchar2(50),
                 Attachment_FilePath Varchar2(500)
				');

	dbms_output.put_line('External table XXCNV_AP_C039_AP_Attachments_EXT  is created');

			EXECUTE IMMEDIATE  'INSERT INTO XXCNV_AP_C039_InvoiceAttachments_Mapping (
					            Record_Id ,
                 InvoiceID ,
                 InvoiceNumber ,
                 Attachment_FilePath,
                 Attachment_FileName ,
                 AttachmentFlag ,
                 ErrorMessage ,
				 Import_Status
							) SELECT
							NULL ,
                            NULL ,
                            InvoiceNumber ,
                            Attachment_FilePath,
                            NULL ,
                            NULL ,
                            NULL ,
				            NULL

								 FROM XXCNV_AP_C039_AP_Attachments_EXT ';

				p_loading_status := gv_status_success;	

				dbms_output.put_line('Inserted records in XXCNV_AP_C039_InvoiceAttachments_Mapping: '||SQL%ROWCOUNT);
				--commit;
        END IF;


    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error creating external table: ' || SQLERRM);
            p_loading_status := gv_status_failure;
            RETURN;
    END;

    -- Count the number of rows in the external table
    BEGIN
        IF gv_oci_file_name = '%APAttachment%' THEN
            SELECT COUNT(*)
            INTO lv_row_count
            FROM XXCNV_AP_C039_InvoiceAttachments_Mapping;
            dbms_output.put_line('Inserted Records in the XXCNV_AP_C039_InvoiceAttachments_Mapping from OCI Source Folder: ' || lv_row_count);
		END IF;



    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error counting rows in the external table: ' || SQLERRM);
            p_loading_status := gv_status_failure;
            RETURN;
    END;

    -- Select FEEDER_IMPORT_BATCH_ID from the external table
   BEGIN
        -- Count the number of rows in the external table
        SELECT COUNT(*)
        INTO lv_row_count
        FROM XXCNV_AP_C039_InvoiceAttachments_Mapping;

        dbms_output.put_line('Log:Inserted Records in the XXCNV_AP_C039_InvoiceAttachments_Mapping from OCI Source Folder: ' || lv_row_count);

        -- Use an implicit cursor in the FOR LOOP to iterate over distinct batch_ids

            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id    => gv_conversion_id,
                p_execution_id     => gv_execution_id,
                p_execution_step   => gv_status_picked,
                p_boundary_system  => gv_boundary_system,
                p_file_path        => gv_oci_file_path,
                p_file_name        => gv_oci_file_name,
                P_attribute1       => NULL,
                P_attribute2       => lv_row_count,
                p_process_reference => NULL
            );


        p_loading_status := gv_status_success;

    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error counting rows in XXCNV_AP_C039_InvoiceAttachments_Mapping: ' || SQLERRM);
            p_loading_status := gv_status_failure;
            RETURN;

END;

END IMPORT_DATA_FROM_OCI_TO_STG_PRC;



END XXCNV_AP_C039_AP_Invoices_Attachments_CONVERSION_PKG;
