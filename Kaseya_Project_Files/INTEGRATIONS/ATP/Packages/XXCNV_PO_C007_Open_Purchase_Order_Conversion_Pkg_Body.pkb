CREATE OR REPLACE PACKAGE BODY xxcnv.xxcnv_po_c007_open_purchase_order_conversion_pkg IS 
    /*************************************************************************************
    NAME              :     PO_Conversion_Package BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
    -- Modification History
    -- Developer          Date         Version     Comments and changes made
    -- -------------   ------       ----------  -----------------------------------------
    -- Bhargavi.K     29-Apr-2025       1.0         Initial Development
    -- Bhargavi.K     26-Jul-2025       1.1         Removed XXCNV. at line 5421
    -- Bhargavi.K     01-Aug-2025       1.2         Added changes for JIRA:LTCI-6487 
    -- Bhargavi.K     02-Aug-2025       1.3         Added changes for JIRA:LTCI-6585
    ****************************************************************************************/

    -- Declaring global Variables
    gv_import_status                VARCHAR2(256) := NULL;
    gv_error_message                VARCHAR2(500) := NULL;
    gv_oci_file_path                VARCHAR2(256) := NULL;
    gv_oci_file_name                VARCHAR2(4000) := NULL;
    gv_oci_file_name_headers        VARCHAR2(50) := NULL;
    gv_oci_file_name_lines          VARCHAR2(50) := NULL;
    gv_oci_file_name_line_locations VARCHAR2(50) := NULL;
    gv_oci_file_name_distributions  VARCHAR2(50) := NULL;
    gv_execution_id                 VARCHAR2(30) := NULL;
    gv_batch_id                     NUMBER(18) := NULL;
    gv_credential_name              CONSTANT VARCHAR2(25) := 'OCI$RESOURCE_PRINCIPAL';
    gv_status_success               CONSTANT VARCHAR2(15) := 'Success';
    gv_status_failure               CONSTANT VARCHAR2(15) := 'Failure';
    gv_coa_transformation_failed    CONSTANT VARCHAR2(50) := 'COA_TRANSFORMATION_FAILED';
    gv_coa_transformation           CONSTANT VARCHAR2(50) := 'COA_TRANSFORMATION';
    gv_file_name                    VARCHAR2(256) := NULL;
    gv_conversion_id                VARCHAR2(15) := NULL;
    gv_boundary_system              CONSTANT VARCHAR2(25) := NULL;
    gv_status_picked                CONSTANT VARCHAR2(100) := 'File_Picked_From_OCI_And_Loaded_To_Stg';
    gv_status_picked_for_tr         CONSTANT VARCHAR2(100) := 'Transformed_Data_From_Ext_To_Stg';
    gv_status_validated             CONSTANT VARCHAR2(50) := 'VALIDATED';
    gv_status_failed                CONSTANT VARCHAR2(50) := 'FAILED_AT_VALIDATION';
    gv_fbdi_export_status           CONSTANT VARCHAR2(50) := 'EXPORTED_TO_FBDI';
    gv_status_staged                CONSTANT VARCHAR2(50) := 'STAGED_FOR_IMPORT';
    gv_transformed_folder           CONSTANT VARCHAR2(100) := 'Transformed_FBDI_Files';
    gv_source_folder                CONSTANT VARCHAR2(100) := 'Source_FBDI_Files';
    gv_properties                   CONSTANT VARCHAR2(15) := 'properties';
    gv_file_picked                  VARCHAR2(50) := 'File_Picked_From_OCI_Server';
    gv_recon_folder                 CONSTANT VARCHAR2(50) := 'ATP_Validation_Error_Files';
    gv_recon_report                 CONSTANT VARCHAR2(100) := 'Recon_Report_Created';

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
                    WHEN lv_file_name LIKE '%PoHeadersInterfaceOrder%.csv' THEN
                        gv_oci_file_name_headers := lv_file_name;
                    WHEN lv_file_name LIKE '%PoLinesInterfaceOrder%.csv' THEN
                        gv_oci_file_name_lines := lv_file_name;
                    WHEN lv_file_name LIKE '%PoLineLocationsInterfaceOrder%.csv' THEN
                        gv_oci_file_name_line_locations := lv_file_name;
                    WHEN lv_file_name LIKE '%PoDistributionsInterfaceOrder%.csv' THEN
                        gv_oci_file_name_distributions := lv_file_name;
                    ELSE
                        dbms_output.put_line('No match found for file name: ' || lv_file_name); -- Debugging output
                END CASE;

                lv_start_pos := lv_end_pos + 1;
            END LOOP;

				-- Output the results for debugging
            dbms_output.put_line('lv_File Name: ' || lv_file_name);
            dbms_output.put_line('Headers File Name: ' || gv_oci_file_name_headers);
            dbms_output.put_line('Lines File Name: ' || gv_oci_file_name_lines);
            dbms_output.put_line('line locations File Name: ' || gv_oci_file_name_line_locations);
            dbms_output.put_line('line distributions File Name: ' || gv_oci_file_name_distributions);
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error fetching execution details: ' || sqlerrm);
                RETURN;
        END;

			-- Call to import data from OCI to staging table
        BEGIN
            import_data_from_oci_to_stg_prc(p_loading_status);
            IF p_loading_status = gv_status_failure THEN
                dbms_output.put_line('Error in IMPORT_DATA_FROM_OCI_TO_STG_PRC');
                RETURN;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling IMPORT_DATA_FROM_OCI_TO_STG_PRC: ' || sqlerrm);
					-- RETURN;
        END;


   -- Call to perform data and business validations in interface table
        BEGIN
            data_validations_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling data_validations: ' || sqlerrm);
                RETURN;
        END;



  /*  Call to perform COA transaction */
 /* BEGIN
        coa_target_segments_dist_prc;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling coa_target_segments: ' ||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
           -- RETURN;
    END;*/



   -- Call to create a CSV file from XXCNV_PO_C007_PO_HEADERS_STG after all validations
        BEGIN
            create_fbdi_file_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_fbdi_file: ' || sqlerrm);
                RETURN;
        END;

     --CREATE RECON REPORT 

        BEGIN
            create_recon_report_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_recon_report: ' || sqlerrm);
            -- RETURN;
        END; 

    -- Call to create a properties file from  after all validations
   /* BEGIN
        CREATE_PROPERTIES_FILE_PRC;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling create_properties_file: ' || SQLERRM);
            RETURN;
    END; 
*/
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
                        upper(object_name) = 'XXCNV_PO_C007_PO_HEADERS_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE XXCNV_PO_C007_PO_HEADERS_EXT';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_PO_C007_PO_HEADERS_STG';
                    dbms_output.put_line('Table XXCNV_PO_C007_PO_HEADERS_EXT dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table XXCNV_PO_C007_PO_HEADERS_EXT: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
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
                        upper(object_name) = 'XXCNV_PO_C007_PO_LINES_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE XXCNV_PO_C007_PO_LINES_EXT';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_PO_C007_PO_LINES_STG';
                    dbms_output.put_line('Table XXCNV_PO_C007_PO_LINES_EXT dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table XXCNV_PO_C007_PO_LINES_EXT: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
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
                        upper(object_name) = 'XXCNV_PO_C007_PO_LINE_LOCATIONS_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE XXCNV_PO_C007_PO_LINE_LOCATIONS_EXT';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_PO_C007_PO_LINE_LOCATIONS_STG';
                    dbms_output.put_line('Table XXCNV_PO_C007_PO_LINE_LOCATIONS_EXT dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table XXCNV_PO_C007_PO_LINE_LOCATIONS_EXT: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
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
                        upper(object_name) = 'XXCNV_PO_C007_PO_DISTRIBUTIONS_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE XXCNV_PO_C007_PO_DISTRIBUTIONS_EXT';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_PO_C007_PO_DISTRIBUTIONS_STG';
                    dbms_output.put_line('Table XXCNV_PO_C007_PO_DISTRIBUTIONS_EXT dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table XXCNV_PO_C007_PO_DISTRIBUTIONS_EXT: '
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

	--TABLE1
            IF gv_oci_file_name_headers LIKE '%PoHeadersInterfaceOrder.csv%' THEN
                dbms_output.put_line('Creating external table XXCNV_PO_C007_PO_HEADERS_EXT');
                dbms_output.put_line(' XXCNV_PO_C007_PO_HEADERS_EXT : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_headers);
                dbms_cloud.create_external_table(
                    table_name      => 'XXCNV_PO_C007_PO_HEADERS_EXT',
                    credential_name => 'OCI$RESOURCE_PRINCIPAL',
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_headers,
		  -- file_uri_list =>  'https://objectstorage.us-ashburn-1.oraclecloud.com/n/nacaus19b/b/O2InnovationBucket/o/mock1/Item/1/SourceFBDI/EgpItemCategoriesInterface.csv',
	--	 format => json_object('type' VALUE 'csv','rejectlimit' value 'UNLIMITED','blankasnull' value 'true', 'dateformat' value 'yyyy/mm/dd'),
                    format          =>
                            JSON_OBJECT(
                                'skipheaders' VALUE '1',
                                'type' VALUE 'csv',
                                'dateformat' VALUE 'yyyy/mm/dd',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'ignoremissingcolumns' VALUE 'true',
                                        'blankasnull' VALUE 'true',
                                'conversionerrors' VALUE 'store_null'
                            ),  --
                    column_list     => '
						       INTERFACE_HEADER_KEY	VARCHAR2(50)	,
                               ACTION	VARCHAR2(25)	,
                               BATCH_ID	NUMBER	,
                               IMPORT_SOURCE_CODE	VARCHAR2(25)	,
                               APPROVAL_ACTION	VARCHAR2(25)	,
                               DOCUMENT_NUM	VARCHAR2(30)	,
                               DOCUMENT_TYPE_CODE	VARCHAR2(25)	,
                               STYLE_DISPLAY_NAME	VARCHAR2(240)	,
                               PRC_BU_NAME	VARCHAR2(240)	,
                               REQ_BU_NAME	VARCHAR2(240)	,
                               SOLDTO_LE_NAME	VARCHAR2(240)	,
                               BILLTO_BU_NAME	VARCHAR2(240)	,
                               AGENT_NAME	VARCHAR2(2000)	,
                               CURRENCY_CODE	VARCHAR2(15)	,
                               RATE	NUMBER	,
                               RATE_TYPE	VARCHAR2(30)	,
                               RATE_DATE	DATE	,
                               COMMENTS	VARCHAR2(500)	,
                               BILL_TO_LOCATION	VARCHAR2(60)	,
                               SHIP_TO_LOCATION	VARCHAR2(60)	,
                               VENDOR_NAME	VARCHAR2(360)	,
                               VENDOR_NUM	VARCHAR2(30)	,
                               SUPPLIER_SITE_CODE	VARCHAR2(240)	,
                               VENDOR_CONTACT	VARCHAR2(360)	,
                               VENDOR_DOC_NUM	VARCHAR2(25)	,
                               FOB	VARCHAR2(30)	,
                               FREIGHT_CARRIER	VARCHAR2(360)	,
                               FREIGHT_TERMS	VARCHAR2(30)	,
                               PAY_ON_CODE	VARCHAR2(25)	,
                               PAYMENT_TERMS	VARCHAR2(50)	,
                               ORIGINATOR_ROLE	VARCHAR2(25)	,
                               CHANGE_ORDER_DESC	VARCHAR2(2000)	,
                               ACCEPTANCE_REQUIRED_FLAG	VARCHAR2(1)	,
                               ACCEPTANCE_WITHIN_DAYS	NUMBER	,
                               SUPPLIER_NOTIF_METHOD	VARCHAR2(25)	,
                               FAX	VARCHAR2(60)	,
                               EMAIL_ADDRESS	VARCHAR2(2000)	,
                               CONFIRMING_ORDER_FLAG	VARCHAR2(1)	,
                               NOTE_TO_VENDOR	VARCHAR2(10000)	,
                               NOTE_TO_RECEIVER	VARCHAR2(10000)	,
                               DEFAULT_TAXATION_COUNTRY_CODE	VARCHAR2(2)	,
                               TAX_DOCUMENT_SUBTYPE	VARCHAR2(240)	,
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
                               ATTRIBUTE_NUMBER1	NUMBER(18)	,
                               ATTRIBUTE_NUMBER2	NUMBER(18)	,
                               ATTRIBUTE_NUMBER3	NUMBER(18)	,
                               ATTRIBUTE_NUMBER4	NUMBER(18)	,
                               ATTRIBUTE_NUMBER5	NUMBER(18)	,
                               ATTRIBUTE_NUMBER6	NUMBER(18)	,
                               ATTRIBUTE_NUMBER7	NUMBER(18)	,
                               ATTRIBUTE_NUMBER8	NUMBER(18)	,
                               ATTRIBUTE_NUMBER9	NUMBER(18)	,
                               ATTRIBUTE_NUMBER10	NUMBER(18)	,
                               ATTRIBUTE_TIMESTAMP1	TIMESTAMP(6)	,
                               ATTRIBUTE_TIMESTAMP2	TIMESTAMP(6)	,
                               ATTRIBUTE_TIMESTAMP3	TIMESTAMP(6)	,
                               ATTRIBUTE_TIMESTAMP4	TIMESTAMP(6)	,
                               ATTRIBUTE_TIMESTAMP5	TIMESTAMP(6)	,
                               ATTRIBUTE_TIMESTAMP6	TIMESTAMP(6)	,
                               ATTRIBUTE_TIMESTAMP7	TIMESTAMP(6)	,
                               ATTRIBUTE_TIMESTAMP8	TIMESTAMP(6)	,
                               ATTRIBUTE_TIMESTAMP9	TIMESTAMP(6)	,
                               ATTRIBUTE_TIMESTAMP10	TIMESTAMP(6)	,
                               AGENT_EMAIL_ADDRESS	VARCHAR2(240)	,
                               MODE_OF_TRANSPORT	VARCHAR2(80)	,
                               SERVICE_LEVEL	VARCHAR2(80)	,
                               FIRST_PTY_REG_NUM	VARCHAR2(50)	,
                               THIRD_PTY_REG_NUM	VARCHAR2(50)	,
                               BUYER_MANAGED_TRANSPORT_FLAG	VARCHAR2(1)	,
                               MASTER_CONTRACT_NUMBER	VARCHAR2(120)	,
                               MASTER_CONTRACT_TYPE	VARCHAR2(150)	,
                               CC_EMAIL_ADDRESS	VARCHAR2(2000)	,
                               BCC_EMAIL_ADDRESS	VARCHAR2(2000)	,
                               GLOBAL_ATTRIBUTE1	VARCHAR2(150)	,
                               GLOBAL_ATTRIBUTE2	VARCHAR2(150)	,
                               GLOBAL_ATTRIBUTE3	VARCHAR2(150)	,
                               GLOBAL_ATTRIBUTE4	VARCHAR2(150)	,
                               GLOBAL_ATTRIBUTE5	VARCHAR2(150)	,
                               GLOBAL_ATTRIBUTE6	VARCHAR2(150)	,
                               OVERRIDING_APPROVER_NAME	VARCHAR2(2000)	,
                               SKIP_ELECTRONIC_COMM_FLAG	VARCHAR2(1)	,
                               CHECKLIST_TITLE	VARCHAR2(80)	,
                               CHECKLIST_NUM	VARCHAR2(30)	,
                               ALT_CONTACT_EMAIL_ADDRESS	VARCHAR2(1500)	,
                               SPECIAL_HANDLING_TYPE	VARCHAR2(30)	,
                               SH_ATTRIBUTE1	VARCHAR2(150)	,
                               SH_ATTRIBUTE2	VARCHAR2(150)	,
                               SH_ATTRIBUTE3	VARCHAR2(150)	,
                               SH_ATTRIBUTE4	VARCHAR2(150)	,
                               SH_ATTRIBUTE5	VARCHAR2(150)	,
                               SH_ATTRIBUTE6	VARCHAR2(150)	,
                               SH_ATTRIBUTE7	VARCHAR2(150)	,
                               SH_ATTRIBUTE8	VARCHAR2(150)	,
                               SH_ATTRIBUTE9	VARCHAR2(150)	,
                               SH_ATTRIBUTE10	VARCHAR2(150)	,
                               SH_ATTRIBUTE11	VARCHAR2(150)	,
                               SH_ATTRIBUTE12	VARCHAR2(150)	,
                               SH_ATTRIBUTE13	VARCHAR2(150)	,
                               SH_ATTRIBUTE14	VARCHAR2(150)	,
                               SH_ATTRIBUTE15	VARCHAR2(150)	,
                               SH_ATTRIBUTE16	VARCHAR2(150)	,
                               SH_ATTRIBUTE17	VARCHAR2(150)	,
                               SH_ATTRIBUTE18	VARCHAR2(150)	,
                               SH_ATTRIBUTE19	VARCHAR2(150)	,
                               SH_ATTRIBUTE20	VARCHAR2(150)	,
                               SH_ATTRIBUTE_NUMBER1	NUMBER	,
                               SH_ATTRIBUTE_NUMBER2	NUMBER	,
                               SH_ATTRIBUTE_NUMBER3	NUMBER	,
                               SH_ATTRIBUTE_NUMBER4	NUMBER	,
                               SH_ATTRIBUTE_NUMBER5	NUMBER	,
                               SH_ATTRIBUTE_NUMBER6	NUMBER	,
                               SH_ATTRIBUTE_NUMBER7	NUMBER	,
                               SH_ATTRIBUTE_NUMBER8	NUMBER	,
                               SH_ATTRIBUTE_NUMBER9	NUMBER	,
                               SH_ATTRIBUTE_NUMBER10	NUMBER	,
                               SH_ATTRIBUTE_DATE1	DATE	,
                               SH_ATTRIBUTE_DATE2	DATE	,
                               SH_ATTRIBUTE_DATE3	DATE	,
                               SH_ATTRIBUTE_DATE4	DATE	,
                               SH_ATTRIBUTE_DATE5	DATE	,
                               SH_ATTRIBUTE_DATE6	DATE	,
                               SH_ATTRIBUTE_DATE7	DATE	,
                               SH_ATTRIBUTE_DATE8	DATE	,
                               SH_ATTRIBUTE_DATE9	DATE	,
                               SH_ATTRIBUTE_DATE10	DATE	,
                               SH_ATTRIBUTE_TIMESTAMP1	TIMESTAMP(6)	,
                               SH_ATTRIBUTE_TIMESTAMP2	TIMESTAMP(6)	,
                               SH_ATTRIBUTE_TIMESTAMP3	TIMESTAMP(6)	,
                               SH_ATTRIBUTE_TIMESTAMP4	TIMESTAMP(6)	,
                               SH_ATTRIBUTE_TIMESTAMP5	TIMESTAMP(6)	,
                               SH_ATTRIBUTE_TIMESTAMP6	TIMESTAMP(6)	,
                               SH_ATTRIBUTE_TIMESTAMP7	TIMESTAMP(6)	,
                               SH_ATTRIBUTE_TIMESTAMP8	TIMESTAMP(6)	,
                               SH_ATTRIBUTE_TIMESTAMP9	TIMESTAMP(6)	,
                               SH_ATTRIBUTE_TIMESTAMP10	TIMESTAMP(6)	

						'
                );

                dbms_output.put_line(' external table XXCNV_PO_C007_PO_HEADERS_EXT is created');
                EXECUTE IMMEDIATE 'INSERT INTO XXCNV_PO_C007_PO_HEADERS_STG (
INTERFACE_HEADER_KEY,
ACTION,
BATCH_ID,
IMPORT_SOURCE_CODE,
APPROVAL_ACTION,
DOCUMENT_NUM,
DOCUMENT_TYPE_CODE,
STYLE_DISPLAY_NAME,
PRC_BU_NAME,
REQ_BU_NAME,
SOLDTO_LE_NAME,
BILLTO_BU_NAME,
AGENT_NAME,
CURRENCY_CODE,
RATE,
RATE_TYPE,
RATE_DATE,
COMMENTS,
BILL_TO_LOCATION,
SHIP_TO_LOCATION,
VENDOR_NAME,
VENDOR_NUM,
SUPPLIER_SITE_CODE,
VENDOR_CONTACT,
VENDOR_DOC_NUM,
FOB,
FREIGHT_CARRIER,
FREIGHT_TERMS,
PAY_ON_CODE,
PAYMENT_TERMS,
ORIGINATOR_ROLE,
CHANGE_ORDER_DESC,
ACCEPTANCE_REQUIRED_FLAG,
ACCEPTANCE_WITHIN_DAYS,
SUPPLIER_NOTIF_METHOD,
FAX,
EMAIL_ADDRESS,
CONFIRMING_ORDER_FLAG,
NOTE_TO_VENDOR,
NOTE_TO_RECEIVER,
DEFAULT_TAXATION_COUNTRY_CODE,
TAX_DOCUMENT_SUBTYPE,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
AGENT_EMAIL_ADDRESS,
MODE_OF_TRANSPORT,
SERVICE_LEVEL,
FIRST_PTY_REG_NUM,
THIRD_PTY_REG_NUM,
BUYER_MANAGED_TRANSPORT_FLAG,
MASTER_CONTRACT_NUMBER,
MASTER_CONTRACT_TYPE,
CC_EMAIL_ADDRESS,
BCC_EMAIL_ADDRESS,
GLOBAL_ATTRIBUTE1,
GLOBAL_ATTRIBUTE2,
GLOBAL_ATTRIBUTE3,
GLOBAL_ATTRIBUTE4,
GLOBAL_ATTRIBUTE5,
GLOBAL_ATTRIBUTE6,
OVERRIDING_APPROVER_NAME,
SKIP_ELECTRONIC_COMM_FLAG,
CHECKLIST_TITLE,
CHECKLIST_NUM,
ALT_CONTACT_EMAIL_ADDRESS,
SPECIAL_HANDLING_TYPE,
SH_ATTRIBUTE1,
SH_ATTRIBUTE2,
SH_ATTRIBUTE3,
SH_ATTRIBUTE4,
SH_ATTRIBUTE5,
SH_ATTRIBUTE6,
SH_ATTRIBUTE7,
SH_ATTRIBUTE8,
SH_ATTRIBUTE9,
SH_ATTRIBUTE10,
SH_ATTRIBUTE11,
SH_ATTRIBUTE12,
SH_ATTRIBUTE13,
SH_ATTRIBUTE14,
SH_ATTRIBUTE15,
SH_ATTRIBUTE16,
SH_ATTRIBUTE17,
SH_ATTRIBUTE18,
SH_ATTRIBUTE19,
SH_ATTRIBUTE20,
SH_ATTRIBUTE_NUMBER1,
SH_ATTRIBUTE_NUMBER2,
SH_ATTRIBUTE_NUMBER3,
SH_ATTRIBUTE_NUMBER4,
SH_ATTRIBUTE_NUMBER5,
SH_ATTRIBUTE_NUMBER6,
SH_ATTRIBUTE_NUMBER7,
SH_ATTRIBUTE_NUMBER8,
SH_ATTRIBUTE_NUMBER9,
SH_ATTRIBUTE_NUMBER10,
SH_ATTRIBUTE_DATE1,
SH_ATTRIBUTE_DATE2,
SH_ATTRIBUTE_DATE3,
SH_ATTRIBUTE_DATE4,
SH_ATTRIBUTE_DATE5,
SH_ATTRIBUTE_DATE6,
SH_ATTRIBUTE_DATE7,
SH_ATTRIBUTE_DATE8,
SH_ATTRIBUTE_DATE9,
SH_ATTRIBUTE_DATE10,
SH_ATTRIBUTE_TIMESTAMP1,
SH_ATTRIBUTE_TIMESTAMP2,
SH_ATTRIBUTE_TIMESTAMP3,
SH_ATTRIBUTE_TIMESTAMP4,
SH_ATTRIBUTE_TIMESTAMP5,
SH_ATTRIBUTE_TIMESTAMP6,
SH_ATTRIBUTE_TIMESTAMP7,
SH_ATTRIBUTE_TIMESTAMP8,
SH_ATTRIBUTE_TIMESTAMP9,
SH_ATTRIBUTE_TIMESTAMP10,
Target_ShipTo_Location,
						FILE_NAME,
						ERROR_MESSAGE,
						IMPORT_STATUS,
						file_reference_identifier ,
                        SOURCE_SYSTEM ,
						EXECUTION_ID 
						)
						SELECT 
						INTERFACE_HEADER_KEY,
ACTION,
BATCH_ID,
IMPORT_SOURCE_CODE,
APPROVAL_ACTION,
DOCUMENT_NUM,
DOCUMENT_TYPE_CODE,
STYLE_DISPLAY_NAME,
PRC_BU_NAME,
REQ_BU_NAME,
SOLDTO_LE_NAME,
BILLTO_BU_NAME,
AGENT_NAME,
CURRENCY_CODE,
RATE,
RATE_TYPE,
RATE_DATE,
COMMENTS,
BILL_TO_LOCATION,
SHIP_TO_LOCATION,
VENDOR_NAME,
VENDOR_NUM,
SUPPLIER_SITE_CODE,
VENDOR_CONTACT,
VENDOR_DOC_NUM,
FOB,
FREIGHT_CARRIER,
FREIGHT_TERMS,
PAY_ON_CODE,
PAYMENT_TERMS,
ORIGINATOR_ROLE,
CHANGE_ORDER_DESC,
ACCEPTANCE_REQUIRED_FLAG,
ACCEPTANCE_WITHIN_DAYS,
SUPPLIER_NOTIF_METHOD,
FAX,
EMAIL_ADDRESS,
CONFIRMING_ORDER_FLAG,
NOTE_TO_VENDOR,
NOTE_TO_RECEIVER,
DEFAULT_TAXATION_COUNTRY_CODE,
TAX_DOCUMENT_SUBTYPE,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
AGENT_EMAIL_ADDRESS,
MODE_OF_TRANSPORT,
SERVICE_LEVEL,
FIRST_PTY_REG_NUM,
THIRD_PTY_REG_NUM,
BUYER_MANAGED_TRANSPORT_FLAG,
MASTER_CONTRACT_NUMBER,
MASTER_CONTRACT_TYPE,
CC_EMAIL_ADDRESS,
BCC_EMAIL_ADDRESS,
GLOBAL_ATTRIBUTE1,
GLOBAL_ATTRIBUTE2,
GLOBAL_ATTRIBUTE3,
GLOBAL_ATTRIBUTE4,
GLOBAL_ATTRIBUTE5,
GLOBAL_ATTRIBUTE6,
OVERRIDING_APPROVER_NAME,
SKIP_ELECTRONIC_COMM_FLAG,
CHECKLIST_TITLE,
CHECKLIST_NUM,
ALT_CONTACT_EMAIL_ADDRESS,
SPECIAL_HANDLING_TYPE,
SH_ATTRIBUTE1,
SH_ATTRIBUTE2,
SH_ATTRIBUTE3,
SH_ATTRIBUTE4,
SH_ATTRIBUTE5,
SH_ATTRIBUTE6,
SH_ATTRIBUTE7,
SH_ATTRIBUTE8,
SH_ATTRIBUTE9,
SH_ATTRIBUTE10,
SH_ATTRIBUTE11,
SH_ATTRIBUTE12,
SH_ATTRIBUTE13,
SH_ATTRIBUTE14,
SH_ATTRIBUTE15,
SH_ATTRIBUTE16,
SH_ATTRIBUTE17,
SH_ATTRIBUTE18,
SH_ATTRIBUTE19,
SH_ATTRIBUTE20,
SH_ATTRIBUTE_NUMBER1,
SH_ATTRIBUTE_NUMBER2,
SH_ATTRIBUTE_NUMBER3,
SH_ATTRIBUTE_NUMBER4,
SH_ATTRIBUTE_NUMBER5,
SH_ATTRIBUTE_NUMBER6,
SH_ATTRIBUTE_NUMBER7,
SH_ATTRIBUTE_NUMBER8,
SH_ATTRIBUTE_NUMBER9,
SH_ATTRIBUTE_NUMBER10,
SH_ATTRIBUTE_DATE1,
SH_ATTRIBUTE_DATE2,
SH_ATTRIBUTE_DATE3,
SH_ATTRIBUTE_DATE4,
SH_ATTRIBUTE_DATE5,
SH_ATTRIBUTE_DATE6,
SH_ATTRIBUTE_DATE7,
SH_ATTRIBUTE_DATE8,
SH_ATTRIBUTE_DATE9,
SH_ATTRIBUTE_DATE10,
SH_ATTRIBUTE_TIMESTAMP1,
SH_ATTRIBUTE_TIMESTAMP2,
SH_ATTRIBUTE_TIMESTAMP3,
SH_ATTRIBUTE_TIMESTAMP4,
SH_ATTRIBUTE_TIMESTAMP5,
SH_ATTRIBUTE_TIMESTAMP6,
SH_ATTRIBUTE_TIMESTAMP7,
SH_ATTRIBUTE_TIMESTAMP8,
SH_ATTRIBUTE_TIMESTAMP9,
SH_ATTRIBUTE_TIMESTAMP10,
null,
                        null,
                        null,
                        null,
						null,
						null,
						'
                                  || chr(39)
                                  || gv_execution_id
                                  || chr(39)
                                  || '
						FROM XXCNV_PO_C007_PO_HEADERS_EXT ';

                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in XXCNV_PO_C007_PO_HEADERS_STG: ' || SQL%rowcount);
            END IF;
    --TABLE2
            IF gv_oci_file_name_lines LIKE '%PoLinesInterfaceOrder.csv%' THEN
                dbms_output.put_line('Creating external table XXCNV_PO_C007_PO_LINES_EXT');
                dbms_output.put_line(' XXCNV_PO_C007_PO_LINES_EXT : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_lines);
                dbms_cloud.create_external_table(
                    table_name      => 'XXCNV_PO_C007_PO_LINES_EXT',
                    credential_name => 'OCI$RESOURCE_PRINCIPAL',
	-- file_uri_list   =>' https://objectstorage.us-phoenix-1.oraclecloud.com/n/axcepiuovkix/b/Non_Prod_Conversion/o/mock1/PurchaseOrders/1/SourceFBDI/PO_LINES_UNITTEST.csv',
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_lines,
                    format          =>
                            JSON_OBJECT(
                                'skipheaders' VALUE '1',
                                'type' VALUE 'csv',
                                'dateformat' VALUE 'yyyy/mm/dd',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'ignoremissingcolumns' VALUE 'true',
                                        'blankasnull' VALUE 'true',
                                'conversionerrors' VALUE 'store_null'
                            ),
                    column_list     => '
						      INTERFACE_LINE_KEY	VARCHAR2(50)	,
INTERFACE_HEADER_KEY	VARCHAR2(50)	,
ACTION	VARCHAR2(25)	,
LINE_NUM	NUMBER	,
LINE_TYPE	VARCHAR2(30)	,
ITEM	VARCHAR2(300)	,
ITEM_DESCRIPTION	VARCHAR2(500)	,
ITEM_REVISION	VARCHAR2(18)	,
CATEGORY	VARCHAR2(2000)	,
AMOUNT	NUMBER	,
QUANTITY	NUMBER	,
SHIPPING_UNIT_OF_MEASURE	VARCHAR2(25)	,
UNIT_PRICE	NUMBER	,
SECONDARY_QUANTITY	NUMBER	,
SECONDARY_UNIT_OF_MEASURE	VARCHAR2(18)	,
VENDOR_PRODUCT_NUM	VARCHAR2(25)	,
NEGOTIATED_BY_PREPARER_FLAG	VARCHAR2(1)	,
HAZARD_CLASS	VARCHAR2(40)	,
UN_NUMBER	VARCHAR2(25)	,
NOTE_TO_VENDOR	VARCHAR2(1000)	,
NOTE_TO_RECEIVER	VARCHAR2(1000)	,
ATTRIBUTE_CATEGORY	VARCHAR2(30)	,
LINE_ATTRIBUTE1	VARCHAR2(150)	,
LINE_ATTRIBUTE2	VARCHAR2(150)	,
LINE_ATTRIBUTE3	VARCHAR2(150)	,
LINE_ATTRIBUTE4	VARCHAR2(150)	,
LINE_ATTRIBUTE5	VARCHAR2(150)	,
LINE_ATTRIBUTE6	VARCHAR2(150)	,
LINE_ATTRIBUTE7	VARCHAR2(150)	,
LINE_ATTRIBUTE8	VARCHAR2(150)	,
LINE_ATTRIBUTE9	VARCHAR2(150)	,
LINE_ATTRIBUTE10	VARCHAR2(150)	,
LINE_ATTRIBUTE11	VARCHAR2(150)	,
LINE_ATTRIBUTE12	VARCHAR2(150)	,
LINE_ATTRIBUTE13	VARCHAR2(150)	,
LINE_ATTRIBUTE14	VARCHAR2(150)	,
LINE_ATTRIBUTE15	VARCHAR2(150)	,
LINE_ATTRIBUTE16	VARCHAR2(150)	,
LINE_ATTRIBUTE17	VARCHAR2(150)	,
LINE_ATTRIBUTE18	VARCHAR2(150)	,
LINE_ATTRIBUTE19	VARCHAR2(150)	,
LINE_ATTRIBUTE20	VARCHAR2(150)	,
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
ATTRIBUTE_NUMBER1	NUMBER(18)	,
ATTRIBUTE_NUMBER2	NUMBER(18)	,
ATTRIBUTE_NUMBER3	NUMBER(18)	,
ATTRIBUTE_NUMBER4	NUMBER(18)	,
ATTRIBUTE_NUMBER5	NUMBER(18)	,
ATTRIBUTE_NUMBER6	NUMBER(18)	,
ATTRIBUTE_NUMBER7	NUMBER(18)	,
ATTRIBUTE_NUMBER8	NUMBER(18)	,
ATTRIBUTE_NUMBER9	NUMBER(18)	,
ATTRIBUTE_NUMBER10	NUMBER(18)	,
ATTRIBUTE_TIMESTAMP1	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP2	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP3	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP4	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP5	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP6	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP7	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP8	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP9	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP10	TIMESTAMP(6)	,
UNIT_WEIGHT	NUMBER	,
WEIGHT_UOM_CODE	VARCHAR2(3)	,
WEIGHT_UNIT_OF_MEASURE	VARCHAR2(25)	,
UNIT_VOLUME	NUMBER	,
VOLUME_UOM_CODE	VARCHAR2(3)	,
VOLUME_UNIT_OF_MEASURE	VARCHAR2(25)	,
TEMPLATE_NAME	VARCHAR2(30)	,
ITEM_ATTRIBUTE_CATEGORY	VARCHAR2(30)	,
ITEM_ATTRIBUTE1	VARCHAR2(150)	,
ITEM_ATTRIBUTE2	VARCHAR2(150)	,
ITEM_ATTRIBUTE3	VARCHAR2(150)	,
ITEM_ATTRIBUTE4	VARCHAR2(150)	,
ITEM_ATTRIBUTE5	VARCHAR2(150)	,
ITEM_ATTRIBUTE6	VARCHAR2(150)	,
ITEM_ATTRIBUTE7	VARCHAR2(150)	,
ITEM_ATTRIBUTE8	VARCHAR2(150)	,
ITEM_ATTRIBUTE9	VARCHAR2(150)	,
ITEM_ATTRIBUTE10	VARCHAR2(150)	,
ITEM_ATTRIBUTE11	VARCHAR2(150)	,
ITEM_ATTRIBUTE12	VARCHAR2(150)	,
ITEM_ATTRIBUTE13	VARCHAR2(150)	,
ITEM_ATTRIBUTE14	VARCHAR2(150)	,
ITEM_ATTRIBUTE15	VARCHAR2(150)	,
SOURCE_AGREEMENT_PRC_BU_NAME	VARCHAR2(240)	,
SOURCE_AGREEMENT	VARCHAR2(30)	,
SOURCE_AGREEMENT_LINE	NUMBER	,
DISCOUNT_TYPE	VARCHAR2(25)	,
DISCOUNT	NUMBER	,
DISCOUNT_REASON	VARCHAR2(240)	,
MAX_RETAINAGE_AMOUNT	NUMBER	,
UNIT_OF_MEASURE	VARCHAR2(25)	,
SH_ATTRIBUTE1	VARCHAR2(150)	,
SH_ATTRIBUTE2	VARCHAR2(150)	,
SH_ATTRIBUTE3	VARCHAR2(150)	,
SH_ATTRIBUTE4	VARCHAR2(150)	,
SH_ATTRIBUTE5	VARCHAR2(150)	,
SH_ATTRIBUTE6	VARCHAR2(150)	,
SH_ATTRIBUTE7	VARCHAR2(150)	,
SH_ATTRIBUTE8	VARCHAR2(150)	,
SH_ATTRIBUTE9	VARCHAR2(150)	,
SH_ATTRIBUTE10	VARCHAR2(150)	,
SH_ATTRIBUTE11	VARCHAR2(150)	,
SH_ATTRIBUTE12	VARCHAR2(150)	,
SH_ATTRIBUTE13	VARCHAR2(150)	,
SH_ATTRIBUTE14	VARCHAR2(150)	,
SH_ATTRIBUTE15	VARCHAR2(150)	,
SH_ATTRIBUTE16	VARCHAR2(150)	,
SH_ATTRIBUTE17	VARCHAR2(150)	,
SH_ATTRIBUTE18	VARCHAR2(150)	,
SH_ATTRIBUTE19	VARCHAR2(150)	,
SH_ATTRIBUTE20	VARCHAR2(150)	,
SH_ATTRIBUTE_NUMBER1	NUMBER	,
SH_ATTRIBUTE_NUMBER2	NUMBER	,
SH_ATTRIBUTE_NUMBER3	NUMBER	,
SH_ATTRIBUTE_NUMBER4	NUMBER	,
SH_ATTRIBUTE_NUMBER5	NUMBER	,
SH_ATTRIBUTE_NUMBER6	NUMBER	,
SH_ATTRIBUTE_NUMBER7	NUMBER	,
SH_ATTRIBUTE_NUMBER8	NUMBER	,
SH_ATTRIBUTE_NUMBER9	NUMBER	,
SH_ATTRIBUTE_NUMBER10	NUMBER	,
SH_ATTRIBUTE_DATE1	DATE	,
SH_ATTRIBUTE_DATE2	DATE	,
SH_ATTRIBUTE_DATE3	DATE	,
SH_ATTRIBUTE_DATE4	DATE	,
SH_ATTRIBUTE_DATE5	DATE	,
SH_ATTRIBUTE_DATE6	DATE	,
SH_ATTRIBUTE_DATE7	DATE	,
SH_ATTRIBUTE_DATE8	DATE	,
SH_ATTRIBUTE_DATE9	DATE	,
SH_ATTRIBUTE_DATE10	DATE	,
SH_ATTRIBUTE_TIMESTAMP1	TIMESTAMP(6)	,
SH_ATTRIBUTE_TIMESTAMP2	TIMESTAMP(6)	,
SH_ATTRIBUTE_TIMESTAMP3	TIMESTAMP(6)	,
SH_ATTRIBUTE_TIMESTAMP4	TIMESTAMP(6)	,
SH_ATTRIBUTE_TIMESTAMP5	TIMESTAMP(6)	,
SH_ATTRIBUTE_TIMESTAMP6	TIMESTAMP(6)	,
SH_ATTRIBUTE_TIMESTAMP7	TIMESTAMP(6)	,
SH_ATTRIBUTE_TIMESTAMP8	TIMESTAMP(6)	,
SH_ATTRIBUTE_TIMESTAMP9	TIMESTAMP(6)	,
SH_ATTRIBUTE_TIMESTAMP10	TIMESTAMP(6)	


						'
                );

                dbms_output.put_line(' external table XXCNV_PO_C007_PO_LINES_EXT is created');
                EXECUTE IMMEDIATE 'INSERT INTO XXCNV_PO_C007_PO_LINES_STG (
INTERFACE_LINE_KEY,
INTERFACE_HEADER_KEY,
ACTION,
LINE_NUM,
LINE_TYPE,
ITEM,
ITEM_DESCRIPTION,
ITEM_REVISION,
CATEGORY,
AMOUNT,
QUANTITY,
SHIPPING_UNIT_OF_MEASURE,
UNIT_PRICE,
SECONDARY_QUANTITY,
SECONDARY_UNIT_OF_MEASURE,
VENDOR_PRODUCT_NUM,
NEGOTIATED_BY_PREPARER_FLAG,
HAZARD_CLASS,
UN_NUMBER,
NOTE_TO_VENDOR,
NOTE_TO_RECEIVER,
ATTRIBUTE_CATEGORY,
LINE_ATTRIBUTE1,
LINE_ATTRIBUTE2,
LINE_ATTRIBUTE3,
LINE_ATTRIBUTE4,
LINE_ATTRIBUTE5,
LINE_ATTRIBUTE6,
LINE_ATTRIBUTE7,
LINE_ATTRIBUTE8,
LINE_ATTRIBUTE9,
LINE_ATTRIBUTE10,
LINE_ATTRIBUTE11,
LINE_ATTRIBUTE12,
LINE_ATTRIBUTE13,
LINE_ATTRIBUTE14,
LINE_ATTRIBUTE15,
LINE_ATTRIBUTE16,
LINE_ATTRIBUTE17,
LINE_ATTRIBUTE18,
LINE_ATTRIBUTE19,
LINE_ATTRIBUTE20,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
UNIT_WEIGHT,
WEIGHT_UOM_CODE,
WEIGHT_UNIT_OF_MEASURE,
UNIT_VOLUME,
VOLUME_UOM_CODE,
VOLUME_UNIT_OF_MEASURE,
TEMPLATE_NAME,
ITEM_ATTRIBUTE_CATEGORY,
ITEM_ATTRIBUTE1,
ITEM_ATTRIBUTE2,
ITEM_ATTRIBUTE3,
ITEM_ATTRIBUTE4,
ITEM_ATTRIBUTE5,
ITEM_ATTRIBUTE6,
ITEM_ATTRIBUTE7,
ITEM_ATTRIBUTE8,
ITEM_ATTRIBUTE9,
ITEM_ATTRIBUTE10,
ITEM_ATTRIBUTE11,
ITEM_ATTRIBUTE12,
ITEM_ATTRIBUTE13,
ITEM_ATTRIBUTE14,
ITEM_ATTRIBUTE15,
SOURCE_AGREEMENT_PRC_BU_NAME,
SOURCE_AGREEMENT,
SOURCE_AGREEMENT_LINE,
DISCOUNT_TYPE,
DISCOUNT,
DISCOUNT_REASON,
MAX_RETAINAGE_AMOUNT,
UNIT_OF_MEASURE,
SH_ATTRIBUTE1,
SH_ATTRIBUTE2,
SH_ATTRIBUTE3,
SH_ATTRIBUTE4,
SH_ATTRIBUTE5,
SH_ATTRIBUTE6,
SH_ATTRIBUTE7,
SH_ATTRIBUTE8,
SH_ATTRIBUTE9,
SH_ATTRIBUTE10,
SH_ATTRIBUTE11,
SH_ATTRIBUTE12,
SH_ATTRIBUTE13,
SH_ATTRIBUTE14,
SH_ATTRIBUTE15,
SH_ATTRIBUTE16,
SH_ATTRIBUTE17,
SH_ATTRIBUTE18,
SH_ATTRIBUTE19,
SH_ATTRIBUTE20,
SH_ATTRIBUTE_NUMBER1,
SH_ATTRIBUTE_NUMBER2,
SH_ATTRIBUTE_NUMBER3,
SH_ATTRIBUTE_NUMBER4,
SH_ATTRIBUTE_NUMBER5,
SH_ATTRIBUTE_NUMBER6,
SH_ATTRIBUTE_NUMBER7,
SH_ATTRIBUTE_NUMBER8,
SH_ATTRIBUTE_NUMBER9,
SH_ATTRIBUTE_NUMBER10,
SH_ATTRIBUTE_DATE1,
SH_ATTRIBUTE_DATE2,
SH_ATTRIBUTE_DATE3,
SH_ATTRIBUTE_DATE4,
SH_ATTRIBUTE_DATE5,
SH_ATTRIBUTE_DATE6,
SH_ATTRIBUTE_DATE7,
SH_ATTRIBUTE_DATE8,
SH_ATTRIBUTE_DATE9,
SH_ATTRIBUTE_DATE10,
SH_ATTRIBUTE_TIMESTAMP1,
SH_ATTRIBUTE_TIMESTAMP2,
SH_ATTRIBUTE_TIMESTAMP3,
SH_ATTRIBUTE_TIMESTAMP4,
SH_ATTRIBUTE_TIMESTAMP5,
SH_ATTRIBUTE_TIMESTAMP6,
SH_ATTRIBUTE_TIMESTAMP7,
SH_ATTRIBUTE_TIMESTAMP8,
SH_ATTRIBUTE_TIMESTAMP9,
SH_ATTRIBUTE_TIMESTAMP10,
Target_Attribute1,
Target_Attribute2,
Target_Attribute3,

						FILE_NAME,
						ERROR_MESSAGE,
						IMPORT_STATUS,
						file_reference_identifier ,
                        SOURCE_SYSTEM,
						EXECUTION_ID 
						)
						SELECT 
						INTERFACE_LINE_KEY,
INTERFACE_HEADER_KEY,
ACTION,
LINE_NUM,
LINE_TYPE,
ITEM,
ITEM_DESCRIPTION,
ITEM_REVISION,
CATEGORY,
AMOUNT,
QUANTITY,
SHIPPING_UNIT_OF_MEASURE,
UNIT_PRICE,
SECONDARY_QUANTITY,
SECONDARY_UNIT_OF_MEASURE,
VENDOR_PRODUCT_NUM,
NEGOTIATED_BY_PREPARER_FLAG,
HAZARD_CLASS,
UN_NUMBER,
NOTE_TO_VENDOR,
NOTE_TO_RECEIVER,
ATTRIBUTE_CATEGORY,
LINE_ATTRIBUTE1,
LINE_ATTRIBUTE2,
LINE_ATTRIBUTE3,
LINE_ATTRIBUTE4,
LINE_ATTRIBUTE5,
LINE_ATTRIBUTE6,
LINE_ATTRIBUTE7,
LINE_ATTRIBUTE8,
LINE_ATTRIBUTE9,
LINE_ATTRIBUTE10,
LINE_ATTRIBUTE11,
LINE_ATTRIBUTE12,
LINE_ATTRIBUTE13,
LINE_ATTRIBUTE14,
LINE_ATTRIBUTE15,
LINE_ATTRIBUTE16,
LINE_ATTRIBUTE17,
LINE_ATTRIBUTE18,
LINE_ATTRIBUTE19,
LINE_ATTRIBUTE20,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
UNIT_WEIGHT,
WEIGHT_UOM_CODE,
WEIGHT_UNIT_OF_MEASURE,
UNIT_VOLUME,
VOLUME_UOM_CODE,
VOLUME_UNIT_OF_MEASURE,
TEMPLATE_NAME,
ITEM_ATTRIBUTE_CATEGORY,
ITEM_ATTRIBUTE1,
ITEM_ATTRIBUTE2,
ITEM_ATTRIBUTE3,
ITEM_ATTRIBUTE4,
ITEM_ATTRIBUTE5,
ITEM_ATTRIBUTE6,
ITEM_ATTRIBUTE7,
ITEM_ATTRIBUTE8,
ITEM_ATTRIBUTE9,
ITEM_ATTRIBUTE10,
ITEM_ATTRIBUTE11,
ITEM_ATTRIBUTE12,
ITEM_ATTRIBUTE13,
ITEM_ATTRIBUTE14,
ITEM_ATTRIBUTE15,
SOURCE_AGREEMENT_PRC_BU_NAME,
SOURCE_AGREEMENT,
SOURCE_AGREEMENT_LINE,
DISCOUNT_TYPE,
DISCOUNT,
DISCOUNT_REASON,
MAX_RETAINAGE_AMOUNT,
UNIT_OF_MEASURE,
SH_ATTRIBUTE1,
SH_ATTRIBUTE2,
SH_ATTRIBUTE3,
SH_ATTRIBUTE4,
SH_ATTRIBUTE5,
SH_ATTRIBUTE6,
SH_ATTRIBUTE7,
SH_ATTRIBUTE8,
SH_ATTRIBUTE9,
SH_ATTRIBUTE10,
SH_ATTRIBUTE11,
SH_ATTRIBUTE12,
SH_ATTRIBUTE13,
SH_ATTRIBUTE14,
SH_ATTRIBUTE15,
SH_ATTRIBUTE16,
SH_ATTRIBUTE17,
SH_ATTRIBUTE18,
SH_ATTRIBUTE19,
SH_ATTRIBUTE20,
SH_ATTRIBUTE_NUMBER1,
SH_ATTRIBUTE_NUMBER2,
SH_ATTRIBUTE_NUMBER3,
SH_ATTRIBUTE_NUMBER4,
SH_ATTRIBUTE_NUMBER5,
SH_ATTRIBUTE_NUMBER6,
SH_ATTRIBUTE_NUMBER7,
SH_ATTRIBUTE_NUMBER8,
SH_ATTRIBUTE_NUMBER9,
SH_ATTRIBUTE_NUMBER10,
SH_ATTRIBUTE_DATE1,
SH_ATTRIBUTE_DATE2,
SH_ATTRIBUTE_DATE3,
SH_ATTRIBUTE_DATE4,
SH_ATTRIBUTE_DATE5,
SH_ATTRIBUTE_DATE6,
SH_ATTRIBUTE_DATE7,
SH_ATTRIBUTE_DATE8,
SH_ATTRIBUTE_DATE9,
SH_ATTRIBUTE_DATE10,
SH_ATTRIBUTE_TIMESTAMP1,
SH_ATTRIBUTE_TIMESTAMP2,
SH_ATTRIBUTE_TIMESTAMP3,
SH_ATTRIBUTE_TIMESTAMP4,
SH_ATTRIBUTE_TIMESTAMP5,
SH_ATTRIBUTE_TIMESTAMP6,
SH_ATTRIBUTE_TIMESTAMP7,
SH_ATTRIBUTE_TIMESTAMP8,
SH_ATTRIBUTE_TIMESTAMP9,
SH_ATTRIBUTE_TIMESTAMP10,
null,
null,
null,
                        null,
                        null,
                        null,
						null,
						null,
						'
                                  || chr(39)
                                  || gv_execution_id
                                  || chr(39)
                                  || '
						FROM XXCNV_PO_C007_PO_LINES_EXT ';

                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in XXCNV_PO_C007_PO_LINES_STG: ' || SQL%rowcount);
            END IF;
--TABLE3
            IF gv_oci_file_name_line_locations LIKE '%PoLineLocationsInterfaceOrder.csv%' THEN
                dbms_output.put_line('Creating external table XXCNV_PO_C007_PO_LINE_LOCATIONS_EXT');
                dbms_output.put_line(' XXCNV_PO_C007_PO_LINE_LOCATIONS_EXT : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_line_locations);
                dbms_cloud.create_external_table(
                    table_name      => 'XXCNV_PO_C007_PO_LINE_LOCATIONS_EXT',
                    credential_name => 'OCI$RESOURCE_PRINCIPAL',
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_line_locations,
		  -- file_uri_list =>  'https://objectstorage.us-ashburn-1.oraclecloud.com/n/nacaus19b/b/O2InnovationBucket/o/mock1/Item/1/SourceFBDI/EgpItemCategoriesInterface.csv',
	--	 format => json_object('type' VALUE 'csv','rejectlimit' value 'UNLIMITED','blankasnull' value 'true' , 'dateformat' value 'yyyy/mm/dd'), 
                    format          =>
                            JSON_OBJECT(
                                'skipheaders' VALUE '1',
                                'type' VALUE 'csv',
                                'dateformat' VALUE 'yyyy/mm/dd',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'ignoremissingcolumns' VALUE 'true',
                                        'blankasnull' VALUE 'true',
                                'conversionerrors' VALUE 'store_null'
                            ),
                    column_list     => 'INTERFACE_LINE_LOCATION_KEY	VARCHAR2(50)	,
INTERFACE_LINE_KEY	VARCHAR2(50)	,
SHIPMENT_NUM	NUMBER	,
SHIP_TO_LOCATION	VARCHAR2(60)	,
SHIP_TO_ORGANIZATION_CODE	VARCHAR2(18)	,
AMOUNT	NUMBER	,
SHIPPING_UOM_QUANTITY	NUMBER	,
NEED_BY_DATE	DATE	,
PROMISED_DATE	DATE	,
SECONDARY_QUANTITY	NUMBER	,
SECONDARY_UNIT_OF_MEASURE	VARCHAR2(18)	,
DESTINATION_TYPE_CODE	VARCHAR2(25)	,
ACCRUE_ON_RECEIPT_FLAG	VARCHAR2(1)	,
ALLOW_SUBSTITUTE_RECEIPTS_FLAG	VARCHAR2(1)	,
ASSESSABLE_VALUE	NUMBER	,
DAYS_EARLY_RECEIPT_ALLOWED	NUMBER	,
DAYS_LATE_RECEIPT_ALLOWED	NUMBER	,
ENFORCE_SHIP_TO_LOCATION_CODE	VARCHAR2(25)	,
INSPECTION_REQUIRED_FLAG	VARCHAR2(1)	,
RECEIPT_REQUIRED_FLAG	VARCHAR2(1)	,
INVOICE_CLOSE_TOLERANCE	NUMBER	,
RECEIVE_CLOSE_TOLERANCE	NUMBER	,
QTY_RCV_TOLERANCE	NUMBER	,
QTY_RCV_EXCEPTION_CODE	VARCHAR2(25)	,
RECEIPT_DAYS_EXCEPTION_CODE	VARCHAR2(25)	,
RECEIVING_ROUTING	VARCHAR2(30)	,
NOTE_TO_RECEIVER	VARCHAR2(1000)	,
INPUT_TAX_CLASSIFICATION_CODE	VARCHAR2(30)	,
LINE_INTENDED_USE	VARCHAR2(240)	,
PRODUCT_CATEGORY	VARCHAR2(240)	,
PRODUCT_FISC_CLASSIFICATION	VARCHAR2(240)	,
PRODUCT_TYPE	VARCHAR2(240)	,
TRX_BUSINESS_CATEGORY	VARCHAR2(240)	,
USER_DEFINED_FISC_CLASS	VARCHAR2(30)	,
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
ATTRIBUTE_NUMBER1	NUMBER(18)	,
ATTRIBUTE_NUMBER2	NUMBER(18)	,
ATTRIBUTE_NUMBER3	NUMBER(18)	,
ATTRIBUTE_NUMBER4	NUMBER(18)	,
ATTRIBUTE_NUMBER5	NUMBER(18)	,
ATTRIBUTE_NUMBER6	NUMBER(18)	,
ATTRIBUTE_NUMBER7	NUMBER(18)	,
ATTRIBUTE_NUMBER8	NUMBER(18)	,
ATTRIBUTE_NUMBER9	NUMBER(18)	,
ATTRIBUTE_NUMBER10	NUMBER(18)	,
ATTRIBUTE_TIMESTAMP1	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP2	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP3	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP4	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP5	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP6	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP7	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP8	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP9	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP10	TIMESTAMP(6)	,
FRIGHT_CARRIER	VARCHAR2(360)	,
MODE_OF_TRANSPORT	VARCHAR2(80)	,
SERVICE_LEVEL	VARCHAR2(80)	,
FINAL_DISCHARGE_LOCATION_CODE	VARCHAR2(60)	,
REQUESTED_SHIP_DATE	DATE	,
PROMISED_SHIP_DATE	DATE	,
REQUESTED_DELIVERY_DATE	DATE	,
PROMISED_DELIVERY_DATE	DATE	,
RETAINAGE_RATE	NUMBER	,
INVOICE_MATCH_OPTION	VARCHAR2(25)

'
                );

                dbms_output.put_line(' external table XXCNV_PO_C007_PO_LINE_LOCATIONS_EXT is created');
                EXECUTE IMMEDIATE 'INSERT INTO XXCNV_PO_C007_PO_LINE_LOCATIONS_STG (
INTERFACE_LINE_LOCATION_KEY,
INTERFACE_LINE_KEY,
SHIPMENT_NUM,
SHIP_TO_LOCATION,
SHIP_TO_ORGANIZATION_CODE,
AMOUNT,
SHIPPING_UOM_QUANTITY,
NEED_BY_DATE,
PROMISED_DATE,
SECONDARY_QUANTITY,
SECONDARY_UNIT_OF_MEASURE,
DESTINATION_TYPE_CODE,
ACCRUE_ON_RECEIPT_FLAG,
ALLOW_SUBSTITUTE_RECEIPTS_FLAG,
ASSESSABLE_VALUE,
DAYS_EARLY_RECEIPT_ALLOWED,
DAYS_LATE_RECEIPT_ALLOWED,
ENFORCE_SHIP_TO_LOCATION_CODE,
INSPECTION_REQUIRED_FLAG,
RECEIPT_REQUIRED_FLAG,
INVOICE_CLOSE_TOLERANCE,
RECEIVE_CLOSE_TOLERANCE,
QTY_RCV_TOLERANCE,
QTY_RCV_EXCEPTION_CODE,
RECEIPT_DAYS_EXCEPTION_CODE,
RECEIVING_ROUTING,
NOTE_TO_RECEIVER,
INPUT_TAX_CLASSIFICATION_CODE,
LINE_INTENDED_USE,
PRODUCT_CATEGORY,
PRODUCT_FISC_CLASSIFICATION,
PRODUCT_TYPE,
TRX_BUSINESS_CATEGORY,
USER_DEFINED_FISC_CLASS,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
FRIGHT_CARRIER,
MODE_OF_TRANSPORT,
SERVICE_LEVEL,
FINAL_DISCHARGE_LOCATION_CODE,
REQUESTED_SHIP_DATE,
PROMISED_SHIP_DATE,
REQUESTED_DELIVERY_DATE,
PROMISED_DELIVERY_DATE,
RETAINAGE_RATE,
INVOICE_MATCH_OPTION,
						FILE_NAME,
						ERROR_MESSAGE,
						IMPORT_STATUS,
						file_reference_identifier ,
                        SOURCE_SYSTEM ,
						EXECUTION_ID
						)
						SELECT 
					INTERFACE_LINE_LOCATION_KEY,
INTERFACE_LINE_KEY,
SHIPMENT_NUM,
SHIP_TO_LOCATION,
SHIP_TO_ORGANIZATION_CODE,
AMOUNT,
SHIPPING_UOM_QUANTITY,
NEED_BY_DATE,
PROMISED_DATE,
SECONDARY_QUANTITY,
SECONDARY_UNIT_OF_MEASURE,
DESTINATION_TYPE_CODE,
ACCRUE_ON_RECEIPT_FLAG,
ALLOW_SUBSTITUTE_RECEIPTS_FLAG,
ASSESSABLE_VALUE,
DAYS_EARLY_RECEIPT_ALLOWED,
DAYS_LATE_RECEIPT_ALLOWED,
ENFORCE_SHIP_TO_LOCATION_CODE,
INSPECTION_REQUIRED_FLAG,
RECEIPT_REQUIRED_FLAG,
INVOICE_CLOSE_TOLERANCE,
RECEIVE_CLOSE_TOLERANCE,
QTY_RCV_TOLERANCE,
QTY_RCV_EXCEPTION_CODE,
RECEIPT_DAYS_EXCEPTION_CODE,
RECEIVING_ROUTING,
NOTE_TO_RECEIVER,
INPUT_TAX_CLASSIFICATION_CODE,
LINE_INTENDED_USE,
PRODUCT_CATEGORY,
PRODUCT_FISC_CLASSIFICATION,
PRODUCT_TYPE,
TRX_BUSINESS_CATEGORY,
USER_DEFINED_FISC_CLASS,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
FRIGHT_CARRIER,
MODE_OF_TRANSPORT,
SERVICE_LEVEL,
FINAL_DISCHARGE_LOCATION_CODE,
REQUESTED_SHIP_DATE,
PROMISED_SHIP_DATE,
REQUESTED_DELIVERY_DATE,
PROMISED_DELIVERY_DATE,
RETAINAGE_RATE,
INVOICE_MATCH_OPTION,
null,
                        null,
                        null,
						null,
						null,
						'
                                  || chr(39)
                                  || gv_execution_id
                                  || chr(39)
                                  || '
						FROM XXCNV_PO_C007_PO_LINE_LOCATIONS_EXT ';

                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in XXCNV_PO_C007_PO_LINE_LOCATIONS_STG: ' || SQL%rowcount);
            END IF;

--TABLE4
            IF gv_oci_file_name_distributions LIKE '%PoDistributionsInterfaceOrder.csv%' THEN
                dbms_output.put_line('Creating external table XXCNV_PO_C007_PO_DISTRIBUTIONS_EXT');
                dbms_output.put_line(' XXCNV_PO_C007_PO_DISTRIBUTIONS_EXT : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_distributions);
                dbms_cloud.create_external_table(
                    table_name      => 'XXCNV_PO_C007_PO_DISTRIBUTIONS_EXT',
                    credential_name => 'OCI$RESOURCE_PRINCIPAL',
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_distributions,
                -- file_uri_list => 'https://objectstorage.us-ashburn-1.oraclecloud.com/n/nacaus19b/b/O2InnovationBucket/o/mock1/Item/1/SourceFBDI/EgoItemAssociationsIntf.csv',
             --   format => json_object('type' VALUE 'csv', 'rejectlimit' VALUE 'UNLIMITED', 'dateformat' VALUE 'yyyy/mm/dd','blankasnull' value 'true' , 'dateformat' value 'yyyy/mm/dd'), 
                    format          =>
                            JSON_OBJECT(
                                'skipheaders' VALUE '1',
                                'type' VALUE 'csv',
                                'dateformat' VALUE 'yyyy/mm/dd',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'ignoremissingcolumns' VALUE 'true',
                                        'blankasnull' VALUE 'true',
                                'conversionerrors' VALUE 'store_null'
                            ),
                    column_list     => 'INTERFACE_DISTRIBUTION_KEY	VARCHAR2(50)	,
INTERFACE_LINE_LOCATION_KEY	VARCHAR2(50)	,
DISTRIBUTION_NUM	NUMBER	,
DELIVER_TO_LOCATION	VARCHAR2(60)	,
DELIVER_TO_PERSON_FULL_NAME	VARCHAR2(2000)	,
DESTINATION_SUBINVENTORY	VARCHAR2(10)	,
AMOUNT_ORDERED	NUMBER	,
SHIPPING_UOM_QUANTITY	NUMBER	,
CHARGE_ACCOUNT_SEGMENT1	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT2	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT3	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT4	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT5	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT6	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT7	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT8	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT9	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT10	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT11	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT12	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT13	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT14	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT15	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT16	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT17	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT18	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT19	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT20	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT21	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT22	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT23	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT24	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT25	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT26	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT27	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT28	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT29	VARCHAR2(25)	,
CHARGE_ACCOUNT_SEGMENT30	VARCHAR2(25)	,
DESTINATION_CONTEXT	VARCHAR2(30)	,
PROJECT	VARCHAR2(240)	,
TASK	VARCHAR2(100)	,
PJC_EXPENDITURE_ITEM_DATE	DATE	,
EXPENDITURE_TYPE	VARCHAR2(240)	,
EXPENDITURE_ORGANIZATION	VARCHAR2(240)	,
PJC_BILLABLE_FLAG	VARCHAR2(1)	,
PJC_CAPITALIZABLE_FLAG	VARCHAR2(1)	,
PJC_WORK_TYPE	VARCHAR2(240)	,
PJC_RESERVED_ATTRIBUTE1	VARCHAR2(150)	,
PJC_RESERVED_ATTRIBUTE2	VARCHAR2(150)	,
PJC_RESERVED_ATTRIBUTE3	VARCHAR2(150)	,
PJC_RESERVED_ATTRIBUTE4	VARCHAR2(150)	,
PJC_RESERVED_ATTRIBUTE5	VARCHAR2(150)	,
PJC_RESERVED_ATTRIBUTE6	VARCHAR2(150)	,
PJC_RESERVED_ATTRIBUTE7	VARCHAR2(150)	,
PJC_RESERVED_ATTRIBUTE8	VARCHAR2(150)	,
PJC_RESERVED_ATTRIBUTE9	VARCHAR2(150)	,
PJC_RESERVED_ATTRIBUTE10	VARCHAR2(150)	,
PJC_USER_DEF_ATTRIBUTE1	VARCHAR2(150)	,
PJC_USER_DEF_ATTRIBUTE2	VARCHAR2(150)	,
PJC_USER_DEF_ATTRIBUTE3	VARCHAR2(150)	,
PJC_USER_DEF_ATTRIBUTE4	VARCHAR2(150)	,
PJC_USER_DEF_ATTRIBUTE5	VARCHAR2(150)	,
PJC_USER_DEF_ATTRIBUTE6	VARCHAR2(150)	,
PJC_USER_DEF_ATTRIBUTE7	VARCHAR2(150)	,
PJC_USER_DEF_ATTRIBUTE8	VARCHAR2(150)	,
PJC_USER_DEF_ATTRIBUTE9	VARCHAR2(150)	,
PJC_USER_DEF_ATTRIBUTE10	VARCHAR2(150)	,
RATE	NUMBER	,
RATE_DATE	DATE	,
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
ATTRIBUTE_NUMBER1	NUMBER(18)	,
ATTRIBUTE_NUMBER2	NUMBER(18)	,
ATTRIBUTE_NUMBER3	NUMBER(18)	,
ATTRIBUTE_NUMBER4	NUMBER(18)	,
ATTRIBUTE_NUMBER5	NUMBER(18)	,
ATTRIBUTE_NUMBER6	NUMBER(18)	,
ATTRIBUTE_NUMBER7	NUMBER(18)	,
ATTRIBUTE_NUMBER8	NUMBER(18)	,
ATTRIBUTE_NUMBER9	NUMBER(18)	,
ATTRIBUTE_NUMBER10	NUMBER(18)	,
ATTRIBUTE_TIMESTAMP1	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP2	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP3	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP4	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP5	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP6	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP7	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP8	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP9	TIMESTAMP(6)	,
ATTRIBUTE_TIMESTAMP10	TIMESTAMP(6)	,
DELIVER_TO_PERSON_EMAIL_ADDR	VARCHAR2(240)	,
BUDGET_DATE	DATE	,
PJC_CONTRACT_NUMBER	VARCHAR2(120)	,
PJC_FUNDING_SOURCE	VARCHAR2(360)	,
GLOBAL_ATTRIBUTE1	VARCHAR2(150)	'
                );

                EXECUTE IMMEDIATE 'INSERT INTO XXCNV_PO_C007_PO_DISTRIBUTIONS_STG (
INTERFACE_DISTRIBUTION_KEY,
INTERFACE_LINE_LOCATION_KEY,
DISTRIBUTION_NUM,
DELIVER_TO_LOCATION,
DELIVER_TO_PERSON_FULL_NAME,
DESTINATION_SUBINVENTORY,
AMOUNT_ORDERED,
SHIPPING_UOM_QUANTITY,
CHARGE_ACCOUNT_SEGMENT1,
CHARGE_ACCOUNT_SEGMENT2,
CHARGE_ACCOUNT_SEGMENT3,
CHARGE_ACCOUNT_SEGMENT4,
CHARGE_ACCOUNT_SEGMENT5,
CHARGE_ACCOUNT_SEGMENT6,
CHARGE_ACCOUNT_SEGMENT7,
CHARGE_ACCOUNT_SEGMENT8,
CHARGE_ACCOUNT_SEGMENT9,
CHARGE_ACCOUNT_SEGMENT10,
CHARGE_ACCOUNT_SEGMENT11,
CHARGE_ACCOUNT_SEGMENT12,
CHARGE_ACCOUNT_SEGMENT13,
CHARGE_ACCOUNT_SEGMENT14,
CHARGE_ACCOUNT_SEGMENT15,
CHARGE_ACCOUNT_SEGMENT16,
CHARGE_ACCOUNT_SEGMENT17,
CHARGE_ACCOUNT_SEGMENT18,
CHARGE_ACCOUNT_SEGMENT19,
CHARGE_ACCOUNT_SEGMENT20,
CHARGE_ACCOUNT_SEGMENT21,
CHARGE_ACCOUNT_SEGMENT22,
CHARGE_ACCOUNT_SEGMENT23,
CHARGE_ACCOUNT_SEGMENT24,
CHARGE_ACCOUNT_SEGMENT25,
CHARGE_ACCOUNT_SEGMENT26,
CHARGE_ACCOUNT_SEGMENT27,
CHARGE_ACCOUNT_SEGMENT28,
CHARGE_ACCOUNT_SEGMENT29,
CHARGE_ACCOUNT_SEGMENT30,
DESTINATION_CONTEXT,
PROJECT,
TASK,
PJC_EXPENDITURE_ITEM_DATE,
EXPENDITURE_TYPE,
EXPENDITURE_ORGANIZATION,
PJC_BILLABLE_FLAG,
PJC_CAPITALIZABLE_FLAG,
PJC_WORK_TYPE,
PJC_RESERVED_ATTRIBUTE1,
PJC_RESERVED_ATTRIBUTE2,
PJC_RESERVED_ATTRIBUTE3,
PJC_RESERVED_ATTRIBUTE4,
PJC_RESERVED_ATTRIBUTE5,
PJC_RESERVED_ATTRIBUTE6,
PJC_RESERVED_ATTRIBUTE7,
PJC_RESERVED_ATTRIBUTE8,
PJC_RESERVED_ATTRIBUTE9,
PJC_RESERVED_ATTRIBUTE10,
PJC_USER_DEF_ATTRIBUTE1,
PJC_USER_DEF_ATTRIBUTE2,
PJC_USER_DEF_ATTRIBUTE3,
PJC_USER_DEF_ATTRIBUTE4,
PJC_USER_DEF_ATTRIBUTE5,
PJC_USER_DEF_ATTRIBUTE6,
PJC_USER_DEF_ATTRIBUTE7,
PJC_USER_DEF_ATTRIBUTE8,
PJC_USER_DEF_ATTRIBUTE9,
PJC_USER_DEF_ATTRIBUTE10,
RATE,
RATE_DATE,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
DELIVER_TO_PERSON_EMAIL_ADDR,
BUDGET_DATE,
PJC_CONTRACT_NUMBER,
PJC_FUNDING_SOURCE,
GLOBAL_ATTRIBUTE1,
FILE_NAME,
						ERROR_MESSAGE,
						IMPORT_STATUS,
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
						file_reference_identifier ,
                        SOURCE_SYSTEM,
						EXECUTION_ID						
							) 
                    SELECT 
					        INTERFACE_DISTRIBUTION_KEY,
INTERFACE_LINE_LOCATION_KEY,
DISTRIBUTION_NUM,
DELIVER_TO_LOCATION,
DELIVER_TO_PERSON_FULL_NAME,
DESTINATION_SUBINVENTORY,
AMOUNT_ORDERED,
SHIPPING_UOM_QUANTITY,
CHARGE_ACCOUNT_SEGMENT1,
CHARGE_ACCOUNT_SEGMENT2,
CHARGE_ACCOUNT_SEGMENT3,
CHARGE_ACCOUNT_SEGMENT4,
CHARGE_ACCOUNT_SEGMENT5,
CHARGE_ACCOUNT_SEGMENT6,
CHARGE_ACCOUNT_SEGMENT7,
CHARGE_ACCOUNT_SEGMENT8,
CHARGE_ACCOUNT_SEGMENT9,
CHARGE_ACCOUNT_SEGMENT10,
CHARGE_ACCOUNT_SEGMENT11,
CHARGE_ACCOUNT_SEGMENT12,
CHARGE_ACCOUNT_SEGMENT13,
CHARGE_ACCOUNT_SEGMENT14,
CHARGE_ACCOUNT_SEGMENT15,
CHARGE_ACCOUNT_SEGMENT16,
CHARGE_ACCOUNT_SEGMENT17,
CHARGE_ACCOUNT_SEGMENT18,
CHARGE_ACCOUNT_SEGMENT19,
CHARGE_ACCOUNT_SEGMENT20,
CHARGE_ACCOUNT_SEGMENT21,
CHARGE_ACCOUNT_SEGMENT22,
CHARGE_ACCOUNT_SEGMENT23,
CHARGE_ACCOUNT_SEGMENT24,
CHARGE_ACCOUNT_SEGMENT25,
CHARGE_ACCOUNT_SEGMENT26,
CHARGE_ACCOUNT_SEGMENT27,
CHARGE_ACCOUNT_SEGMENT28,
CHARGE_ACCOUNT_SEGMENT29,
CHARGE_ACCOUNT_SEGMENT30,
DESTINATION_CONTEXT,
PROJECT,
TASK,
PJC_EXPENDITURE_ITEM_DATE,
EXPENDITURE_TYPE,
EXPENDITURE_ORGANIZATION,
PJC_BILLABLE_FLAG,
PJC_CAPITALIZABLE_FLAG,
PJC_WORK_TYPE,
PJC_RESERVED_ATTRIBUTE1,
PJC_RESERVED_ATTRIBUTE2,
PJC_RESERVED_ATTRIBUTE3,
PJC_RESERVED_ATTRIBUTE4,
PJC_RESERVED_ATTRIBUTE5,
PJC_RESERVED_ATTRIBUTE6,
PJC_RESERVED_ATTRIBUTE7,
PJC_RESERVED_ATTRIBUTE8,
PJC_RESERVED_ATTRIBUTE9,
PJC_RESERVED_ATTRIBUTE10,
PJC_USER_DEF_ATTRIBUTE1,
PJC_USER_DEF_ATTRIBUTE2,
PJC_USER_DEF_ATTRIBUTE3,
PJC_USER_DEF_ATTRIBUTE4,
PJC_USER_DEF_ATTRIBUTE5,
PJC_USER_DEF_ATTRIBUTE6,
PJC_USER_DEF_ATTRIBUTE7,
PJC_USER_DEF_ATTRIBUTE8,
PJC_USER_DEF_ATTRIBUTE9,
PJC_USER_DEF_ATTRIBUTE10,
RATE,
RATE_DATE,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
DELIVER_TO_PERSON_EMAIL_ADDR,
BUDGET_DATE,
PJC_CONTRACT_NUMBER,
PJC_FUNDING_SOURCE,
GLOBAL_ATTRIBUTE1,

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
							'
                                  || chr(39)
                                  || gv_execution_id
                                  || chr(39)
                                  || '
                            FROM XXCNV_PO_C007_PO_DISTRIBUTIONS_EXT';

                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in XXCNV_PO_C007_PO_DISTRIBUTIONS_STG: ' || SQL%rowcount);
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error creating external table: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

    -- Count the number of rows in the external table
        BEGIN
            IF gv_oci_file_name = '%PO_HEADERS%' THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_po_c007_po_headers_stg;

                dbms_output.put_line('Inserted Records in the XXCNV_PO_C007_PO_HEADERS_STG from OCI Source Folder: ' || lv_row_count)
                ;
            END IF;

            IF gv_oci_file_name = '%XXCNV_PO_C007_PO_LINES_STG%' THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_po_c007_po_lines_stg;

                dbms_output.put_line('Inserted Records in the XXCNV_PO_C007_PO_LINES_STG from OCI Source Folder: ' || lv_row_count);
            END IF;

            IF gv_oci_file_name = '%PO_LINE_LOCATIONS%' THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_po_c007_po_line_locations_stg;

                dbms_output.put_line('Inserted Records in the XXCNV_PO_C007_PO_LINE_LOCATIONS_STG from OCI Source Folder: ' || lv_row_count
                );
            END IF;

            IF gv_oci_file_name = '%PO_DISTRIBUTIONS%' THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_po_c007_po_distributions_stg;

                dbms_output.put_line('Inserted Records in the XXCNV_PO_C007_PO_DISTRIBUTIONS_STG from OCI Source Folder: ' || lv_row_count
                );
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in the external table: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

    -- Select batch_id from the external table
        BEGIN
        -- Count the number of rows in the external table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_po_c007_po_headers_stg;

            dbms_output.put_line('Log:Inserted Records in the XXCNV_PO_C007_PO_HEADERS_STG from OCI Source Folder: ' || lv_row_count)
            ;
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_picked,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_headers,
                p_attribute1        => NULL,
                p_attribute2        => lv_row_count,
                p_process_reference => NULL
            );
       -- END LOOP;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in XXCNV_PO_C007_PO_HEADERS_STG: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

        BEGIN
        -- Count the number of rows in the external table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_po_c007_po_lines_stg;

            dbms_output.put_line('Log:Inserted Records in the XXCNV_PO_C007_PO_LINES_STG from OCI Source Folder: ' || lv_row_count);
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_picked,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_lines,
                p_attribute1        => NULL,
                p_attribute2        => lv_row_count,
                p_process_reference => NULL
            );
       -- END LOOP;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in XXCNV_PO_C007_PO_LINES_STG: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

        BEGIN
        -- Count the number of rows in the external table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_po_c007_po_line_locations_stg;

            dbms_output.put_line('Log:Inserted Records in the XXCNV_PO_C007_PO_LINE_LOCATIONS_STG from OCI Source Folder: ' || lv_row_count
            );
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_picked,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_line_locations,
                p_attribute1        => NULL,
                p_attribute2        => lv_row_count,
                p_process_reference => NULL
            );
       -- END LOOP;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in XXCNV_PO_C007_PO_LINE_LOCATIONS_STG: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

        BEGIN
        -- Count the number of rows in the external table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_po_c007_po_distributions_stg;

            dbms_output.put_line('Log:Inserted Records in the XXCNV_PO_C007_PO_DISTRIBUTIONS_STG from OCI Source Folder: ' || lv_row_count
            );

        -- Use an implicit cursor in the FOR LOOP to iterate over distinct batch_ids
       -- FOR rec IN (SELECT DISTINCT batch_id FROM XXCNV_PO_C007_PO_DISTRIBUTIONS_STG) LOOP
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_picked,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_distributions,
                p_attribute1        => NULL,
                p_attribute2        => lv_row_count,
                p_process_reference => NULL
            );
       -- END LOOP;

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in XXCNV_PO_C007_PO_DISTRIBUTIONS_STG: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

    END import_data_from_oci_to_stg_prc;

/*==================================================================================================
-- PROCEDURE : DATA_VALIDATIONS_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for the validating the mandatory columns and business validations as per lean spec
====================================================================================================*/
--TABLE1
    PROCEDURE data_validations_prc IS

  -- Declaring Local Variables for validation.

        lv_row_count      NUMBER;
        lv_error_count    NUMBER;
        lv_exp_source_coa VARCHAR2(100);
    BEGIN
        BEGIN

  -- Initialize ERROR_MESSAGE to an empty string if it is NULL
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = ''
                WHERE
                    error_message IS NULL;

            END;



-- Validate INTERFACE_HEADER_KEY
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '**INTERFACE HEADER KEY SHOULD NOT BE NULL'
                WHERE
                    interface_header_key IS NULL;

                dbms_output.put_line('Interface Header Key is validated');
            END;

 -- Validate Unique INTERFACE_HEADER_KEY in XXCNV_PO_C007_PO_HEADERS_STG

-- Step 1: Check for duplicate INTERFACE_HEADER_KEY in XXCNV_PO_C007_PO_HEADERS_STG
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|Duplicate INTERFACE_HEADER_KEY found in XXCNV_PO_C007_PO_HEADERS_STG.'
                WHERE
                    interface_header_key IN (
                        SELECT
                            interface_header_key
                        FROM
                            xxcnv_po_c007_po_headers_stg
                        WHERE
                            execution_id = gv_execution_id
                        GROUP BY
                            interface_header_key
                        HAVING
                            COUNT(*) > 1
                    )
                    AND execution_id = gv_execution_id;

            END;

-- Step 2: Check for missing INTERFACE_HEADER_KEY in XXCNV_PO_C007_PO_LINES_STG
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg h
                SET
                    error_message = error_message || '|Some INTERFACE_HEADER_KEY in Header Table do not have corresponding entries in Lines Table. '
                WHERE
                    NOT EXISTS (
                        SELECT
                            1
                        FROM
                            xxcnv_po_c007_po_lines_stg l
                        WHERE
                                l.interface_header_key = h.interface_header_key
                            AND l.execution_id = h.execution_id
                    )
                        AND h.execution_id = gv_execution_id;

                dbms_output.put_line('Validation of unique INTERFACE_HEADER_KEY and consistency completed');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    action = 'ORIGINAL',
                    batch_id = to_char(sysdate, 'YYYYMMDDHH24MISS'),
                    import_source_code = 'NETSUITE PO CONVERSION',
                    approval_action = 'BYPASS',
                    document_type_code = 'STANDARD',
                    style_display_name = 'Purchase Order',
                    acceptance_required_flag = 'N';

                dbms_output.put_line('All the hardcoded fileds are updated in Headers');
            END;



    -- Validate Order
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|DOCUMENT NUM SHOULD NOT BE NULL'
                WHERE
                    document_num IS NULL;

                dbms_output.put_line('Document Num is validated');
            END;
	-- Validating Unique Order
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|Duplicate DOCUMENT_NUM found in XXCNV_PO_C007_PO_HEADERS_STG. '
                WHERE
                    document_num IN (
                        SELECT
                            document_num
                        FROM
                            xxcnv_po_c007_po_headers_stg
                        WHERE
                            execution_id = gv_execution_id
                        GROUP BY
                            document_num
                        HAVING
                            COUNT(*) > 1
                    )
                    AND execution_id = gv_execution_id;

            END;

    -- Validate Requisitioning BU
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|REQ_BU_NAME SHOULD NOT BE NULL'
                WHERE
                    req_bu_name IS NULL;

                dbms_output.put_line('Requisitioning BU is validated');
            END;
	 --  validate Requisitioning BU
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    req_bu_name = (
                        SELECT
                            oc_business_unit_name
                        FROM
                            xxcnv_gl_le_bu_mapping
                        WHERE
                            ns_legal_entity_name = req_bu_name
                    )
                WHERE
                    req_bu_name IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Requisitioning BU is updated');
            END;


-- ATTRIBUTE_NUMBER1
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    attribute_number1 = (
                        SELECT
                            oc_legal_entity_id
                        FROM
                            xxcnv_gl_le_bu_mapping
                        WHERE
                            ns_legal_entity_name = soldto_le_name
                    )
                WHERE
                        1 = 1
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('ATTRIBUTE_NUMBER1 is updated');
            END;

 -- Validate Sold-to Legal Entity
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|SOLD TO LEGAL ENTITY SHOULD NOT BE NULL'
                WHERE
                    soldto_le_name IS NULL;

                dbms_output.put_line('Sold-to Legal Entity is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    soldto_le_name = (
                        SELECT
                            oc_legal_entity_name
                        FROM
                            xxcnv_gl_le_bu_mapping
                        WHERE
                            ns_legal_entity_name = soldto_le_name
                    )
                WHERE
                    soldto_le_name IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('SOLDTO Legal Entity is updated');
            END;
-- Sold To LE Validated with comma
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    soldto_le_name = '"'
                                     || ( replace(soldto_le_name, '"', NULL) )
                                     || '"'
                WHERE
                    soldto_le_name LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line(' SOLDTO Legal Entity With Comma is validated');
            END;

  -- Validate Buyer
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|AGENT NAME SHOULD NOT BE NULL'
                WHERE
                    agent_name IS NULL;

                dbms_output.put_line('Buyer is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    agent_name = (
                        SELECT
                            emp_name
                        FROM
                            xxcnv_po_employee_mapping
                        WHERE
                            emp_id = agent_name
                    )
                WHERE
                    agent_name IS NOT NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line(' AGENT_NAME With Comma is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    agent_name = 'Ventrapragada, Sindhuja'
                WHERE
                    agent_name LIKE '%Ã%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line(' AGENT_NAME is replaced');
            END;			


-- Agent Name Validated with comma
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    agent_name = '"'
                                 || agent_name
                                 || '"'
                WHERE
                    agent_name LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line(' AGENT_NAME With Comma is validated');
            END;

    -- Validate Bill-to BU
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|BILL TO BU SHOULD NOT BE NULL'
                WHERE
                    billto_bu_name IS NULL;

                dbms_output.put_line('Bill-to BU is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    billto_bu_name = (
                        SELECT
                            m.bill_to_bu_name
                        FROM
                            xxcnv_ap_supplier_mapping m
                        WHERE
                                1 = 1
                            AND m.ns_vendor_num = billto_bu_name
                            AND m.bill_to_bu_name = req_bu_name
                    )
                WHERE
                    billto_bu_name IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('BILLTO_BU Name is updated');
            END;




    -- Validate Currency Code
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|CURRENCY CODE SHOULD NOT BE NULL'
                WHERE
                    currency_code IS NULL;

                dbms_output.put_line('Currency Code is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    comments = ( replace(comments, '"', NULL) )
                WHERE
                        1 = 1
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line(' Description With Comma is validated');
            END;

--Validate COMMENTS	Length		
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    comments = substr(comments, 1, 230)
                WHERE
                        length(comments) > 240
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line(' Description With Comma is validated');
            END;
--Validate COMMENTS with comma's

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    comments = '"'
                               || ( replace(comments, '"', NULL) )
                               || '"'
                WHERE
                    comments LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line(' Description With Comma is validated');
            END;



    -- Validate Bill-to Location
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|BILL TO LOCATION SHOULD NOT BE NULL'
                WHERE
                    bill_to_location IS NULL;

                dbms_output.put_line('Bill-to Location is validated');
            END;

		 --Update Bill-to Location
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    bill_to_location = (
                        SELECT
                            oc_bill_to_location
                        FROM
                            xxcnv_gl_le_bu_mapping
                        WHERE
                            ns_legal_entity_name = bill_to_location
                    )
                WHERE
                        1 = 1
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Bill-to Location is updated');
            END;





	 --Update Ship-to Location
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    target_shipto_location = (
                        SELECT
                            os_location_name
                        FROM
                            xxcnv_po_ship_to_location_mapping
                        WHERE
                            ns_location_name = ship_to_location
                    )
                WHERE
                        1 = 1
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('SHIP_TO_LOCATION is updated');
            END;

    -- Validate Ship-to Location
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    target_shipto_location = bill_to_location
                WHERE
                    target_shipto_location IS NULL;

                dbms_output.put_line('Ship-to Location is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    target_shipto_location = '"'
                                             || ( replace(target_shipto_location, '"', NULL) )
                                             || '"'
                WHERE
                    target_shipto_location LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Target_ShipTo_Location With Comma is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    ship_to_location = '"'
                                       || ( replace(ship_to_location, '"', NULL) )
                                       || '"'
                WHERE
                    ship_to_location LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('SHIP_TO_LOCATION With Comma is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    bill_to_location = '"'
                                       || ( replace(bill_to_location, '"', NULL) )
                                       || '"'
                WHERE
                    bill_to_location LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('BILL_TO_LOCATION With Comma is validated');
            END;


  --Update Suplier Name
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg 
/*  --Commented for V1.3 
          SET VENDOR_NAME =(SELECT oc_vendor_name FROM xxcnv_ap_supplier_mapping WHERE ns_vendor_num = SUPPLIER_SITE_CODE)
*/
-- Start Added changes for V1.3
                SET
                    vendor_name = (
                        SELECT
                            m.oc_vendor_name
                        FROM
                            xxcnv_ap_supplier_mapping m
                        WHERE
                                m.ns_vendor_num = supplier_site_code
                            AND m.bill_to_bu_name = req_bu_name
                    )
--End changes for V1.3
                WHERE
                        1 = 1
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('VENDOR_NAME is updated');
            END;

  --Validate Supplier Name with comma's

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    vendor_name = '"'
                                  || ( replace(vendor_name, '"', NULL) )
                                  || '"'
                WHERE
                    vendor_name LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('VENDOR_NAME With Comma is validated');
            END;

    -- Validate Supplier
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|SUPPLIER SHOULD NOT BE NULL'
                WHERE
                    vendor_num IS NULL;

                dbms_output.put_line('Supplier is validated');
            END;

    -- Validate Supplier Site
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|SUPPLIER SITE CODE SHOULD NOT BE NULL'
                WHERE
                    supplier_site_code IS NULL;

                dbms_output.put_line('Supplier Site is validated');
            END;

	 --Update Supplier 
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
/* --Commented for V1.3 
    SET    VENDOR_NUM = (SELECT oc_vendor_num FROM xxcnv_ap_supplier_mapping WHERE ns_vendor_num = SUPPLIER_SITE_CODE)
*/
-- Start Added changes for V1.3
                SET
                    vendor_num = (
                        SELECT
                            m.oc_vendor_num
                        FROM
                            xxcnv_ap_supplier_mapping m
                        WHERE
                                m.ns_vendor_num = supplier_site_code
                            AND m.bill_to_bu_name = req_bu_name
                    )
--End changes for V1.3

                WHERE
                        1 = 1
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('SUPPLIER is updated');
            END;

    -- Validate Supplier
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|Corresponding Supplier not found in Oracle'
                WHERE
                    vendor_num IS NULL;

                dbms_output.put_line('Corresponding Supplier not found in Oracle');
            END;

  --  update PAYMENT_TERMS
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    payment_terms = (
                        SELECT
                            oc_value
                        FROM
                            xxcnv_ap_payment_terms_mapping
                        WHERE
                            ns_value = payment_terms
                    )
                WHERE
                    payment_terms IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('PAYMENT_TERMS is updated');
            END;




--Supplier Site Code update
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    supplier_site_code = (
                        SELECT
                            m.oc_vendor_site
                        FROM
                            xxcnv_ap_supplier_mapping m
                        WHERE
                                m.ns_vendor_num = supplier_site_code
                            AND m.bill_to_bu_name = req_bu_name
                    )
                WHERE
                    supplier_site_code IS NOT NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('SUPPLIER_Site_Code is Updated');
            END; 

	 --Validate Supplier Site with comma's

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    supplier_site_code = '"'
                                         || supplier_site_code
                                         || '"'
                WHERE
                    supplier_site_code LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Supplier site With Comma is validated');
            END; 

			 -- Validate Supplier Site
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|Corresponding SUPPLIER SITE CODE not found in Oracle'
                WHERE
                    supplier_site_code IS NULL;

                dbms_output.put_line('Corresponding SUPPLIER SITE CODE not found in Oracle');
            END;


			  -- Update import_status based on error_message
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
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

			  -- Final update to set error_message and import_status
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
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
                        execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('import_status column is updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    source_system = gv_boundary_system
                WHERE
                    file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('source_system is updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    file_name = gv_oci_file_name_headers
                WHERE
                    file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_name column is updated');
            END;

           -- Check if there are any error messages
            SELECT
                COUNT(*)
            INTO lv_error_count
            FROM
                xxcnv_po_c007_po_headers_stg
            WHERE
                error_message IS NOT NULL;


  -- Check if there are any error messages

            UPDATE xxcnv_po_c007_po_headers_stg
            SET
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure
            WHERE
                error_message IS NOT NULL
                AND file_reference_identifier IS NULL
                AND execution_id = gv_execution_id;

            dbms_output.put_line('file_reference_identifier column is updated');
            UPDATE xxcnv_po_c007_po_headers_stg
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

     -- Logging the message IF data is not validated
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_failed,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_headers,
                    p_attribute1        => gv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );
            END IF;

            IF
                lv_error_count = 0
                AND gv_oci_file_name_headers IS NOT NULL
            THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_validated,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_headers,
                    p_attribute1        => gv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );
            END IF;

        END;



  --2 TABLE VALIDATION
        BEGIN
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    execution_id = gv_execution_id;

            END;


 -- Validate INTERFACE_LINE_KEY for NULL values
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || ' **INTERFACE_LINE_KEY should not be NULL'
                WHERE
                    interface_line_key IS NULL;

                dbms_output.put_line('INTERFACE_LINE_KEY is validated');
            END;

            BEGIN
    -- Validate INTERFACE_LINE_KEY for duplicates
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || ' **Duplicate INTERFACE_LINE_KEY'
                WHERE
                    interface_line_key IN (
                        SELECT
                            interface_line_key
                        FROM
                            xxcnv_po_c007_po_lines_stg
                        GROUP BY
                            interface_line_key
                        HAVING
                            COUNT(*) > 1
                    );

                dbms_output.put_line('Duplicate INTERFACE_LINE_KEY is validated');
            END;

-- Check for missing INTERFACE_LINE_KEY in XXCNV_PO_C007_PO_LINE_LOCATIONS_STG
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg h
                SET
                    error_message = error_message || '|Some INTERFACE_LINE_KEY in Lines Table do not have corresponding entries in Locations Table. '
                WHERE
                    NOT EXISTS (
                        SELECT
                            1
                        FROM
                            xxcnv_po_c007_po_line_locations_stg l
                        WHERE
                                l.interface_line_key = h.interface_line_key
                            AND l.execution_id = h.execution_id
                    )
                        AND h.execution_id = gv_execution_id;

                dbms_output.put_line('Validation of unique INTERFACE_LINE_LOCATION_KEY and consistency completed');
            END;

--Validating Lines Interface Header Key presence in Headers
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg l
                SET
                    l.error_message = 'PO Lines Interface Header Key not found in Headers'
                WHERE
                    l.interface_header_key NOT IN (
                        SELECT
                            h.interface_header_key
                        FROM
                            xxcnv_po_c007_po_headers_stg h
                    );

            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    action = 'ADD'
                WHERE
                    file_reference_identifier IS NULL;

                dbms_output.put_line('All the hardcoded fileds are updated in Lines');
            END;

            BEGIN
    -- Validate LINE_NUM
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || ' **LINE_NUM SHOULD NOT BE NULL'
                WHERE
                    line_num IS NULL;

                dbms_output.put_line('LINE_NUM is validated');
            END;

 -- Validate Line Number
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || ' **Invalid Line Number'
                WHERE
                    NOT REGEXP_LIKE ( line_num,
                                      '^\d+$' );

                dbms_output.put_line(' Line Number validation completed');
            END;

            BEGIN
    -- Validate ITEM_DESCRIPTION and CATEGORY for non-item based purchases
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || ' **ITEM_DESCRIPTION and ITEM SHOULD NOT BE NULL'
                WHERE
                    item_description IS NULL
                    AND item IS NULL;

                dbms_output.put_line('ITEM_DESCRIPTION and ITEM is validated');
            END;

            BEGIN
    -- Validate ITEM
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    item = item
                WHERE
                    item IS NOT NULL
                    AND item IN (
                        SELECT
                            oc_item
                        FROM
                            xxcnv_po_item_list_mapping
                    );

                dbms_output.put_line('ITEM is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    category = (
                        SELECT
                            oc_category
                        FROM
                            xxcnv_po_item_category_mapping
                        WHERE
                                1 = 1
                            AND oc_item = item
                    )
                WHERE
                        execution_id = gv_execution_id
                    AND item NOT IN (
                        SELECT
                            oc_item
                        FROM
                            xxcnv_po_item_list_mapping
                    )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Category is Updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    item_description = (
                        SELECT
                            oc_item
                        FROM
                            xxcnv_po_item_category_mapping
                        WHERE
                                1 = 1
                            AND oc_item = item
                    )
                WHERE
                        execution_id = gv_execution_id
                    AND item NOT IN (
                        SELECT
                            oc_item
                        FROM
                            xxcnv_po_item_list_mapping
                    )
                    AND item_description IS NULL
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('ITEM_DESCRIPTION is Updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    item = NULL
                WHERE
                    item IN (
                        SELECT
                            oc_item
                        FROM
                            xxcnv_po_item_category_mapping
                    )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('ITEM is Updated');
            END;


-- Item Error message
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || '|Corresponding Items are not found'
                WHERE
                    item NOT IN (
                        SELECT
                            oc_item
                        FROM
                            xxcnv_po_item_category_mapping
                    )
                    AND item NOT IN (
                        SELECT
                            oc_item
                        FROM
                            xxcnv_po_item_list_mapping
                    )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Items Error Message is Updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    item = '"'
                           || ( replace(item, '"', NULL) )
                           || '"'
                WHERE
                    item LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('ITEM comma is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    item_description = substr(item_description, 1, 230)
                WHERE
                        length(item_description) > 240
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line(' ITEM_DESCRIPTION With Comma is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    item_description = '"'
                                       || ( replace(item_description, '"', NULL) )
                                       || '"'
                WHERE
                    item_description LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('ITEM_DESCRIPTION comma is validated');
            END;


--Line Type
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    line_type = 'Goods'
                WHERE
                    item IS NOT NULL
--and INTERFACE_LINE_KEY in (select ll.INTERFACE_LINE_KEY from XXCNV_PO_C007_PO_LINE_LOCATIONS_STG ll WHERE ll.DESTINATION_TYPE_CODE = 'INVENTORY')
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Line Type is Updated');
            END;




--Line Type
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    line_type = 'Rate Based Services'
                WHERE
                    item IS NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Line Type is Updated');
            END;

--Linetype
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    line_type = 'Goods'
                WHERE
                    item IS NULL
                    AND category IN (
                        SELECT
                            category
                        FROM
                            xxcnv_po_category_mapping
                    ) -- Added changes for Version 1.2
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Line Type is Updated');
            END;


--Header PRC BU Update
            BEGIN
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    prc_bu_name = req_bu_name
                WHERE
                    interface_header_key IN (
                        SELECT
                            l.interface_header_key
                        FROM
                            xxcnv_po_c007_po_lines_stg l
                        WHERE
                            l.item IS NOT NULL
                        GROUP BY
                            l.interface_header_key
                    );

                dbms_output.put_line('PRC BU is Updated');
            END;


-- Validate QUANTITY
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || '|Quantity cannot be Null or Negative'
                WHERE
                    ( quantity IS NULL
                      OR quantity < 0 )
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('QUANTITY is validated');
            END;




-- Update SHIPPING_UNIT_OF_MEASURE
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    shipping_unit_of_measure = 'Each'
                WHERE
                    ( ( shipping_unit_of_measure IS NULL )
                      OR ( shipping_unit_of_measure IN ( 'ea' ) ) );

                dbms_output.put_line('SHIPPING_UNIT_OF_MEASURE is updated');
            END;

-- Validate NOTE_TO_VENDOR

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    note_to_vendor = '"'
                                     || ( replace(note_to_vendor, '"', NULL) )
                                     || '"'
                WHERE
                    note_to_vendor LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('NOTE_TO_VENDOR comma is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || '|Division should not be null'
                WHERE
                    line_attribute2 IS NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Attribute2 is Updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || '|Department should not be null'
                WHERE
                    line_attribute3 IS NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Attribute3 is Updated');
            END;

 -- Validate Product Line   
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg s
                SET
                    s.target_attribute1 = (
                        SELECT
                            coa_oc_desc
                        FROM
                            xxmap.xxmap_gl_e001_kaseya_ns_productline l
                        WHERE
                            l.ns_productline_attribute_1 = s.line_attribute1
                    )
                WHERE
                        1 = 1
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Product Line Attribute1 is Updated');
            END; 

  -- Validate Division    
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg s
                SET
                    s.target_attribute2 = (
                        SELECT
                            coa_oc_desc
                        FROM
                            xxmap.xxmap_gl_e001_kaseya_ns_divison l
                        WHERE
                            l.ns_divison_attribute_1 = s.line_attribute2
                    )
                WHERE
                        1 = 1
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Division Attribute2 is Updated');
            END; 


  -- Validate Department   
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg s
                SET
                    s.target_attribute3 = (
                        SELECT
                            coa_oc_desc
                        FROM
                            xxmap.xxmap_gl_e001_kaseya_ns_costcenter l
                        WHERE
                            l.ns_costcenter_attribute_1 = s.line_attribute3
                    )
                WHERE
                        1 = 1
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Department ATTRIBUTE3 is Updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || '|Corresponding Division not found in oracle'
                WHERE
                    target_attribute2 IS NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Attribute2 is Updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || '|Corresponding Department not found in oracle'
                WHERE
                    target_attribute3 IS NULL
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('Attribute3 is Updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    line_attribute4 = '"'
                                      || ( replace(line_attribute4, '"', NULL) )
                                      || '"'
                WHERE
                    line_attribute4 LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('LINE_ATTRIBUTE4 comma is validated');
            END;

  --Erroring out the record in child table as it errored out in parent table
            BEGIN
              -- Update the import_status in XXCNV_PO_C007_PO_LINES_STG to 'ERROR' where the PARENT RECORD   has import_status 'ERROR'
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || '|Header record failed at validation',
                    import_status = 'ERROR'
                WHERE
                    ( interface_header_key IN (
                        SELECT
                            interface_header_key
                        FROM
                            xxcnv_po_c007_po_headers_stg
                        WHERE
                                import_status = 'ERROR'
                            AND execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;


  -- Update import_status based on error_message
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
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

			  -- Final update to set error_message and import_status
            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
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
                        execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('import_status column is updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    source_system = gv_boundary_system
                WHERE
                    file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('source_system is updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    file_name = gv_oci_file_name_lines
                WHERE
                    file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_name column is updated');
            END;

           -- Check if there are any error messages
            SELECT
                COUNT(*)
            INTO lv_error_count
            FROM
                xxcnv_po_c007_po_lines_stg
            WHERE
                error_message IS NOT NULL;

  -- Check if there are any error messages

            UPDATE xxcnv_po_c007_po_lines_stg
            SET
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure
            WHERE
                error_message IS NOT NULL
                AND file_reference_identifier IS NULL
                AND execution_id = gv_execution_id;

            dbms_output.put_line('file_reference_identifier column is updated');
            UPDATE xxcnv_po_c007_po_lines_stg
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

     -- Logging the message IF data is not validated
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_failed,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_lines,
                    p_attribute1        => gv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );
            END IF;

            IF
                lv_error_count = 0
                AND gv_oci_file_name_lines IS NOT NULL
            THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_validated,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_lines,
                    p_attribute1        => gv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );
            END IF;

        END;

--3 TABLE
        BEGIN
            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    execution_id = gv_execution_id;

            END;


-- Validate INTERFACE_LINE_LOCATION_KEY 
            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    error_message = error_message || '**INTERFACE_LINE_LOCATION_KEY  SHOULD NOT BE NULL'
                WHERE
                    interface_line_location_key IS NULL;

                dbms_output.put_line('INTERFACE_LINE_LOCATION_KEY is validated');
            END;

 -- Validate Unique INTERFACE_LINE_LOCATION_KEY in XXCNV_PO_C007_PO_LINE_LOCATIONS_STG

-- Step 1: Check for duplicate INTERFACE_LINE_LOCATION_KEY in XXCNV_PO_C007_PO_LINE_LOCATIONS_STG
            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    error_message = error_message || '|Duplicate INTERFACE_LINE_LOCATION_KEY found in XXCNV_PO_C007_PO_LINE_LOCATIONS_STG. '
                WHERE
                    interface_line_location_key IN (
                        SELECT
                            interface_line_location_key
                        FROM
                            xxcnv_po_c007_po_line_locations_stg
                        WHERE
                            execution_id = gv_execution_id
                        GROUP BY
                            interface_line_location_key
                        HAVING
                            COUNT(*) > 1
                    )
                    AND execution_id = gv_execution_id;

            END;

-- Step 2: Check for missing INTERFACE_LINE_LOCATION_KEY in XXCNV_PO_C007_PO_DISTRIBUTIONS_STG
            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg h
                SET
                    error_message = error_message || '|Some INTERFACE_LINE_LOCATION_KEY in Locations Table do not have corresponding entries in Distributions Table. '
                WHERE
                    NOT EXISTS (
                        SELECT
                            1
                        FROM
                            xxcnv_po_c007_po_distributions_stg l
                        WHERE
                                l.interface_line_location_key = h.interface_line_location_key
                            AND l.execution_id = h.execution_id
                    )
                        AND h.execution_id = gv_execution_id;

                dbms_output.put_line('Validation of unique INTERFACE_LINE_LOCATION_KEY and consistency completed');
            END;

-- Validate Interface Line Key Presence in Lines Table
            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg ll
                SET
                    ll.error_message = 'PO Location Interface line Key not found in Lines Table'
                WHERE
                    ll.interface_line_key NOT IN (
                        SELECT
                            l.interface_line_key
                        FROM
                            xxcnv_po_c007_po_lines_stg l
                    );

            END;

            BEGIN
    -- Validate Interface Line Key
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    error_message = nvl(error_message, '')
                                    || ' **Interface Line Key should not be null'
                WHERE
                    interface_line_key IS NULL;

            END;


    -- Validate Schedule
            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg ll
                SET
                    ll.shipment_num = (
                        SELECT
                            l.line_num
                        FROM
                            xxcnv_po_c007_po_lines_stg l
                        WHERE
                            l.interface_line_key = ll.interface_line_key
                    )
                WHERE
                    file_reference_identifier IS NULL;

                dbms_output.put_line('SHIPMENT_NUM is Updated');
            END;

-- Validate Ship-to Organization
            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg ll
                SET
                    ll.ship_to_organization_code = (
                        SELECT
                            oc_ship_to_org_code
                        FROM
                            xxcnv_po_reqbu_shiporg_mapping
                        WHERE
                            oc_requisition_bu = (
                                SELECT
                                    h.req_bu_name
                                FROM
                                    xxcnv_po_c007_po_headers_stg h,
                                    xxcnv_po_c007_po_lines_stg   l
                                WHERE
                                        1 = 1
                                    AND h.interface_header_key = l.interface_header_key
                                    AND l.interface_line_key = ll.interface_line_key
                            )
                    )
                WHERE
                        1 = 1
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('SHIP_TO_ORGANIZATION_CODE is updated');
            END;

  	 --Update Ship-to Location
            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg ll
                SET
                    ll.ship_to_location = (
                        SELECT
                            h.target_shipto_location
                        FROM
                            xxcnv_po_c007_po_headers_stg h,
                            xxcnv_po_c007_po_lines_stg   l
                        WHERE
                                1 = 1
                            AND h.interface_header_key = l.interface_header_key
                            AND l.interface_line_key = ll.interface_line_key
                    )
                WHERE
                        1 = 1
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('SHIP_TO_LOCATION is updated');
            END;

            BEGIN
    -- Validate Destination Type Code
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    error_message = nvl(error_message, '')
                                    || ' **Invalid Destination Type Code'
                WHERE
                    destination_type_code NOT IN ( 'INVENTORY', 'EXPENSE' );

            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    promised_date = sysdate
                WHERE
                        1 = 1
                    AND promised_date IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('PROMISED_DATE is update');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    error_message = error_message || ' Both NEED_BY_DATE and PROMISED_DATE Should Not Be Null'
                WHERE
                    need_by_date IS NULL
                    AND promised_date IS NULL;

                dbms_output.put_line(' NEED_BY_DATE and PROMISED_DATE validation completed');
            END;

            BEGIN
    -- Validate Invoice Match Option and Recipt Flag
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    invoice_match_option = NULL,
                    receipt_required_flag = 'N'
                WHERE
                    interface_line_key IN (
                        SELECT
                            l.interface_line_key
                        FROM
                            xxcnv_po_c007_po_lines_stg l
                        WHERE
                                1 = 1
                            AND l.interface_line_key = interface_line_key
                        GROUP BY
                            l.interface_line_key
                    );

                dbms_output.put_line('Validation completed for XXCNV_PO_C007_PO_LINE_LOCATIONS_STG');
            END; 

  --Erroring out the record in child table as it errored out in parent table
            BEGIN
              -- Update the import_status in XXCNV_PO_C007_PO_LINE_LOCATIONS_STG to 'ERROR' where the PARENT RECORD   has import_status 'ERROR'
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    error_message = error_message || '|Line Record failed at validation',
                    import_status = 'ERROR'
                WHERE
                    ( interface_line_key IN (
                        SELECT
                            interface_line_key
                        FROM
                            xxcnv_po_c007_po_lines_stg
                        WHERE
                                import_status = 'ERROR'
                            AND execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;

    -- Update import_status based on error_message
            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg
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

			  -- Final update to set error_message and import_status
            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg
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
                        execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('import_status column is updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    source_system = gv_boundary_system
                WHERE
                    file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('source_system is updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    file_name = gv_oci_file_name_line_locations
                WHERE
                    file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_name column is updated');
            END;

           -- Check if there are any error messages
            SELECT
                COUNT(*)
            INTO lv_error_count
            FROM
                xxcnv_po_c007_po_line_locations_stg
            WHERE
                error_message IS NOT NULL;



  -- Check if there are any error messages

            UPDATE xxcnv_po_c007_po_line_locations_stg
            SET
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure
            WHERE
                error_message IS NOT NULL
                AND file_reference_identifier IS NULL
                AND execution_id = gv_execution_id;

            dbms_output.put_line('file_reference_identifier column is updated');
            UPDATE xxcnv_po_c007_po_line_locations_stg
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

     -- Logging the message IF data is not validated
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_failed,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_line_locations,
                    p_attribute1        => gv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );
            END IF;

            IF
                lv_error_count = 0
                AND gv_oci_file_name_line_locations IS NOT NULL
            THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_validated,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_line_locations,
                    p_attribute1        => gv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );
            END IF;

        END;


--4 TABLE VALIDATION
        BEGIN
            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg
                SET
                    execution_id = gv_execution_id;

            END;
            BEGIN
        -- Update records where INTERFACE_DISTRIBUTION_KEY is NULL
                UPDATE xxcnv_po_c007_po_distributions_stg
                SET
                    error_message = error_message || ' **Interface Distribution Key should not be NULL'
                WHERE
                    interface_distribution_key IS NULL;

                dbms_output.put_line('INTERFACE_DISTRIBUTION_KEY is validated for NULL values');
            END;

            BEGIN
    -- Update records where INTERFACE_DISTRIBUTION_KEY is not unique
                UPDATE xxcnv_po_c007_po_distributions_stg
                SET
                    error_message = error_message || ' **Interface Distribution Key is not unique'
                WHERE
                    interface_distribution_key IN (
                        SELECT
                            interface_distribution_key
                        FROM
                            xxcnv_po_c007_po_distributions_stg
                        GROUP BY
                            interface_distribution_key
                        HAVING
                            COUNT(*) > 1
                    );

                dbms_output.put_line('INTERFACE_DISTRIBUTION_KEY uniqueness is validated');
            END;

-- Validating Missing Line Location Key In Distributions
            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg dl
                SET
                    dl.error_message = 'PO Distribution Interface line location Key not found in Locations Table'
                WHERE
                    dl.interface_line_location_key NOT IN (
                        SELECT
                            ll.interface_line_location_key
                        FROM
                            xxcnv_po_c007_po_line_locations_stg ll
                    );

            END;

	  -- Validate Distribution Number

            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg d
                SET
                    d.distribution_num = (
                        SELECT
                            l.line_num
                        FROM
                            xxcnv_po_c007_po_lines_stg          l,
                            xxcnv_po_c007_po_line_locations_stg ll
                        WHERE
                                l.interface_line_key = ll.interface_line_key
                            AND ll.interface_line_location_key = d.interface_line_location_key
                    )
                WHERE
                    file_reference_identifier IS NULL;

                dbms_output.put_line('DISTRIBUTION_NUM is Updated');
            END;

	 -- Validate Deliver To Location
            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg d
                SET
                    d.deliver_to_location = (
                        SELECT
                            ll.ship_to_location
                        FROM
                            xxcnv_po_c007_po_line_locations_stg ll
                        WHERE
                            ll.interface_line_location_key = d.interface_line_location_key
                    )
                WHERE
                    file_reference_identifier IS NULL;

                dbms_output.put_line('Deliver To Location is Updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg
                SET
                    deliver_to_person_full_name = (
                        SELECT
                            emp_name
                        FROM
                            xxcnv_po_employee_mapping
                        WHERE
                            emp_id = deliver_to_person_full_name
                    )
                WHERE
                    deliver_to_person_full_name IS NOT NULL;

                dbms_output.put_line(' DELIVER_TO_PERSON_FULL_NAME Is updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg
                SET
                    deliver_to_person_full_name = 'Ventrapragada, Sindhuja'
                WHERE
                    deliver_to_person_full_name LIKE '%Ã%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line(' DELIVER_TO_PERSON_FULL_NAME is replaced');
            END;



	 -- DELIVER_TO_PERSON_FULL_NAME Validated with comma
            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg
                SET
                    deliver_to_person_full_name = '"'
                                                  || ( replace(deliver_to_person_full_name, '"', NULL) )
                                                  || '"'
                WHERE
                    deliver_to_person_full_name LIKE '%,%'
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line(' DELIVER_TO_PERSON_FULL_NAME With Comma is validated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg d
                SET
                    d.destination_subinventory = (
                        SELECT
                            os_sub_inventory
                        FROM
                            xxcnv_po_reqbu_shiporg_mapping
                        WHERE
                            oc_ship_to_org_code = (
                                SELECT
                                    ll.ship_to_organization_code
                                FROM
                                    xxcnv_po_c007_po_line_locations_stg ll
                                WHERE
                                        1 = 1
                                    AND ll.interface_line_location_key = d.interface_line_location_key
                                    AND ll.destination_type_code = 'INVENTORY'
                            )
                    );

                dbms_output.put_line('DESTINATION_SUBINVENTORY is validated');
            END;

            BEGIN
              -- Update the import_status in XXCNV_PO_C007_PO_DISTRIBUTIONS_STG to 'ERROR' where the PARENT RECORD   has import_status 'ERROR'
                UPDATE xxcnv_po_c007_po_distributions_stg
                SET
                    error_message = error_message || '|Line Location failed at validation',
                    import_status = 'ERROR'
                WHERE
                    ( interface_line_location_key IN (
                        SELECT
                            interface_line_location_key
                        FROM
                            xxcnv_po_c007_po_line_locations_stg
                        WHERE
                                import_status = 'ERROR'
                            AND execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

            END;


  -- Update import_status based on error_message
            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg
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

			  -- Final update to set error_message and import_status
            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg
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
                        execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

                dbms_output.put_line('import_status column is updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg
                SET
                    source_system = gv_boundary_system
                WHERE
                    file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('source_system is updated');
            END;

            BEGIN
                UPDATE xxcnv_po_c007_po_distributions_stg
                SET
                    file_name = gv_oci_file_name_distributions
                WHERE
                    file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_name column is updated');
            END;

           -- Check if there are any error messages
            SELECT
                COUNT(*)
            INTO lv_error_count
            FROM
                xxcnv_po_c007_po_distributions_stg
            WHERE
                error_message IS NOT NULL;


  -- Check if there are any error messages

            UPDATE xxcnv_po_c007_po_distributions_stg
            SET
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure
            WHERE
                error_message IS NOT NULL
                AND file_reference_identifier IS NULL
                AND execution_id = gv_execution_id;

            dbms_output.put_line('file_reference_identifier column is updated');
            UPDATE xxcnv_po_c007_po_distributions_stg
            SET
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_success
            WHERE
                error_message IS NULL
                AND file_reference_identifier IS NULL
                AND execution_id = gv_execution_id;

            dbms_output.put_line('file_reference_identifier column is updated');
            BEGIN
              -- Update the import_status in XXCNV_PO_C007_PO_LINE_LOCATIONS_STG to 'ERROR' where the CHILD RECORD   has import_status 'ERROR'
                UPDATE xxcnv_po_c007_po_line_locations_stg
                SET
                    error_message = error_message || '|Distribution record failed at validation',
                    import_status = 'ERROR',
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    ( interface_line_location_key IN (
                        SELECT
                            interface_line_location_key
                        FROM
                            xxcnv_po_c007_po_distributions_stg
                        WHERE
                                import_status = 'ERROR'
                            AND execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id;

            END;

            BEGIN
              -- Update the import_status in XXCNV_PO_C007_PO_LINES_STG to 'ERROR' where the CHILD RECORD   has import_status 'ERROR'
                UPDATE xxcnv_po_c007_po_lines_stg
                SET
                    error_message = error_message || '|Line Location record failed at validation',
                    import_status = 'ERROR',
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    ( interface_line_key IN (
                        SELECT
                            interface_line_key
                        FROM
                            xxcnv_po_c007_po_line_locations_stg
                        WHERE
                                import_status = 'ERROR'
                            AND execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id;

            END;

            BEGIN
              -- Update the import_status in XXCNV_PO_C007_PO_HEADERS_STG to 'ERROR' where the CHILD RECORD   has import_status 'ERROR'
                UPDATE xxcnv_po_c007_po_headers_stg
                SET
                    error_message = error_message || '|Line record failed at validation',
                    import_status = 'ERROR',
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    ( interface_header_key IN (
                        SELECT
                            interface_header_key
                        FROM
                            xxcnv_po_c007_po_lines_stg
                        WHERE
                                import_status = 'ERROR'
                            AND execution_id = gv_execution_id
                    ) )
                    AND execution_id = gv_execution_id;

            END;

            IF lv_error_count > 0 THEN
     -- Logging the message IF data is not validated
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_failed,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_distributions,
                    p_attribute1        => gv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );
            END IF;

            IF
                lv_error_count = 0
                AND gv_oci_file_name_distributions IS NOT NULL
            THEN
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_validated,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => gv_oci_file_path,
                    p_file_name         => gv_oci_file_name_distributions,
                    p_attribute1        => gv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );
            END IF;

        END;

    END data_validations_prc;

/*==============================================================================================================================
-- PROCEDURE : CREATE_FBDI_FILE_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the FBDI CSV file after all validations.
=================================================================================================================================*/
    PROCEDURE create_fbdi_file_prc IS

        lv_success_count NUMBER := 0;
        lv_batch_id      NUMBER;
        CURSOR fbatch_id_cursor IS
        SELECT DISTINCT
            batch_id
        INTO lv_batch_id
        FROM
            xxcnv_po_c007_po_headers_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_success;

        CURSOR fbatch_id_cursor_lines IS
        SELECT DISTINCT
            h.batch_id
        INTO lv_batch_id
        FROM
            xxcnv_po_c007_po_headers_stg h,
            xxcnv_po_c007_po_lines_stg   l
        WHERE
                h.interface_header_key = l.interface_header_key
            AND l.execution_id = gv_execution_id
            AND l.file_reference_identifier = gv_execution_id
                                              || '_'
                                              || gv_status_success;

        CURSOR fbatch_id_cursor_line_loc IS
        SELECT DISTINCT
            h.batch_id
        INTO lv_batch_id
        FROM
            xxcnv_po_c007_po_headers_stg        h,
            xxcnv_po_c007_po_lines_stg          l,
            xxcnv_po_c007_po_line_locations_stg loc
        WHERE
                h.interface_header_key = l.interface_header_key
            AND l.interface_line_key = loc.interface_line_key
            AND loc.execution_id = gv_execution_id
            AND loc.file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success;

        CURSOR fbatch_id_cursor_dis IS
        SELECT DISTINCT
            h.batch_id
        INTO lv_batch_id
        FROM
            xxcnv_po_c007_po_headers_stg        h,
            xxcnv_po_c007_po_lines_stg          l,
            xxcnv_po_c007_po_line_locations_stg loc,
            xxcnv_po_c007_po_distributions_stg  d
        WHERE
                h.interface_header_key = l.interface_header_key
            AND l.interface_line_key = loc.interface_line_key
            AND loc.interface_line_location_key = d.interface_line_location_key
            AND d.execution_id = gv_execution_id
            AND d.file_reference_identifier = gv_execution_id
                                              || '_'
                                              || gv_status_success;

    BEGIN
        BEGIN
            FOR g_id IN fbatch_id_cursor LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('In create FBDI Processing Batch_ID: ' || lv_batch_id);
                BEGIN
                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                    SELECT
                        COUNT(*)
                    INTO lv_success_count
                    FROM
                        xxcnv_po_c007_po_headers_stg
                    WHERE
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                    dbms_output.put_line('Success count for batch_id '
                                         || lv_batch_id
                                         || ':'
                                         || lv_success_count);
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for XXCNV_PO_C007_PO_HEADERS_STG  batch_id: ' || lv_batch_id);
                        RETURN;
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error checking error_message column for XXCNV_PO_C007_PO_HEADERS_STG  batch_id '
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
                                             || gv_oci_file_name_headers,
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv',
                                        'trimspaces' VALUE 'rtrim',
                                        'header' VALUE FALSE
                                    ),
                            query           => 'SELECT 
                            INTERFACE_HEADER_KEY,
ACTION,
BATCH_ID,
IMPORT_SOURCE_CODE,
APPROVAL_ACTION,
DOCUMENT_NUM,
DOCUMENT_TYPE_CODE,
STYLE_DISPLAY_NAME,
PRC_BU_NAME,
REQ_BU_NAME,
SOLDTO_LE_NAME,
BILLTO_BU_NAME,
AGENT_NAME,
CURRENCY_CODE,
RATE,
RATE_TYPE,
TO_CHAR(RATE_DATE, ''YYYY/MM/DD'') AS RATE_DATE,
COMMENTS,
BILL_TO_LOCATION,
Target_ShipTo_Location,
VENDOR_NAME,
VENDOR_NUM,
SUPPLIER_SITE_CODE,
VENDOR_CONTACT,
VENDOR_DOC_NUM,
FOB,
FREIGHT_CARRIER,
FREIGHT_TERMS,
PAY_ON_CODE,
PAYMENT_TERMS,
ORIGINATOR_ROLE,
CHANGE_ORDER_DESC,
ACCEPTANCE_REQUIRED_FLAG,
ACCEPTANCE_WITHIN_DAYS,
SUPPLIER_NOTIF_METHOD,
FAX,
EMAIL_ADDRESS,
CONFIRMING_ORDER_FLAG,
NOTE_TO_VENDOR,
NOTE_TO_RECEIVER,
DEFAULT_TAXATION_COUNTRY_CODE,
TAX_DOCUMENT_SUBTYPE,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
AGENT_EMAIL_ADDRESS,
MODE_OF_TRANSPORT,
SERVICE_LEVEL,
FIRST_PTY_REG_NUM,
THIRD_PTY_REG_NUM,
BUYER_MANAGED_TRANSPORT_FLAG,
MASTER_CONTRACT_NUMBER,
MASTER_CONTRACT_TYPE,
CC_EMAIL_ADDRESS,
BCC_EMAIL_ADDRESS,
GLOBAL_ATTRIBUTE1,
GLOBAL_ATTRIBUTE2,
GLOBAL_ATTRIBUTE3,
GLOBAL_ATTRIBUTE4,
GLOBAL_ATTRIBUTE5,
GLOBAL_ATTRIBUTE6,
OVERRIDING_APPROVER_NAME,
SKIP_ELECTRONIC_COMM_FLAG,
CHECKLIST_TITLE,
CHECKLIST_NUM,
ALT_CONTACT_EMAIL_ADDRESS,
SPECIAL_HANDLING_TYPE,
SH_ATTRIBUTE1,
SH_ATTRIBUTE2,
SH_ATTRIBUTE3,
SH_ATTRIBUTE4,
SH_ATTRIBUTE5,
SH_ATTRIBUTE6,
SH_ATTRIBUTE7,
SH_ATTRIBUTE8,
SH_ATTRIBUTE9,
SH_ATTRIBUTE10,
SH_ATTRIBUTE11,
SH_ATTRIBUTE12,
SH_ATTRIBUTE13,
SH_ATTRIBUTE14,
SH_ATTRIBUTE15,
SH_ATTRIBUTE16,
SH_ATTRIBUTE17,
SH_ATTRIBUTE18,
SH_ATTRIBUTE19,
SH_ATTRIBUTE20,
SH_ATTRIBUTE_NUMBER1,
SH_ATTRIBUTE_NUMBER2,
SH_ATTRIBUTE_NUMBER3,
SH_ATTRIBUTE_NUMBER4,
SH_ATTRIBUTE_NUMBER5,
SH_ATTRIBUTE_NUMBER6,
SH_ATTRIBUTE_NUMBER7,
SH_ATTRIBUTE_NUMBER8,
SH_ATTRIBUTE_NUMBER9,
SH_ATTRIBUTE_NUMBER10,
SH_ATTRIBUTE_DATE1,
SH_ATTRIBUTE_DATE2,
SH_ATTRIBUTE_DATE3,
SH_ATTRIBUTE_DATE4,
SH_ATTRIBUTE_DATE5,
SH_ATTRIBUTE_DATE6,
SH_ATTRIBUTE_DATE7,
SH_ATTRIBUTE_DATE8,
SH_ATTRIBUTE_DATE9,
SH_ATTRIBUTE_DATE10,
SH_ATTRIBUTE_TIMESTAMP1,
SH_ATTRIBUTE_TIMESTAMP2,
SH_ATTRIBUTE_TIMESTAMP3,
SH_ATTRIBUTE_TIMESTAMP4,
SH_ATTRIBUTE_TIMESTAMP5,
SH_ATTRIBUTE_TIMESTAMP6,
SH_ATTRIBUTE_TIMESTAMP7,
SH_ATTRIBUTE_TIMESTAMP8,
SH_ATTRIBUTE_TIMESTAMP9,
SH_ATTRIBUTE_TIMESTAMP10           
                                FROM XXCNV_PO_C007_PO_HEADERS_STG
                                WHERE import_status = '''
                                     || 'PROCESSED'
                                     || '''
								and batch_id ='''
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
                                             || ' exported successfully to OCI Object Storage.');
                        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                            p_conversion_id     => gv_conversion_id,
                            p_execution_id      => gv_execution_id,
                            p_execution_step    => gv_fbdi_export_status,
                            p_boundary_system   => gv_boundary_system,
                            p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                            p_file_name         => lv_batch_id
                                           || '_'
                                           || gv_oci_file_name_headers
                                           || '.csv',
                            p_attribute1        => lv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );

                    EXCEPTION
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error exporting data to CSV for  XXCNV_PO_C007_PO_HEADERS_STG batch_id '
                                                 || lv_batch_id
                                                 || ':'
                                                 || sqlerrm);
                            RETURN;
                    END;
                ELSE
                    dbms_output.put_line('Process Stopped for XXCNV_PO_C007_PO_HEADERS_STG batch_id '
                                         || lv_batch_id
                                         || ': Error message columns contain data.');
                END IF;

            END LOOP;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
                RETURN;
        END;

--table 2

        BEGIN
            FOR g_id IN fbatch_id_cursor_lines LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('In create FBDI Processing Batch_ID: ' || lv_batch_id);
                BEGIN
                    SELECT
                        COUNT(*)
                    INTO lv_success_count
                    FROM
                        xxcnv_po_c007_po_lines_stg   l,
                        xxcnv_po_c007_po_headers_stg h
                    WHERE
                            l.interface_header_key = h.interface_header_key
                        AND h.batch_id = lv_batch_id
                        AND l.file_reference_identifier = gv_execution_id
                                                          || '_'
                                                          || gv_status_success;

                    dbms_output.put_line('Success count for XXCNV_PO_C007_PO_LINES_STG batch_id '
                                         || lv_batch_id
                                         || ':'
                                         || lv_success_count);
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for XXCNV_PO_C007_PO_LINES_STG batch_id: ' || lv_batch_id);
                    -- CONTINUE;
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error checking error_message column for batch_id '
                                             || lv_batch_id
                                             || ':'
                                             || sqlerrm);
                    -- CONTINUE;
                END;

                IF lv_success_count > 0 THEN
                    BEGIN
                        dbms_cloud.export_data(
                            credential_name => gv_credential_name,
                            file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                             || '/'
                                             || lv_batch_id
                                             || gv_oci_file_name_lines,
                        -- FILE_URI_LIST   => 'https://objectstorage.us-ashburn-1.oraclecloud.com/n/nacaus19b/b/O2InnovationBucket/o/mock1/Item/1/TransformedFBDI/EgpItemRelationshipsIntf.csv',
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv',
                                        'header' VALUE FALSE
                                    ),
                            query           => 'SELECT 
                               INTERFACE_LINE_KEY,
INTERFACE_HEADER_KEY,
ACTION,
LINE_NUM,
LINE_TYPE,
ITEM,
ITEM_DESCRIPTION,
ITEM_REVISION,
CATEGORY,
AMOUNT,
QUANTITY,
SHIPPING_UNIT_OF_MEASURE,
UNIT_PRICE,
SECONDARY_QUANTITY,
SECONDARY_UNIT_OF_MEASURE,
VENDOR_PRODUCT_NUM,
NEGOTIATED_BY_PREPARER_FLAG,
HAZARD_CLASS,
UN_NUMBER,
NOTE_TO_VENDOR,
NOTE_TO_RECEIVER,
ATTRIBUTE_CATEGORY,
Target_ATTRIBUTE1,
Target_ATTRIBUTE2,
Target_ATTRIBUTE3,
LINE_ATTRIBUTE4,
LINE_ATTRIBUTE5,
LINE_ATTRIBUTE6,
LINE_ATTRIBUTE7,
LINE_ATTRIBUTE8,
LINE_ATTRIBUTE9,
LINE_ATTRIBUTE10,
LINE_ATTRIBUTE11,
LINE_ATTRIBUTE12,
LINE_ATTRIBUTE13,
LINE_ATTRIBUTE14,
LINE_ATTRIBUTE15,
LINE_ATTRIBUTE16,
LINE_ATTRIBUTE17,
LINE_ATTRIBUTE18,
LINE_ATTRIBUTE19,
LINE_ATTRIBUTE20,
TO_CHAR(ATTRIBUTE_DATE1, ''YYYY/MM/DD'') AS ATTRIBUTE_DATE1,
TO_CHAR(ATTRIBUTE_DATE2, ''YYYY/MM/DD'') AS ATTRIBUTE_DATE2,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
UNIT_WEIGHT,
WEIGHT_UOM_CODE,
WEIGHT_UNIT_OF_MEASURE,
UNIT_VOLUME,
VOLUME_UOM_CODE,
VOLUME_UNIT_OF_MEASURE,
TEMPLATE_NAME,
ITEM_ATTRIBUTE_CATEGORY,
ITEM_ATTRIBUTE1,
ITEM_ATTRIBUTE2,
ITEM_ATTRIBUTE3,
ITEM_ATTRIBUTE4,
ITEM_ATTRIBUTE5,
ITEM_ATTRIBUTE6,
ITEM_ATTRIBUTE7,
ITEM_ATTRIBUTE8,
ITEM_ATTRIBUTE9,
ITEM_ATTRIBUTE10,
ITEM_ATTRIBUTE11,
ITEM_ATTRIBUTE12,
ITEM_ATTRIBUTE13,
ITEM_ATTRIBUTE14,
ITEM_ATTRIBUTE15,
SOURCE_AGREEMENT_PRC_BU_NAME,
SOURCE_AGREEMENT,
SOURCE_AGREEMENT_LINE,
DISCOUNT_TYPE,
DISCOUNT,
DISCOUNT_REASON,
MAX_RETAINAGE_AMOUNT,
UNIT_OF_MEASURE,
SH_ATTRIBUTE1,
SH_ATTRIBUTE2,
SH_ATTRIBUTE3,
SH_ATTRIBUTE4,
SH_ATTRIBUTE5,
SH_ATTRIBUTE6,
SH_ATTRIBUTE7,
SH_ATTRIBUTE8,
SH_ATTRIBUTE9,
SH_ATTRIBUTE10,
SH_ATTRIBUTE11,
SH_ATTRIBUTE12,
SH_ATTRIBUTE13,
SH_ATTRIBUTE14,
SH_ATTRIBUTE15,
SH_ATTRIBUTE16,
SH_ATTRIBUTE17,
SH_ATTRIBUTE18,
SH_ATTRIBUTE19,
SH_ATTRIBUTE20,
SH_ATTRIBUTE_NUMBER1,
SH_ATTRIBUTE_NUMBER2,
SH_ATTRIBUTE_NUMBER3,
SH_ATTRIBUTE_NUMBER4,
SH_ATTRIBUTE_NUMBER5,
SH_ATTRIBUTE_NUMBER6,
SH_ATTRIBUTE_NUMBER7,
SH_ATTRIBUTE_NUMBER8,
SH_ATTRIBUTE_NUMBER9,
SH_ATTRIBUTE_NUMBER10,
SH_ATTRIBUTE_DATE1,
SH_ATTRIBUTE_DATE2,
SH_ATTRIBUTE_DATE3,
SH_ATTRIBUTE_DATE4,
SH_ATTRIBUTE_DATE5,
SH_ATTRIBUTE_DATE6,
SH_ATTRIBUTE_DATE7,
SH_ATTRIBUTE_DATE8,
SH_ATTRIBUTE_DATE9,
SH_ATTRIBUTE_DATE10,
SH_ATTRIBUTE_TIMESTAMP1,
SH_ATTRIBUTE_TIMESTAMP2,
SH_ATTRIBUTE_TIMESTAMP3,
SH_ATTRIBUTE_TIMESTAMP4,
SH_ATTRIBUTE_TIMESTAMP5,
SH_ATTRIBUTE_TIMESTAMP6,
SH_ATTRIBUTE_TIMESTAMP7,
SH_ATTRIBUTE_TIMESTAMP8,
SH_ATTRIBUTE_TIMESTAMP9,
SH_ATTRIBUTE_TIMESTAMP10


	                            FROM XXCNV_PO_C007_PO_LINES_STG
                                            WHERE import_status = '''
                                     || 'PROCESSED'
                                     || '''
											and INTERFACE_HEADER_KEY IN (SELECT INTERFACE_HEADER_KEY FROM XXCNV_PO_C007_PO_HEADERS_STG WHERE batch_id ='''
                                     || lv_batch_id
                                     || ''' AND import_status = '''
                                     || 'PROCESSED'
                                     || ''' )
											AND file_reference_identifier= '''
                                     || gv_execution_id
                                     || '_'
                                     || gv_status_success
                                     || ''''
                        );

                        dbms_output.put_line('CSV file for BATCH_ID '
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
                                           || gv_oci_file_name_lines,
                            p_attribute1        => lv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );

                    EXCEPTION
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error exporting data to CSV for XXCNV_PO_C007_PO_LINES_STG batch_id '
                                                 || lv_batch_id
                                                 || ':'
                                                 || sqlerrm);
                            RETURN;
                    END;
                ELSE
                    dbms_output.put_line('Process Stopped for XXCNV_PO_C007_PO_LINES_STG batch_id '
                                         || lv_batch_id
                                         || ': Error message columns contain data.');
                END IF;
       -- END LOOP;
            END LOOP;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
                RETURN;
        END;


--TABLE3

        BEGIN
            FOR g_id IN fbatch_id_cursor_line_loc LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('In create FBDI Processing Batch_ID: ' || lv_batch_id);
                BEGIN
                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                    SELECT
                        COUNT(*)
                    INTO lv_success_count
                    FROM
                        xxcnv_po_c007_po_line_locations_stg loc,
                        xxcnv_po_c007_po_lines_stg          l,
                        xxcnv_po_c007_po_headers_stg        h
                    WHERE
                            loc.interface_line_key = l.interface_line_key
                        AND l.interface_header_key = h.interface_header_key
                        AND h.batch_id = lv_batch_id
                        AND loc.file_reference_identifier = gv_execution_id
                                                            || '_'
                                                            || gv_status_success;

                    dbms_output.put_line('Success count for XXCNV_PO_C007_PO_LINE_LOCATIONS_STG batch_id '
                                         || lv_batch_id
                                         || ':'
                                         || lv_success_count);
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for XXCNV_PO_C007_PO_LINES_STG batch_id: ' || lv_batch_id);
                    -- CONTINUE;
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error checking error_message column for batch_id '
                                             || lv_batch_id
                                             || ':'
                                             || sqlerrm);
                    -- CONTINUE;
                END;

                IF lv_success_count > 0 THEN
                    BEGIN
                        dbms_cloud.export_data(
                            credential_name => gv_credential_name,
                            file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                             || '/'
                                             || lv_batch_id
                                             || gv_oci_file_name_line_locations,
                        -- FILE_URI_LIST   => 'https://objectstorage.us-ashburn-1.oraclecloud.com/n/nacaus19b/b/O2InnovationBucket/o/mock1/Item/1/TransformedFBDI/EgoItemAssociationsIntf.csv',
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv',
                                        'header' VALUE FALSE
                                    ),
                            query           => 'SELECT 
                           INTERFACE_LINE_LOCATION_KEY,
INTERFACE_LINE_KEY,
SHIPMENT_NUM,
SHIP_TO_LOCATION,
SHIP_TO_ORGANIZATION_CODE,
AMOUNT,
SHIPPING_UOM_QUANTITY,
TO_CHAR(NEED_BY_DATE, ''YYYY/MM/DD'') AS NEED_BY_DATE,
TO_CHAR(PROMISED_DATE, ''YYYY/MM/DD'') AS PROMISED_DATE,
SECONDARY_QUANTITY,
SECONDARY_UNIT_OF_MEASURE,
DESTINATION_TYPE_CODE,
ACCRUE_ON_RECEIPT_FLAG,
ALLOW_SUBSTITUTE_RECEIPTS_FLAG,
ASSESSABLE_VALUE,
DAYS_EARLY_RECEIPT_ALLOWED,
DAYS_LATE_RECEIPT_ALLOWED,
ENFORCE_SHIP_TO_LOCATION_CODE,
INSPECTION_REQUIRED_FLAG,
RECEIPT_REQUIRED_FLAG,
INVOICE_CLOSE_TOLERANCE,
RECEIVE_CLOSE_TOLERANCE,
QTY_RCV_TOLERANCE,
QTY_RCV_EXCEPTION_CODE,
RECEIPT_DAYS_EXCEPTION_CODE,
RECEIVING_ROUTING,
NOTE_TO_RECEIVER,
INPUT_TAX_CLASSIFICATION_CODE,
LINE_INTENDED_USE,
PRODUCT_CATEGORY,
PRODUCT_FISC_CLASSIFICATION,
PRODUCT_TYPE,
TRX_BUSINESS_CATEGORY,
USER_DEFINED_FISC_CLASS,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
FRIGHT_CARRIER,
MODE_OF_TRANSPORT,
SERVICE_LEVEL,
FINAL_DISCHARGE_LOCATION_CODE,
REQUESTED_SHIP_DATE,
PROMISED_SHIP_DATE,
TO_CHAR(REQUESTED_DELIVERY_DATE, ''YYYY/MM/DD'') AS REQUESTED_DELIVERY_DATE,
PROMISED_DELIVERY_DATE,
RETAINAGE_RATE,
INVOICE_MATCH_OPTION

FROM XXCNV_PO_C007_PO_LINE_LOCATIONS_STG LOC
                                            WHERE LOC.import_status = '''
                                     || 'PROCESSED'
                                     || '''
											and LOC.INTERFACE_LINE_KEY IN (SELECT L.INTERFACE_LINE_KEY FROM XXCNV_PO_C007_PO_LINES_STG L
                                            JOIN XXCNV_PO_C007_PO_HEADERS_STG H ON L.INTERFACE_HEADER_KEY= H.INTERFACE_HEADER_KEY
                                            WHERE H.batch_id ='''
                                     || lv_batch_id
                                     || '''and H.import_status = '''
                                     || 'PROCESSED'
                                     || ''' AND L.import_status = '''
                                     || 'PROCESSED'
                                     || ''' )
											AND file_reference_identifier= '''
                                     || gv_execution_id
                                     || '_'
                                     || gv_status_success
                                     || ''''
                        );

                        dbms_output.put_line('CSV file for BATCH_ID '
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
                                           || gv_oci_file_name_line_locations,
                            p_attribute1        => lv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );

                    EXCEPTION
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error exporting data to CSV for XXCNV_PO_C007_PO_LINE_LOCATIONS_STG batch_id '
                                                 || lv_batch_id
                                                 || ':'
                                                 || sqlerrm);
                            RETURN;
                    END;
                ELSE
                    dbms_output.put_line('Process Stopped for XXCNV_PO_C007_PO_LINE_LOCATIONS_STG batch_id '
                                         || lv_batch_id
                                         || ': Error message columns contain data.');
                END IF;
        -- END LOOP;



            END LOOP;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
            --EXIT;

        END;


	--table 4

        BEGIN
            FOR g_id IN fbatch_id_cursor_dis LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('In create FBDI Processing Batch_ID: ' || lv_batch_id);
                BEGIN 
                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                    SELECT
                        COUNT(*)
                    INTO lv_success_count
                    FROM
                        xxcnv_po_c007_po_distributions_stg  d,
                        xxcnv_po_c007_po_line_locations_stg loc,
                        xxcnv_po_c007_po_lines_stg          l,
                        xxcnv_po_c007_po_headers_stg        h
                    WHERE
                            d.interface_line_location_key = loc.interface_line_location_key
                        AND loc.interface_line_key = l.interface_line_key
                        AND l.interface_header_key = h.interface_header_key
                        AND h.batch_id = lv_batch_id
                        AND d.file_reference_identifier = gv_execution_id
                                                          || '_'
                                                          || gv_status_success;

                    dbms_output.put_line('Success count for XXCNV_PO_C007_PO_DISTRIBUTIONS_STG batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || lv_success_count);
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for XXCNV_PO_C007_PO_DISTRIBUTIONS_STG batch_id: ' || lv_batch_id);
                    -- CONTINUE;
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error checking error_message column for batch_id '
                                             || lv_batch_id
                                             || ':'
                                             || sqlerrm);
                    -- CONTINUE;
                END;

                IF lv_success_count > 0 THEN
                    BEGIN
                        dbms_cloud.export_data(
                            credential_name => gv_credential_name,
                            file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                             || '/'
                                             || lv_batch_id
                                             || gv_oci_file_name_distributions,
                        -- FILE_URI_LIST   => 'https://objectstorage.us-ashburn-1.oraclecloud.com/n/nacaus19b/b/O2InnovationBucket/o/mock1/Item/1/TransformedFBDI/EgpItemRelationshipsIntf.csv',
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv',
                                        'header' VALUE FALSE
                                    ),
                            query           => 'SELECT 

D.INTERFACE_DISTRIBUTION_KEY,
D.INTERFACE_LINE_LOCATION_KEY,
D.DISTRIBUTION_NUM,
D.DELIVER_TO_LOCATION,
D.DELIVER_TO_PERSON_FULL_NAME,
D.DESTINATION_SUBINVENTORY,
D.AMOUNT_ORDERED,
D.SHIPPING_UOM_QUANTITY,
 D.target_SEGMENT1,
 D.target_SEGMENT2,
 D.target_SEGMENT3,
 D.target_SEGMENT4,
 D.target_SEGMENT5,
 D.target_SEGMENT6,
 D.target_SEGMENT7,
 D.target_SEGMENT8,
 D.target_SEGMENT9,
 D.target_SEGMENT10,
D.CHARGE_ACCOUNT_SEGMENT11,
D.CHARGE_ACCOUNT_SEGMENT12,
D.CHARGE_ACCOUNT_SEGMENT13,
D.CHARGE_ACCOUNT_SEGMENT14,
D.CHARGE_ACCOUNT_SEGMENT15,
D.CHARGE_ACCOUNT_SEGMENT16,
D.CHARGE_ACCOUNT_SEGMENT17,
D.CHARGE_ACCOUNT_SEGMENT18,
D.CHARGE_ACCOUNT_SEGMENT19,
D.CHARGE_ACCOUNT_SEGMENT20,
D.CHARGE_ACCOUNT_SEGMENT21,
D.CHARGE_ACCOUNT_SEGMENT22,
D.CHARGE_ACCOUNT_SEGMENT23,
D.CHARGE_ACCOUNT_SEGMENT24,
D.CHARGE_ACCOUNT_SEGMENT25,
D.CHARGE_ACCOUNT_SEGMENT26,
D.CHARGE_ACCOUNT_SEGMENT27,
D.CHARGE_ACCOUNT_SEGMENT28,
D.CHARGE_ACCOUNT_SEGMENT29,
D.CHARGE_ACCOUNT_SEGMENT30,
D.DESTINATION_CONTEXT,
D.PROJECT,
D.TASK,
TO_CHAR(PJC_EXPENDITURE_ITEM_DATE, ''YYYY/MM/DD'') AS PJC_EXPENDITURE_ITEM_DATE,
D.EXPENDITURE_TYPE,
D.EXPENDITURE_ORGANIZATION,
D.PJC_BILLABLE_FLAG,
D.PJC_CAPITALIZABLE_FLAG,
D.PJC_WORK_TYPE,
D.PJC_RESERVED_ATTRIBUTE1,
D.PJC_RESERVED_ATTRIBUTE2,
D.PJC_RESERVED_ATTRIBUTE3,
D.PJC_RESERVED_ATTRIBUTE4,
D.PJC_RESERVED_ATTRIBUTE5,
D.PJC_RESERVED_ATTRIBUTE6,
D.PJC_RESERVED_ATTRIBUTE7,
D.PJC_RESERVED_ATTRIBUTE8,
D.PJC_RESERVED_ATTRIBUTE9,
D.PJC_RESERVED_ATTRIBUTE10,
D.PJC_USER_DEF_ATTRIBUTE1,
D.PJC_USER_DEF_ATTRIBUTE2,
D.PJC_USER_DEF_ATTRIBUTE3,
D.PJC_USER_DEF_ATTRIBUTE4,
D.PJC_USER_DEF_ATTRIBUTE5,
D.PJC_USER_DEF_ATTRIBUTE6,
D.PJC_USER_DEF_ATTRIBUTE7,
D.PJC_USER_DEF_ATTRIBUTE8,
D.PJC_USER_DEF_ATTRIBUTE9,
D.PJC_USER_DEF_ATTRIBUTE10,
D.RATE,
D.RATE_DATE,
D.ATTRIBUTE_CATEGORY,
D.ATTRIBUTE1,
D.ATTRIBUTE2,
D.ATTRIBUTE3,
D.ATTRIBUTE4,
D.ATTRIBUTE5,
D.ATTRIBUTE6,
D.ATTRIBUTE7,
D.ATTRIBUTE8,
D.ATTRIBUTE9,
D.ATTRIBUTE10,
D.ATTRIBUTE11,
D.ATTRIBUTE12,
D.ATTRIBUTE13,
D.ATTRIBUTE14,
D.ATTRIBUTE15,
D.ATTRIBUTE16,
D.ATTRIBUTE17,
D.ATTRIBUTE18,
D.ATTRIBUTE19,
D.ATTRIBUTE20,
D.ATTRIBUTE_DATE1,
D.ATTRIBUTE_DATE2,
D.ATTRIBUTE_DATE3,
D.ATTRIBUTE_DATE4,
D.ATTRIBUTE_DATE5,
D.ATTRIBUTE_DATE6,
D.ATTRIBUTE_DATE7,
D.ATTRIBUTE_DATE8,
D.ATTRIBUTE_DATE9,
D.ATTRIBUTE_DATE10,
D.ATTRIBUTE_NUMBER1,
D.ATTRIBUTE_NUMBER2,
D.ATTRIBUTE_NUMBER3,
D.ATTRIBUTE_NUMBER4,
D.ATTRIBUTE_NUMBER5,
D.ATTRIBUTE_NUMBER6,
D.ATTRIBUTE_NUMBER7,
D.ATTRIBUTE_NUMBER8,
D.ATTRIBUTE_NUMBER9,
D.ATTRIBUTE_NUMBER10,
D.ATTRIBUTE_TIMESTAMP1,
D.ATTRIBUTE_TIMESTAMP2,
D.ATTRIBUTE_TIMESTAMP3,
D.ATTRIBUTE_TIMESTAMP4,
D.ATTRIBUTE_TIMESTAMP5,
D.ATTRIBUTE_TIMESTAMP6,
D.ATTRIBUTE_TIMESTAMP7,
D.ATTRIBUTE_TIMESTAMP8,
D.ATTRIBUTE_TIMESTAMP9,
D.ATTRIBUTE_TIMESTAMP10,
D.DELIVER_TO_PERSON_EMAIL_ADDR,
D.BUDGET_DATE,
D.PJC_CONTRACT_NUMBER,
D.PJC_FUNDING_SOURCE,
D.GLOBAL_ATTRIBUTE1
	                   FROM XXCNV_PO_C007_PO_DISTRIBUTIONS_STG D
                                JOIN XXCNV_PO_C007_PO_LINE_LOCATIONS_STG LOC 
                                ON D.INTERFACE_LINE_LOCATION_KEY = LOC.INTERFACE_LINE_LOCATION_KEY
                                JOIN XXCNV_PO_C007_PO_LINES_STG L
                                ON LOC.INTERFACE_LINE_KEY = L.INTERFACE_LINE_KEY
                                JOIN XXCNV_PO_C007_PO_HEADERS_STG H
                                ON L.INTERFACE_HEADER_KEY = H.INTERFACE_HEADER_KEY
                                WHERE D.import_status = '''
                                     || 'PROCESSED'
                                     || '''
                                AND H.import_status = '''
                                     || 'PROCESSED'
                                     || '''
                                AND L.import_status = '''
                                     || 'PROCESSED'
                                     || '''
                                AND LOC.import_status = '''
                                     || 'PROCESSED'
                                     || '''

                                       AND H.BATCH_ID = '''
                                     || lv_batch_id
                                     || '''
											AND D.file_reference_identifier= '''
                                     || gv_execution_id
                                     || '_'
                                     || gv_status_success
                                     || ''''
                        );

                        dbms_output.put_line('CSV file for BATCH_ID '
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
                                           || gv_oci_file_name_distributions,
                            p_attribute1        => lv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );

                    EXCEPTION
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error exporting data to CSV for XXCNV_PO_C007_PO_DISTRIBUTIONS_STG batch_id '
                                                 || lv_batch_id
                                                 || ':'
                                                 || sqlerrm);
                            RETURN;
                    END;
                ELSE
                    dbms_output.put_line('Process Stopped for XXCNV_PO_C007_PO_DISTRIBUTIONS_STG batch_id '
                                         || lv_batch_id
                                         || ': Error message columns contain data.');
                END IF;
        -- END LOOP;
            END LOOP;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
                RETURN;
        END;

--END;
    END create_fbdi_file_prc;


/*==============================================================================================================================
-- PROCEDURE : CREATE_RECON_REPORT_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating properties file.
================================================================================================================================= */

    PROCEDURE create_recon_report_prc IS

        lv_error_count NUMBER;
        lv_batch_id    NUMBER;
        CURSOR batch_id_cursor IS
        SELECT DISTINCT
            batch_id
        INTO lv_batch_id
        FROM
            xxcnv_po_c007_po_headers_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_cursor_lines IS
        SELECT DISTINCT
            h.batch_id
        INTO lv_batch_id
        FROM
            xxcnv_po_c007_po_headers_stg h,
            xxcnv_po_c007_po_lines_stg   l
        WHERE
                h.interface_header_key = l.interface_header_key
            AND l.execution_id = gv_execution_id
            AND l.file_reference_identifier = gv_execution_id
                                              || '_'
                                              || gv_status_failure;

        CURSOR batch_id_cursor_line_loc IS
        SELECT DISTINCT
            h.batch_id
        INTO lv_batch_id
        FROM
            xxcnv_po_c007_po_headers_stg        h,
            xxcnv_po_c007_po_lines_stg          l,
            xxcnv_po_c007_po_line_locations_stg loc
        WHERE
                h.interface_header_key = l.interface_header_key
            AND l.interface_line_key = loc.interface_line_key
            AND loc.execution_id = gv_execution_id
            AND loc.file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure;

        CURSOR batch_id_cursor_dis IS
        SELECT DISTINCT
            h.batch_id
        INTO lv_batch_id
        FROM
            xxcnv_po_c007_po_headers_stg        h,
            xxcnv_po_c007_po_lines_stg          l,
            xxcnv_po_c007_po_line_locations_stg loc,
            xxcnv_po_c007_po_distributions_stg  d
        WHERE
                h.interface_header_key = l.interface_header_key
            AND l.interface_line_key = loc.interface_line_key
            AND loc.interface_line_location_key = d.interface_line_location_key
            AND d.execution_id = gv_execution_id
            AND d.file_reference_identifier = gv_execution_id
                                              || '_'
                                              || gv_status_failure;

    BEGIN
        BEGIN
            FOR g_id IN batch_id_cursor LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing recon report for batch_id: '
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
                                         || gv_oci_file_name_headers,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
INTERFACE_HEADER_KEY,
ACTION,
BATCH_ID,
IMPORT_SOURCE_CODE,
APPROVAL_ACTION,
DOCUMENT_NUM,
DOCUMENT_TYPE_CODE,
STYLE_DISPLAY_NAME,
PRC_BU_NAME,
REQ_BU_NAME,
SOLDTO_LE_NAME,
BILLTO_BU_NAME,
AGENT_NAME,
CURRENCY_CODE,
RATE,
RATE_TYPE,
RATE_DATE,
COMMENTS,
BILL_TO_LOCATION,
SHIP_TO_LOCATION,
VENDOR_NAME,
VENDOR_NUM,
SUPPLIER_SITE_CODE,
VENDOR_CONTACT,
VENDOR_DOC_NUM,
FOB,
FREIGHT_CARRIER,
FREIGHT_TERMS,
PAY_ON_CODE,
PAYMENT_TERMS,
ORIGINATOR_ROLE,
CHANGE_ORDER_DESC,
ACCEPTANCE_REQUIRED_FLAG,
ACCEPTANCE_WITHIN_DAYS,
SUPPLIER_NOTIF_METHOD,
FAX,
EMAIL_ADDRESS,
CONFIRMING_ORDER_FLAG,
NOTE_TO_VENDOR,
NOTE_TO_RECEIVER,
DEFAULT_TAXATION_COUNTRY_CODE,
TAX_DOCUMENT_SUBTYPE,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
AGENT_EMAIL_ADDRESS,
MODE_OF_TRANSPORT,
SERVICE_LEVEL,
FIRST_PTY_REG_NUM,
THIRD_PTY_REG_NUM,
BUYER_MANAGED_TRANSPORT_FLAG,
MASTER_CONTRACT_NUMBER,
MASTER_CONTRACT_TYPE,
CC_EMAIL_ADDRESS,
BCC_EMAIL_ADDRESS,
GLOBAL_ATTRIBUTE1,
GLOBAL_ATTRIBUTE2,
GLOBAL_ATTRIBUTE3,
GLOBAL_ATTRIBUTE4,
GLOBAL_ATTRIBUTE5,
GLOBAL_ATTRIBUTE6,
OVERRIDING_APPROVER_NAME,
SKIP_ELECTRONIC_COMM_FLAG,
CHECKLIST_TITLE,
CHECKLIST_NUM,
ALT_CONTACT_EMAIL_ADDRESS,
SPECIAL_HANDLING_TYPE,
SH_ATTRIBUTE1,
SH_ATTRIBUTE2,
SH_ATTRIBUTE3,
SH_ATTRIBUTE4,
SH_ATTRIBUTE5,
SH_ATTRIBUTE6,
SH_ATTRIBUTE7,
SH_ATTRIBUTE8,
SH_ATTRIBUTE9,
SH_ATTRIBUTE10,
SH_ATTRIBUTE11,
SH_ATTRIBUTE12,
SH_ATTRIBUTE13,
SH_ATTRIBUTE14,
SH_ATTRIBUTE15,
SH_ATTRIBUTE16,
SH_ATTRIBUTE17,
SH_ATTRIBUTE18,
SH_ATTRIBUTE19,
SH_ATTRIBUTE20,
SH_ATTRIBUTE_NUMBER1,
SH_ATTRIBUTE_NUMBER2,
SH_ATTRIBUTE_NUMBER3,
SH_ATTRIBUTE_NUMBER4,
SH_ATTRIBUTE_NUMBER5,
SH_ATTRIBUTE_NUMBER6,
SH_ATTRIBUTE_NUMBER7,
SH_ATTRIBUTE_NUMBER8,
SH_ATTRIBUTE_NUMBER9,
SH_ATTRIBUTE_NUMBER10,
SH_ATTRIBUTE_DATE1,
SH_ATTRIBUTE_DATE2,
SH_ATTRIBUTE_DATE3,
SH_ATTRIBUTE_DATE4,
SH_ATTRIBUTE_DATE5,
SH_ATTRIBUTE_DATE6,
SH_ATTRIBUTE_DATE7,
SH_ATTRIBUTE_DATE8,
SH_ATTRIBUTE_DATE9,
SH_ATTRIBUTE_DATE10,
SH_ATTRIBUTE_TIMESTAMP1,
SH_ATTRIBUTE_TIMESTAMP2,
SH_ATTRIBUTE_TIMESTAMP3,
SH_ATTRIBUTE_TIMESTAMP4,
SH_ATTRIBUTE_TIMESTAMP5,
SH_ATTRIBUTE_TIMESTAMP6,
SH_ATTRIBUTE_TIMESTAMP7,
SH_ATTRIBUTE_TIMESTAMP8,
SH_ATTRIBUTE_TIMESTAMP9,
SH_ATTRIBUTE_TIMESTAMP10,

						FILE_NAME,
						ERROR_MESSAGE,
						IMPORT_STATUS,						
                        SOURCE_SYSTEM
                                            FROM XXCNV_PO_C007_PO_HEADERS_STG
                                            where execution_id  =  '''
                                 || gv_execution_id
                                 || '''
											AND import_status = '''
                                 || 'ERROR'
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for BATCH_ID '
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
                                       || gv_oci_file_name_headers,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for  XXCNV_PO_C007_PO_HEADERS_STG batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        -- RETURN;
                END;

            END LOOP;
        END;


--table 2	


        BEGIN
            FOR g_id IN batch_id_cursor_lines LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing recon report for batch_id: '
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
                                         || gv_oci_file_name_lines,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
                                           INTERFACE_LINE_KEY,
INTERFACE_HEADER_KEY,
ACTION,
LINE_NUM,
LINE_TYPE,
ITEM,
ITEM_DESCRIPTION,
ITEM_REVISION,
CATEGORY,
AMOUNT,
QUANTITY,
SHIPPING_UNIT_OF_MEASURE,
UNIT_PRICE,
SECONDARY_QUANTITY,
SECONDARY_UNIT_OF_MEASURE,
VENDOR_PRODUCT_NUM,
NEGOTIATED_BY_PREPARER_FLAG,
HAZARD_CLASS,
UN_NUMBER,
NOTE_TO_VENDOR,
NOTE_TO_RECEIVER,
ATTRIBUTE_CATEGORY,
LINE_ATTRIBUTE1,
LINE_ATTRIBUTE2,
LINE_ATTRIBUTE3,
LINE_ATTRIBUTE4,
LINE_ATTRIBUTE5,
LINE_ATTRIBUTE6,
LINE_ATTRIBUTE7,
LINE_ATTRIBUTE8,
LINE_ATTRIBUTE9,
LINE_ATTRIBUTE10,
LINE_ATTRIBUTE11,
LINE_ATTRIBUTE12,
LINE_ATTRIBUTE13,
LINE_ATTRIBUTE14,
LINE_ATTRIBUTE15,
LINE_ATTRIBUTE16,
LINE_ATTRIBUTE17,
LINE_ATTRIBUTE18,
LINE_ATTRIBUTE19,
LINE_ATTRIBUTE20,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
UNIT_WEIGHT,
WEIGHT_UOM_CODE,
WEIGHT_UNIT_OF_MEASURE,
UNIT_VOLUME,
VOLUME_UOM_CODE,
VOLUME_UNIT_OF_MEASURE,
TEMPLATE_NAME,
ITEM_ATTRIBUTE_CATEGORY,
ITEM_ATTRIBUTE1,
ITEM_ATTRIBUTE2,
ITEM_ATTRIBUTE3,
ITEM_ATTRIBUTE4,
ITEM_ATTRIBUTE5,
ITEM_ATTRIBUTE6,
ITEM_ATTRIBUTE7,
ITEM_ATTRIBUTE8,
ITEM_ATTRIBUTE9,
ITEM_ATTRIBUTE10,
ITEM_ATTRIBUTE11,
ITEM_ATTRIBUTE12,
ITEM_ATTRIBUTE13,
ITEM_ATTRIBUTE14,
ITEM_ATTRIBUTE15,
SOURCE_AGREEMENT_PRC_BU_NAME,
SOURCE_AGREEMENT,
SOURCE_AGREEMENT_LINE,
DISCOUNT_TYPE,
DISCOUNT,
DISCOUNT_REASON,
MAX_RETAINAGE_AMOUNT,
UNIT_OF_MEASURE,
SH_ATTRIBUTE1,
SH_ATTRIBUTE2,
SH_ATTRIBUTE3,
SH_ATTRIBUTE4,
SH_ATTRIBUTE5,
SH_ATTRIBUTE6,
SH_ATTRIBUTE7,
SH_ATTRIBUTE8,
SH_ATTRIBUTE9,
SH_ATTRIBUTE10,
SH_ATTRIBUTE11,
SH_ATTRIBUTE12,
SH_ATTRIBUTE13,
SH_ATTRIBUTE14,
SH_ATTRIBUTE15,
SH_ATTRIBUTE16,
SH_ATTRIBUTE17,
SH_ATTRIBUTE18,
SH_ATTRIBUTE19,
SH_ATTRIBUTE20,
SH_ATTRIBUTE_NUMBER1,
SH_ATTRIBUTE_NUMBER2,
SH_ATTRIBUTE_NUMBER3,
SH_ATTRIBUTE_NUMBER4,
SH_ATTRIBUTE_NUMBER5,
SH_ATTRIBUTE_NUMBER6,
SH_ATTRIBUTE_NUMBER7,
SH_ATTRIBUTE_NUMBER8,
SH_ATTRIBUTE_NUMBER9,
SH_ATTRIBUTE_NUMBER10,
SH_ATTRIBUTE_DATE1,
SH_ATTRIBUTE_DATE2,
SH_ATTRIBUTE_DATE3,
SH_ATTRIBUTE_DATE4,
SH_ATTRIBUTE_DATE5,
SH_ATTRIBUTE_DATE6,
SH_ATTRIBUTE_DATE7,
SH_ATTRIBUTE_DATE8,
SH_ATTRIBUTE_DATE9,
SH_ATTRIBUTE_DATE10,
SH_ATTRIBUTE_TIMESTAMP1,
SH_ATTRIBUTE_TIMESTAMP2,
SH_ATTRIBUTE_TIMESTAMP3,
SH_ATTRIBUTE_TIMESTAMP4,
SH_ATTRIBUTE_TIMESTAMP5,
SH_ATTRIBUTE_TIMESTAMP6,
SH_ATTRIBUTE_TIMESTAMP7,
SH_ATTRIBUTE_TIMESTAMP8,
SH_ATTRIBUTE_TIMESTAMP9,
SH_ATTRIBUTE_TIMESTAMP10,


						FILE_NAME,
						ERROR_MESSAGE,
						IMPORT_STATUS,						
                        SOURCE_SYSTEM 
                                            FROM XXCNV_PO_C007_PO_LINES_STG
                                            where execution_id  = '''
                                 || gv_execution_id
                                 || '''
											AND import_status = '''
                                 || 'ERROR'
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for BATCH_ID '
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
                                       || gv_oci_file_name_lines,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for XXCNV_PO_C007_PO_LINES_STG batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        -- RETURN;
                END;

            END LOOP;

        END;

--table3




        BEGIN
            FOR g_id IN batch_id_cursor_line_loc LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing recon report for batch_id: '
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
                                         || gv_oci_file_name_line_locations,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
                                            INTERFACE_LINE_LOCATION_KEY,
INTERFACE_LINE_KEY,
SHIPMENT_NUM,
SHIP_TO_LOCATION,
SHIP_TO_ORGANIZATION_CODE,
AMOUNT,
SHIPPING_UOM_QUANTITY,
NEED_BY_DATE,
PROMISED_DATE,
SECONDARY_QUANTITY,
SECONDARY_UNIT_OF_MEASURE,
DESTINATION_TYPE_CODE,
ACCRUE_ON_RECEIPT_FLAG,
ALLOW_SUBSTITUTE_RECEIPTS_FLAG,
ASSESSABLE_VALUE,
DAYS_EARLY_RECEIPT_ALLOWED,
DAYS_LATE_RECEIPT_ALLOWED,
ENFORCE_SHIP_TO_LOCATION_CODE,
INSPECTION_REQUIRED_FLAG,
RECEIPT_REQUIRED_FLAG,
INVOICE_CLOSE_TOLERANCE,
RECEIVE_CLOSE_TOLERANCE,
QTY_RCV_TOLERANCE,
QTY_RCV_EXCEPTION_CODE,
RECEIPT_DAYS_EXCEPTION_CODE,
RECEIVING_ROUTING,
NOTE_TO_RECEIVER,
INPUT_TAX_CLASSIFICATION_CODE,
LINE_INTENDED_USE,
PRODUCT_CATEGORY,
PRODUCT_FISC_CLASSIFICATION,
PRODUCT_TYPE,
TRX_BUSINESS_CATEGORY,
USER_DEFINED_FISC_CLASS,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
FRIGHT_CARRIER,
MODE_OF_TRANSPORT,
SERVICE_LEVEL,
FINAL_DISCHARGE_LOCATION_CODE,
REQUESTED_SHIP_DATE,
PROMISED_SHIP_DATE,
REQUESTED_DELIVERY_DATE,
PROMISED_DELIVERY_DATE,
RETAINAGE_RATE,
INVOICE_MATCH_OPTION,




						FILE_NAME,
						ERROR_MESSAGE,
						IMPORT_STATUS,						
                        SOURCE_SYSTEM 
                                            FROM XXCNV_PO_C007_PO_LINE_LOCATIONS_STG
                                            where execution_id  = '''
                                 || gv_execution_id
                                 || '''
											AND import_status = '''
                                 || 'ERROR'
                                 || ''''
                    );

                    dbms_output.put_line('Recon file for BATCH_ID '
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
                                       || gv_oci_file_name_line_locations,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for XXCNV_PO_C007_PO_LINE_LOCATIONS_STG batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        -- RETURN;
                END;

            END LOOP;
        END;


	--table4



        BEGIN
            FOR g_id IN batch_id_cursor_dis LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Processing recon report for batch_id: '
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
                                         || gv_oci_file_name_distributions,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'header' VALUE FALSE
                                ),
                        query           => 'SELECT 
                                           INTERFACE_DISTRIBUTION_KEY,
INTERFACE_LINE_LOCATION_KEY,
DISTRIBUTION_NUM,
DELIVER_TO_LOCATION,
DELIVER_TO_PERSON_FULL_NAME,
DESTINATION_SUBINVENTORY,
AMOUNT_ORDERED,
SHIPPING_UOM_QUANTITY,
CHARGE_ACCOUNT_SEGMENT1,
CHARGE_ACCOUNT_SEGMENT2,
CHARGE_ACCOUNT_SEGMENT3,
CHARGE_ACCOUNT_SEGMENT4,
CHARGE_ACCOUNT_SEGMENT5,
CHARGE_ACCOUNT_SEGMENT6,
CHARGE_ACCOUNT_SEGMENT7,
CHARGE_ACCOUNT_SEGMENT8,
CHARGE_ACCOUNT_SEGMENT9,
CHARGE_ACCOUNT_SEGMENT10,
CHARGE_ACCOUNT_SEGMENT11,
CHARGE_ACCOUNT_SEGMENT12,
CHARGE_ACCOUNT_SEGMENT13,
CHARGE_ACCOUNT_SEGMENT14,
CHARGE_ACCOUNT_SEGMENT15,
CHARGE_ACCOUNT_SEGMENT16,
CHARGE_ACCOUNT_SEGMENT17,
CHARGE_ACCOUNT_SEGMENT18,
CHARGE_ACCOUNT_SEGMENT19,
CHARGE_ACCOUNT_SEGMENT20,
CHARGE_ACCOUNT_SEGMENT21,
CHARGE_ACCOUNT_SEGMENT22,
CHARGE_ACCOUNT_SEGMENT23,
CHARGE_ACCOUNT_SEGMENT24,
CHARGE_ACCOUNT_SEGMENT25,
CHARGE_ACCOUNT_SEGMENT26,
CHARGE_ACCOUNT_SEGMENT27,
CHARGE_ACCOUNT_SEGMENT28,
CHARGE_ACCOUNT_SEGMENT29,
CHARGE_ACCOUNT_SEGMENT30,
DESTINATION_CONTEXT,
PROJECT,
TASK,
PJC_EXPENDITURE_ITEM_DATE,
EXPENDITURE_TYPE,
EXPENDITURE_ORGANIZATION,
PJC_BILLABLE_FLAG,
PJC_CAPITALIZABLE_FLAG,
PJC_WORK_TYPE,
PJC_RESERVED_ATTRIBUTE1,
PJC_RESERVED_ATTRIBUTE2,
PJC_RESERVED_ATTRIBUTE3,
PJC_RESERVED_ATTRIBUTE4,
PJC_RESERVED_ATTRIBUTE5,
PJC_RESERVED_ATTRIBUTE6,
PJC_RESERVED_ATTRIBUTE7,
PJC_RESERVED_ATTRIBUTE8,
PJC_RESERVED_ATTRIBUTE9,
PJC_RESERVED_ATTRIBUTE10,
PJC_USER_DEF_ATTRIBUTE1,
PJC_USER_DEF_ATTRIBUTE2,
PJC_USER_DEF_ATTRIBUTE3,
PJC_USER_DEF_ATTRIBUTE4,
PJC_USER_DEF_ATTRIBUTE5,
PJC_USER_DEF_ATTRIBUTE6,
PJC_USER_DEF_ATTRIBUTE7,
PJC_USER_DEF_ATTRIBUTE8,
PJC_USER_DEF_ATTRIBUTE9,
PJC_USER_DEF_ATTRIBUTE10,
RATE,
RATE_DATE,
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
ATTRIBUTE_TIMESTAMP1,
ATTRIBUTE_TIMESTAMP2,
ATTRIBUTE_TIMESTAMP3,
ATTRIBUTE_TIMESTAMP4,
ATTRIBUTE_TIMESTAMP5,
ATTRIBUTE_TIMESTAMP6,
ATTRIBUTE_TIMESTAMP7,
ATTRIBUTE_TIMESTAMP8,
ATTRIBUTE_TIMESTAMP9,
ATTRIBUTE_TIMESTAMP10,
DELIVER_TO_PERSON_EMAIL_ADDR,
BUDGET_DATE,
PJC_CONTRACT_NUMBER,
PJC_FUNDING_SOURCE,
GLOBAL_ATTRIBUTE1,
FILE_NAME,
						ERROR_MESSAGE,
						IMPORT_STATUS,
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
						file_reference_identifier ,
                        SOURCE_SYSTEM	
                                            FROM XXCNV_PO_C007_PO_DISTRIBUTIONS_STG
                                            where execution_id  = '''
                                 || gv_execution_id
                                 || '''
											AND import_status = '''
                                 || 'ERROR'
                                 || ''''
                    );

                    dbms_output.put_line('Recon file for BATCH_ID '
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
                                       || gv_oci_file_name_distributions,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for XXCNV_PO_C007_PO_DISTRIBUTIONS_STG batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        -- RETURN;
                END;

            END LOOP;
        END;

    END create_recon_report_prc;

END xxcnv_po_c007_open_purchase_order_conversion_pkg;