CREATE OR REPLACE PACKAGE BODY xxcnv.xxcnv_po_c009_po_receipts_conversion_pkg IS
	/*************************************************************************************
    NAME              :     PO RECEIPTS CONVERSION package BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -----------------------------------------
	-- 	Phanindra 	    16-MAR-2025 	    1.0         Initial Development
	--  Phanindra       2-Aug-2025          1.1         Changes for the jira LTCI-6582
	--  Phanindra       7-Aug-2025          1.2         Changes for the jira LTCI - 6622 & LTCI - 6593
	--  Phanindra       26-Aug-2025         1.3         Changes for the Jira LTCI - 8107
	****************************************************************************************/

---Declaring global Variables

    gv_import_status           VARCHAR2(256) := NULL;
    gv_error_message           VARCHAR2(500) := NULL;
    gv_file_name               VARCHAR2(256) := NULL;
    gv_oci_file_name           VARCHAR2(4000) := NULL;
    gv_oci_file_path           VARCHAR2(200) := NULL;
    gv_oci_file_name_poheader  VARCHAR2(2000) := NULL;
    gv_oci_file_name_potrans   VARCHAR2(2000) := NULL;
    gv_oci_file_name_porecmap  VARCHAR2(2000) := NULL;
    gv_execution_id            VARCHAR2(100) := NULL;
    gv_book_type_code          VARCHAR2(50) := NULL;
    gv_interface_line_number   VARCHAR2(50) := NULL;
	 -- gv_group_id                         NUMBER(18)      := NULL;
    gv_batch_id                VARCHAR2(200) := to_char(sysdate, 'yyyymmddhhmmss');
    gv_credential_name         CONSTANT VARCHAR2(30) := 'OCI$RESOURCE_PRINCIPAL';
    gv_status_success          CONSTANT VARCHAR2(100) := 'Success';
    gv_status_failure          CONSTANT VARCHAR2(100) := 'Failure';
    gv_conversion_id           VARCHAR2(100) := NULL;
    gv_boundary_system         VARCHAR2(100) := NULL;
    gv_status_picked           CONSTANT VARCHAR2(100) := 'File_Picked_From_OCI_And_Loaded_To_Stg';
    gv_status_picked_for_tr    CONSTANT VARCHAR2(100) := 'Transformed_Data_From_Ext_To_Stg';
    gv_status_validated        CONSTANT VARCHAR2(100) := 'Validated';
    gv_status_failed           CONSTANT VARCHAR2(100) := 'Failed_At_Validation';
    gv_fbdi_export_status      CONSTANT VARCHAR2(100) := 'Exported_To_Fbdi';
    gv_fbdi_export_status_fail CONSTANT VARCHAR2(100) := 'Exported_To_Fbdi_Failed';
    gv_status_staged           CONSTANT VARCHAR2(100) := 'Staged_For_Import';
    gv_transformed_folder      CONSTANT VARCHAR2(100) := 'Transformed_FBDI_Files';
    gv_source_folder           CONSTANT VARCHAR2(100) := 'Source_FBDI_Files';
    gv_properties              CONSTANT VARCHAR2(100) := 'properties';
    gv_file_picked             VARCHAR2(100) := 'File_Picked_From_OCI_Server';
    gv_file_not_found          CONSTANT VARCHAR2(100) := 'File_not_found';
    gv_recon_folder            CONSTANT VARCHAR2(50) := 'ATP_Validation_Error_Files';
    gv_recon_report            CONSTANT VARCHAR2(50) := 'Recon_Report_Created';

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
        dbms_output.put_line('----------------------MAIN_PRC started-----------------');
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
                    WHEN lv_file_name LIKE '%RcvHeadersInterface%.csv' THEN
                        gv_oci_file_name_poheader := lv_file_name;
                    WHEN lv_file_name LIKE '%RcvTransactionsInterface%.csv' THEN
                        gv_oci_file_name_potrans := lv_file_name;
                    WHEN lv_file_name LIKE '%RcvReceiptQtyMap%.csv' THEN
                        gv_oci_file_name_porecmap := lv_file_name;
                    ELSE
                        dbms_output.put_line('No match found for file name: ' || lv_file_name); -- Debugging output
                END CASE;

                lv_start_pos := lv_end_pos + 1;
            END LOOP;

        -- Output the results for debugging
            dbms_output.put_line('lv_File Name: ' || lv_file_name);
            dbms_output.put_line('PO Receipts Headers File Name: ' || gv_oci_file_name_poheader);
            dbms_output.put_line('PO Receipts Transaction File Name: ' || gv_oci_file_name_potrans);
            dbms_output.put_line('PO Receipts Mapping File Name: ' || gv_oci_file_name_porecmap);
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error fetching execution details: ' || sqlerrm);
		--RETURN;
        END;	

   -- Call to import data from OCI to Stage table
        BEGIN
            dbms_output.put_line('----------------------IMPORT_DATA_FROM_OCI_TO_STG_PRC started-----------------');
            import_data_from_oci_to_stg_prc(p_loading_status);
            IF p_loading_status = gv_status_failure THEN
                dbms_output.put_line('Error in IMPORT_DATA_FROM_OCI_TO_STG_PRC');
                RETURN;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling IMPORT_DATA_FROM_OCI_TO_STG_PRC: ' || sqlerrm);
                RETURN;
                dbms_output.put_line('----------------------IMPORT_DATA_FROM_OCI_TO_STG_PRC ended-----------------');
        END;

    -- Call to perform data and business validations in staging table
        BEGIN
            dbms_output.put_line('----------------------DATA_VALIDATIONS_PRC started-----------------');
            data_validations_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling data_validations_prc: ' || sqlerrm);
                RETURN;
                dbms_output.put_line('----------------------DATA_VALIDATIONS_PRC ended-----------------');
        END;

	-- Call to perform data and business validations in staging table
        BEGIN
            dbms_output.put_line('----------------------rec_qty_update_prc started-----------------');
            rec_qty_update_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling rec_qty_update_prc: ' || sqlerrm);
                RETURN;
                dbms_output.put_line('----------------------rec_qty_update_prc ended-----------------');
        END;

    -- Call to create a CSV file from XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG after all validations
        BEGIN
            dbms_output.put_line('----------------------CREATE_FBDI_FILE_PRC started-----------------');
            create_fbdi_file_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_fbdi_file_prc: ' || sqlerrm);
                RETURN;
                dbms_output.put_line('----------------------CREATE_FBDI_FILE_PRC ended-----------------');
        END;

        ---create a atp recon report
        BEGIN
            dbms_output.put_line('----------------------CREATE_RECON_REPORT_PRC started-----------------');
            create_recon_report_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling CREATE_RECON_REPORT_PRC: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                RETURN;
                dbms_output.put_line('----------------------CREATE_RECON_REPORT_PRC ended-----------------');
        END;

        dbms_output.put_line('----------------------MAIN_PRC ended-----------------');
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
       -- Check if the external table exists and drop it if it does
                SELECT
                    COUNT(*)
                INTO lv_table_count
                FROM
                    all_objects
                WHERE
                        upper(object_name) = 'XXCNV_PO_C009_PO_RECEIPTS_HEADERS_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE XXCNV_PO_C009_PO_RECEIPTS_HEADERS_EXT';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG';
                    dbms_output.put_line('Table xxcnv_po_c009_po_receipts_headers_ext dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_po_c009_po_receipts_headers_ext: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
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
                        upper(object_name) = 'XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_EXT';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG';
                    dbms_output.put_line('Table xxcnv_po_c009_po_receipts_transactions_ext dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table xxcnv_po_c009_po_receipts_transactions_ext: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
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
                        upper(object_name) = 'XXCNV_PO_RECEIPTS_QUANTITY_MAPPING_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE XXCNV_PO_RECEIPTS_QUANTITY_MAPPING_EXT';
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_PO_RECEIPTS_QUANTITY_MAPPING';
                    dbms_output.put_line('Table XXCNV_PO_RECEIPTS_QUANTITY_MAPPING_EXT dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table XXCNV_PO_RECEIPTS_QUANTITY_MAPPING_EXT: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    p_loading_status := gv_status_failure;
            END;

        END;	

-- Create the external table
        BEGIN
            IF gv_oci_file_name_poheader LIKE '%RcvHeadersInterface.csv%' THEN
                dbms_output.put_line('Creating external table xxcnv_po_c009_po_receipts_headers_ext');
                dbms_output.put_line(' xxcnv_po_c009_po_receipts_headers_ext : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_poheader);
                dbms_cloud.create_external_table(
                    table_name      => 'xxcnv_po_c009_po_receipts_headers_ext',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_poheader,
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
                    column_list     => 'HEADER_INTERFACE_NUM         VARCHAR2(30)                            ,
                RECEIPT_SOURCE_CODE          VARCHAR2(25)                            ,
                ASN_TYPE                     VARCHAR2(25)                            ,
                TRANSACTION_TYPE             VARCHAR2(25)                            ,
				NOTICE_CREATION_DATE         VARCHAR2(30)                            ,
                SHIPMENT_NUM                 VARCHAR2(30)                            ,
                RECEIPT_NUM                  VARCHAR2(30)                            ,
                VENDOR_NAME                  VARCHAR2(240)                           ,
                VENDOR_NUM                   VARCHAR2(30)                            ,
                VENDOR_SITE_CODE             VARCHAR2(240)                           ,
                FROM_ORGANIZATION_CODE       VARCHAR2(18)                            ,
                LOCATION_CODE                VARCHAR2(60)                            ,
                BILL_OF_LADING               VARCHAR2(25)                            ,
                PACKING_SLIP                 VARCHAR2(25)                            ,
                SHIPPED_DATE                 DATE                                    ,
                FREIGHT_CARRIER_NAME         VARCHAR2(360)                           ,
                EXPECTED_RECEIPT_DATE        VARCHAR2(50)                                    ,
                NUM_OF_CONTAINERS            NUMBER                                  ,
                WAYBILL_AIRBILL_NUM          VARCHAR2(20)                            ,
                COMMENTS                     VARCHAR2(4000)                          ,
                GROSS_WEIGHT                 NUMBER                                  ,
                GROSS_WEIGHT_UNIT_OF_MEASURE VARCHAR2(25)                            ,
                NET_WEIGHT                   NUMBER                                  ,
                NET_WEIGHT_UNIT_OF_MEASURE   VARCHAR2(25)                            ,
                TAR_WEIGHT                   NUMBER                                  ,
                TAR_WEIGHT_UNIT_OF_MEASURE   VARCHAR2(25)                            ,
                PACKAGING_CODE               VARCHAR2(5)                             ,
                CARRIER_METHOD               VARCHAR2(2)                             ,
                CARRIER_EQUIPMENT            VARCHAR2(10)                            ,
                SPECIAL_HANDLING_CODE        VARCHAR2(3)                             ,
                HAZARD_CODE                  VARCHAR2(1)                             ,
                HAZARD_CLASS                 VARCHAR2(4)                             ,
                HAZARD_DESCRIPTION           VARCHAR2(80)                            ,
                FREIGHT_TERMS                VARCHAR2(25)                            ,
                FREIGHT_BILL_NUMBER          VARCHAR2(35)                            ,
                INVOICE_NUM                  VARCHAR2(30)                            ,
                INVOICE_DATE                 DATE                                    ,
                TOTAL_INVOICE_AMOUNT         NUMBER                                  ,
                TAX_NAME                     VARCHAR2(15)                            ,
                TAX_AMOUNT                   NUMBER                                  ,
                FREIGHT_AMOUNT               NUMBER                                  ,
                CURRENCY_CODE                VARCHAR2(15)                            ,
                CONVERSION_RATE_TYPE         VARCHAR2(30)                            ,
                CONVERSION_RATE              NUMBER                                  ,
                CONVERSION_RATE_DATE         DATE                                    ,
                PAYMENT_TERMS_NAME           VARCHAR2(50)                            ,
                EMPLOYEE_NAME                VARCHAR2(240)                           ,
                TRANSACTION_DATE             DATE                                    ,
                CUSTOMER_ACCOUNT_NUMBER      NUMBER                                  ,
                CUSTOMER_PARTY_NAME          VARCHAR2(360)                           ,
                CUSTOMER_PARTY_NUMBER        VARCHAR2(30)                            ,
                BUSINESS_UNIT                VARCHAR2(240)                           ,
                RA_OUTSOURCER_PARTY_NAME     VARCHAR2(240)                           ,
                RECEIPT_ADVICE_NUMBER        VARCHAR2(80)                            ,
                RA_DOCUMENT_CODE             VARCHAR2(25)                            ,
                RA_DOCUMENT_NUMBER           VARCHAR2(80)                            ,
                RA_DOC_REVISION_NUMBER       VARCHAR2(80)                            ,
                RA_DOC_REVISION_DATE         DATE                                    ,
                RA_DOC_CREATION_DATE         DATE                                    ,
                RA_DOC_LAST_UPDATE_DATE      DATE                                    ,
                RA_OUTSOURCER_CONTACT_NAME   VARCHAR2(240)                           ,
                RA_VENDOR_SITE_NAME          VARCHAR2(240)                           ,
                RA_NOTE_TO_RECEIVER          CHAR(480)                               ,
				RA_DOO_SOURCE_SYSTEM_NAME    VARCHAR2(30)                            ,
                ATTRIBUTE_CATEGORY           VARCHAR2(30)                            ,
				ATTRIBUTE1                     VARCHAR2(150)  ,          
                ATTRIBUTE2                     VARCHAR2(150)  ,
                ATTRIBUTE3                     VARCHAR2(150)  ,
				ATTRIBUTE4                     VARCHAR2(150),      --v1.3 Added new columns as per the extract file
				ATTRIBUTE5                     VARCHAR2(150),
				ATTRIBUTE6                     VARCHAR2(150),
				ATTRIBUTE7                     VARCHAR2(150),
				ATTRIBUTE8                     VARCHAR2(150),
				ATTRIBUTE9                     VARCHAR2(150),
				ATTRIBUTE10                    VARCHAR2(150),
				ATTRIBUTE11                    VARCHAR2(150),
				ATTRIBUTE12                    VARCHAR2(150),
				ATTRIBUTE13                    VARCHAR2(150),
				ATTRIBUTE14                    VARCHAR2(150),
				ATTRIBUTE15                    VARCHAR2(150),
				ATTRIBUTE16                    VARCHAR2(150),
				ATTRIBUTE17                    VARCHAR2(150),
				ATTRIBUTE18                    VARCHAR2(150),
				ATTRIBUTE19                    VARCHAR2(150),
				ATTRIBUTE20                    VARCHAR2(150),
				ATTRIBUTE_NUMBER1              NUMBER,
				ATTRIBUTE_NUMBER2              NUMBER,
				ATTRIBUTE_NUMBER3              NUMBER,
				ATTRIBUTE_NUMBER4              NUMBER,
				ATTRIBUTE_NUMBER5              NUMBER,
				ATTRIBUTE_NUMBER6              NUMBER,
				ATTRIBUTE_NUMBER7              NUMBER,
				ATTRIBUTE_NUMBER8              NUMBER,
				ATTRIBUTE_NUMBER9              NUMBER,
				ATTRIBUTE_NUMBER10             NUMBER,
				ATTRIBUTE_DATE1                DATE            ,
                ATTRIBUTE_DATE2                DATE           ,
                ATTRIBUTE_DATE3                DATE            ,
				ATTRIBUTE_DATE4                DATE         , 
                ATTRIBUTE_DATE5                DATE         , 
                ATTRIBUTE_TIMESTAMP1           TIMESTAMP(6) , 
                ATTRIBUTE_TIMESTAMP2           TIMESTAMP(6) , 
                ATTRIBUTE_TIMESTAMP3           TIMESTAMP(6) , 
                ATTRIBUTE_TIMESTAMP4           TIMESTAMP(6) , 
                ATTRIBUTE_TIMESTAMP5           TIMESTAMP(6) ,
                GL_DATE                      DATE                                    ,
                RECEIPT_HEADER_ID            NUMBER                                  ,
                EXTERNAL_SYS_TXN_REFERENCE   VARCHAR2(300)                           '
                );

                dbms_output.put_line('External table is created');
                EXECUTE IMMEDIATE 'INSERT INTO XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG ( 
									HEADER_INTERFACE_NUM,
									RECEIPT_SOURCE_CODE,
									ASN_TYPE,
									TRANSACTION_TYPE,
									NOTICE_CREATION_DATE,
									SHIPMENT_NUM,
									RECEIPT_NUM,
									VENDOR_NAME,
									VENDOR_NUM,
									VENDOR_SITE_CODE,
									FROM_ORGANIZATION_CODE,
									LOCATION_CODE,
									BILL_OF_LADING,
									PACKING_SLIP,
									SHIPPED_DATE,
									FREIGHT_CARRIER_NAME,
									EXPECTED_RECEIPT_DATE,
									NUM_OF_CONTAINERS,
									WAYBILL_AIRBILL_NUM,
									COMMENTS,
									GROSS_WEIGHT,
									GROSS_WEIGHT_UNIT_OF_MEASURE,
									NET_WEIGHT,
									NET_WEIGHT_UNIT_OF_MEASURE,
									TAR_WEIGHT,
									TAR_WEIGHT_UNIT_OF_MEASURE,
									PACKAGING_CODE,
									CARRIER_METHOD,
									CARRIER_EQUIPMENT,
									SPECIAL_HANDLING_CODE,
									HAZARD_CODE,
									HAZARD_CLASS,
									HAZARD_DESCRIPTION,
									FREIGHT_TERMS,
									FREIGHT_BILL_NUMBER,
									INVOICE_NUM,
									INVOICE_DATE,
									TOTAL_INVOICE_AMOUNT,
									TAX_NAME,
									TAX_AMOUNT,
									FREIGHT_AMOUNT,
									CURRENCY_CODE,
									CONVERSION_RATE_TYPE,
									CONVERSION_RATE,
									CONVERSION_RATE_DATE,
									PAYMENT_TERMS_NAME,
									EMPLOYEE_NAME,
									TRANSACTION_DATE,
									CUSTOMER_ACCOUNT_NUMBER,
									CUSTOMER_PARTY_NAME,
									CUSTOMER_PARTY_NUMBER,
									BUSINESS_UNIT,
									RA_OUTSOURCER_PARTY_NAME,
									RECEIPT_ADVICE_NUMBER,
									RA_DOCUMENT_CODE,
									RA_DOCUMENT_NUMBER,
									RA_DOC_REVISION_NUMBER,
									RA_DOC_REVISION_DATE,
									RA_DOC_CREATION_DATE,
									RA_DOC_LAST_UPDATE_DATE,
									RA_OUTSOURCER_CONTACT_NAME,
									RA_VENDOR_SITE_NAME,
									RA_NOTE_TO_RECEIVER,
									RA_DOO_SOURCE_SYSTEM_NAME,
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
					                ATTRIBUTE_DATE1,
					                ATTRIBUTE_DATE2,
					                ATTRIBUTE_DATE3,
					                ATTRIBUTE_DATE4,
       		                        ATTRIBUTE_DATE5,
       		                        ATTRIBUTE_TIMESTAMP1,
       		                        ATTRIBUTE_TIMESTAMP2,
       		                        ATTRIBUTE_TIMESTAMP3,
       		                        ATTRIBUTE_TIMESTAMP4,
       		                        ATTRIBUTE_TIMESTAMP5,
									GL_DATE,
									RECEIPT_HEADER_ID,
									EXTERNAL_SYS_TXN_REFERENCE,
									FILE_NAME 						,
									ERROR_MESSAGE 					,
									IMPORT_STATUS  					,
									EXECUTION_ID  					,
									FILE_REFERENCE_IDENTIFIER		,
									SOURCE_SYSTEM   				,
									BATCH_ID
									) 
									SELECT 
									HEADER_INTERFACE_NUM,
									RECEIPT_SOURCE_CODE,
									ASN_TYPE,
									TRANSACTION_TYPE,
									NOTICE_CREATION_DATE,
									SHIPMENT_NUM,
									RECEIPT_NUM,
									VENDOR_NAME,
									VENDOR_NUM,
									VENDOR_SITE_CODE,
									FROM_ORGANIZATION_CODE,
									LOCATION_CODE,
									BILL_OF_LADING,
									PACKING_SLIP,
									SHIPPED_DATE,
									FREIGHT_CARRIER_NAME,
									EXPECTED_RECEIPT_DATE,
									NUM_OF_CONTAINERS,
									WAYBILL_AIRBILL_NUM,
									COMMENTS,
									GROSS_WEIGHT,
									GROSS_WEIGHT_UNIT_OF_MEASURE,
									NET_WEIGHT,
									NET_WEIGHT_UNIT_OF_MEASURE,
									TAR_WEIGHT,
									TAR_WEIGHT_UNIT_OF_MEASURE,
									PACKAGING_CODE,
									CARRIER_METHOD,
									CARRIER_EQUIPMENT,
									SPECIAL_HANDLING_CODE,
									HAZARD_CODE,
									HAZARD_CLASS,
									HAZARD_DESCRIPTION,
									FREIGHT_TERMS,
									FREIGHT_BILL_NUMBER,
									INVOICE_NUM,
									INVOICE_DATE,
									TOTAL_INVOICE_AMOUNT,
									TAX_NAME,
									TAX_AMOUNT,
									FREIGHT_AMOUNT,
									CURRENCY_CODE,
									CONVERSION_RATE_TYPE,
									CONVERSION_RATE,
									CONVERSION_RATE_DATE,
									PAYMENT_TERMS_NAME,
									EMPLOYEE_NAME,
									TRANSACTION_DATE,
									CUSTOMER_ACCOUNT_NUMBER,
									CUSTOMER_PARTY_NAME,
									CUSTOMER_PARTY_NUMBER,
									BUSINESS_UNIT,
									RA_OUTSOURCER_PARTY_NAME,
									RECEIPT_ADVICE_NUMBER,
									RA_DOCUMENT_CODE,
									RA_DOCUMENT_NUMBER,
									RA_DOC_REVISION_NUMBER,
									RA_DOC_REVISION_DATE,
									RA_DOC_CREATION_DATE,
									RA_DOC_LAST_UPDATE_DATE,
									RA_OUTSOURCER_CONTACT_NAME,
									RA_VENDOR_SITE_NAME,
									RA_NOTE_TO_RECEIVER,
									RA_DOO_SOURCE_SYSTEM_NAME,
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
					                ATTRIBUTE_DATE1,
					                ATTRIBUTE_DATE2,
					                ATTRIBUTE_DATE3,
					                ATTRIBUTE_DATE4,
       		                        ATTRIBUTE_DATE5,
       		                        ATTRIBUTE_TIMESTAMP1,
       		                        ATTRIBUTE_TIMESTAMP2,
       		                        ATTRIBUTE_TIMESTAMP3,
       		                        ATTRIBUTE_TIMESTAMP4,
       		                        ATTRIBUTE_TIMESTAMP5,
									GL_DATE,
									RECEIPT_HEADER_ID,
									EXTERNAL_SYS_TXN_REFERENCE,
									null
									,null
									,null
									,'
                                  || chr(39)
                                  || gv_execution_id
                                  || chr(39)
                                  || '
									,null	
									,null
									,'
                                  || gv_batch_id
                                  || '
									FROM XXCNV_PO_C009_PO_RECEIPTS_HEADERS_EXT ';

                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted Records in the xxcnv_po_c009_po_receipts_headers_stg: ' || SQL%rowcount);
			--commit;

            END IF;				

---TABLE2
            BEGIN
                IF gv_oci_file_name_potrans LIKE '%RcvTransactionsInterface.csv%' THEN
                    dbms_output.put_line('Creating external table XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_EXT');
                    dbms_output.put_line(' xxcnv_po_c009_po_receipts_transactions_ext : '
                                         || gv_oci_file_path
                                         || '/'
                                         || gv_oci_file_name_potrans);

        -- Create the external table

                    dbms_cloud.create_external_table(
                        table_name      => 'xxcnv_po_c009_po_receipts_transactions_ext',
                        credential_name => gv_credential_name,
                        file_uri_list   => gv_oci_file_path
                                         || '/'
                                         || gv_oci_file_name_potrans,
                        format          =>
                                JSON_OBJECT(
                                    'skipheaders' VALUE '1',
                                    'type' VALUE 'csv',
                                    'rejectlimit' VALUE 'UNLIMITED',
                                    'dateformat' VALUE 'yyyy/mm/dd',
                                    'ignoremissingcolumns' VALUE 'true',
                                            'blankasnull' VALUE 'true',
                                    'conversionerrors' VALUE 'store_null'
                                ),
                        column_list     => 'Interface_Line_Number          VARCHAR2(30) ,
                TRANSACTION_TYPE               VARCHAR2(25) ,
                AUTO_TRANSACT_CODE             VARCHAR2(25) ,
                TRANSACTION_DATE               VARCHAR2(30)         ,
                SOURCE_DOCUMENT_CODE           VARCHAR2(25) ,
                RECEIPT_SOURCE_CODE            VARCHAR2(25) ,
                HEADER_INTERFACE_NUM           VARCHAR2(30) ,
                PARENT_TRANSACTION_ID          NUMBER       ,
                PARENT_INTF_LINE_NUM           VARCHAR2(30) ,
                TO_ORGANIZATION_CODE           VARCHAR2(18) ,
                ITEM_NUM                       VARCHAR2(300),
                ITEM_DESCRIPTION               VARCHAR2(240),
                ITEM_REVISION                  VARCHAR2(18) ,
                DOCUMENT_NUM                   VARCHAR2(30) ,
                DOCUMENT_LINE_NUM              NUMBER       ,
                DOCUMENT_SHIPMENT_LINE_NUM     NUMBER       ,
                DOCUMENT_DISTRIBUTION_NUM      NUMBER       ,
                BUSINESS_UNIT                  VARCHAR2(240),
                SHIPMENT_NUM                   VARCHAR2(30) ,
                EXPECTED_RECEIPT_DATE          VARCHAR2(30)         ,
                SUBINVENTORY                   VARCHAR2(30) ,
                LOCATOR                        VARCHAR2(81) ,
                QUANTITY                       NUMBER       ,
                UNIT_OF_MEASURE                VARCHAR2(25) ,
                PRIMARY_QUANTITY               NUMBER       ,
                PRIMARY_UNIT_OF_MEASURE        VARCHAR2(25) ,
                SECONDARY_QUANTITY             NUMBER       ,
                SECONDARY_UNIT_OF_MEASURE      VARCHAR2(25) ,
                VENDOR_NAME                    VARCHAR2(240),
                VENDOR_NUM                     VARCHAR2(30) ,
                VENDOR_SITE_CODE               VARCHAR2(240),
                CUSTOMER_PARTY_NAME            VARCHAR2(360),
                CUSTOMER_PARTY_NUMBER          VARCHAR2(30) ,
                CUSTOMER_ACCOUNT_NUMBER        NUMBER       ,
                SHIP_TO_LOCATION_CODE          VARCHAR2(60) ,
                LOCATION_CODE                  VARCHAR2(60) ,
                REASON_NAME                    VARCHAR2(30) ,
                DELIVER_TO_PERSON_NAME         VARCHAR2(240),
                DELIVER_TO_LOCATION_CODE       VARCHAR2(60) ,
                RECEIPT_EXCEPTION_FLAG         VARCHAR2(1)  ,
                ROUTING_HEADER_ID              NUMBER       ,
                DESTINATION_TYPE_CODE          VARCHAR2(25) ,
                INTERFACE_SOURCE_CODE          VARCHAR2(30) ,
                INTERFACE_SOURCE_LINE_ID       NUMBER       ,
                AMOUNT                         NUMBER       ,
                CURRENCY_CODE                  VARCHAR2(15) ,
                CURRENCY_CONVERSION_TYPE       VARCHAR2(30) ,
                CURRENCY_CONVERSION_RATE       NUMBER       ,
                CURRENCY_CONVERSION_DATE       DATE         ,
                INSPECTION_STATUS_CODE         VARCHAR2(25) ,
                INSPECTION_QUALITY_CODE        VARCHAR2(25) ,
                FROM_ORGANIZATION_CODE         VARCHAR2(18) ,
                FROM_SUBINVENTORY              VARCHAR2(10) ,
                FROM_LOCATOR                   VARCHAR2(81) ,
                FREIGHT_CARRIER_NAME           VARCHAR2(360),
                BILL_OF_LADING                 VARCHAR2(25) ,
                PACKING_SLIP                   VARCHAR2(25) ,
                SHIPPED_DATE                   DATE         ,
                NUM_OF_CONTAINERS              NUMBER       ,
                WAYBILL_AIRBILL_NUM            VARCHAR2(20) ,
                RMA_REFERENCE                  VARCHAR2(30) ,
                COMMENTS                       VARCHAR2(240),
                TRUCK_NUM                      VARCHAR2(35) ,
                CONTAINER_NUM                  VARCHAR2(35) ,
                SUBSTITUTE_ITEM_NUM            VARCHAR2(300),
                NOTICE_UNIT_PRICE              NUMBER       ,
                ITEM_CATEGORY                  VARCHAR2(81) ,
                INTRANSIT_OWNING_ORG_CODE      VARCHAR2(18) ,
                ROUTING_CODE                   VARCHAR2(30) ,
                BARCODE_LABEL                  VARCHAR2(35) ,
                COUNTRY_OF_ORIGIN_CODE         VARCHAR2(2)  ,
                CREATE_DEBIT_MEMO_FLAG         VARCHAR2(1)  ,
                LICENSE_PLATE_NUMBER           VARCHAR2(30) ,
                TRANSFER_LICENSE_PLATE_NUMBER  VARCHAR2(30) ,
                LPN_GROUP_NUM                  VARCHAR2(30) ,
                ASN_LINE_NUM                   NUMBER       ,
                EMPLOYEE_NAME                  VARCHAR2(240),
                SOURCE_TRANSACTION_NUM         VARCHAR2(25) ,
                PARENT_SOURCE_TRANSACTION_NUM  VARCHAR2(25) ,
                PARENT_INTERFACE_TXN_ID        NUMBER       ,
                MATCHING_BASIS                 VARCHAR2(30) ,
                RA_OUTSOURCER_PARTY_NAME       VARCHAR2(240),
                RA_DOCUMENT_NUMBER             VARCHAR2(80) ,
                RA_DOCUMENT_LINE_NUMBER        VARCHAR2(80) ,
                RA_NOTE_TO_RECEIVER            VARCHAR2(480),
                RA_VENDOR_SITE_NAME            VARCHAR2(240),
                ATTRIBUTE_CATEGORY             VARCHAR2(30) ,
				ATTRIBUTE1                     VARCHAR2(150)  ,          
                ATTRIBUTE2                     VARCHAR2(150)  ,
                ATTRIBUTE3                     VARCHAR2(150)  ,
				ATTRIBUTE4                     VARCHAR2(150),      --v1.3 Added new columns as per the extract file
				ATTRIBUTE5                     VARCHAR2(150),
				ATTRIBUTE6                     VARCHAR2(150),
				ATTRIBUTE7                     VARCHAR2(150),
				ATTRIBUTE8                     VARCHAR2(150),
				ATTRIBUTE9                     VARCHAR2(150),
				ATTRIBUTE10                    VARCHAR2(150),
				ATTRIBUTE11                    VARCHAR2(150),
				ATTRIBUTE12                    VARCHAR2(150),
				ATTRIBUTE13                    VARCHAR2(150),
				ATTRIBUTE14                    VARCHAR2(150),
				ATTRIBUTE15                    VARCHAR2(150),
				ATTRIBUTE16                    VARCHAR2(150),
				ATTRIBUTE17                    VARCHAR2(150),
				ATTRIBUTE18                    VARCHAR2(150),
				ATTRIBUTE19                    VARCHAR2(150),
				ATTRIBUTE20                    VARCHAR2(150),
				ATTRIBUTE_NUMBER1              NUMBER,
				ATTRIBUTE_NUMBER2              NUMBER,
				ATTRIBUTE_NUMBER3              NUMBER,
				ATTRIBUTE_NUMBER4              NUMBER,
				ATTRIBUTE_NUMBER5              NUMBER,
				ATTRIBUTE_NUMBER6              NUMBER,
				ATTRIBUTE_NUMBER7              NUMBER,
				ATTRIBUTE_NUMBER8              NUMBER,
				ATTRIBUTE_NUMBER9              NUMBER,
				ATTRIBUTE_NUMBER10             NUMBER,
				ATTRIBUTE_DATE1                VARCHAR2(30)            ,
                ATTRIBUTE_DATE2                VARCHAR2(30)            ,
                ATTRIBUTE_DATE3                VARCHAR2(30)            ,
				ATTRIBUTE_DATE4                DATE         , 
                ATTRIBUTE_DATE5                DATE         , 
                ATTRIBUTE_TIMESTAMP1           TIMESTAMP(6) , 
                ATTRIBUTE_TIMESTAMP2           TIMESTAMP(6) , 
                ATTRIBUTE_TIMESTAMP3           TIMESTAMP(6) , 
                ATTRIBUTE_TIMESTAMP4           TIMESTAMP(6) , 
                ATTRIBUTE_TIMESTAMP5           TIMESTAMP(6) ,
                CONSIGNED_FLAG                 VARCHAR2(1)  ,
                SOLDTO_LEGAL_ENTITY            VARCHAR2(240),
                CONSUMED_QUANTITY              NUMBER       ,
                DEFAULT_TAXATION_COUNTRY       VARCHAR2(2)  ,
                TRX_BUSINESS_CATEGORY          VARCHAR2(240),
                DOCUMENT_FISCAL_CLASSIFICATION VARCHAR2(240),
                USER_DEFINED_FISC_CLASS        VARCHAR2(30) ,
                PRODUCT_FISC_CLASS_NAME        VARCHAR2(250),
                INTENDED_USE                   VARCHAR2(240),
                PRODUCT_CATEGORY               VARCHAR2(240),
                TAX_CLASSIFICATION_CODE        VARCHAR2(50) ,
                PRODUCT_TYPE                   VARCHAR2(240),
                FIRST_PTY_NUMBER               VARCHAR2(30) ,
                THIRD_PTY_NUMBER               VARCHAR2(30) ,
                TAX_INVOICE_NUMBER             VARCHAR2(150),
                TAX_INVOICE_DATE               DATE         ,
                FINAL_DISCHARGE_LOC_CODE       VARCHAR2(60) ,
                ASSESSABLE_VALUE               NUMBER       ,
                PHYSICAL_RETURN_REQD           VARCHAR2(1)  ,
                EXTERNAL_SYSTEM_PACKING_UNIT   VARCHAR2(150),
                EWAY_BILL_NUMBER               NUMBER       ,
                EWAY_BILL_DATE                 DATE         ,
                RECALL_NOTICE_NUMBER           VARCHAR2(60) ,
                RECALL_LINE_NUMBER             NUMBER       ,
                EXTERNAL_SYS_TXN_REFERENCE     VARCHAR2(300),
                DEFAULT_LOTSER_FROM_ASN        VARCHAR2(1)  '
                    );

                    EXECUTE IMMEDIATE 'INSERT INTO XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG (
					Interface_Line_Number,
       		        TRANSACTION_TYPE,
       		        AUTO_TRANSACT_CODE,
       		        TRANSACTION_DATE,
       		        SOURCE_DOCUMENT_CODE,
       		        RECEIPT_SOURCE_CODE,
       		        HEADER_INTERFACE_NUM,
       		        PARENT_TRANSACTION_ID,
       		        PARENT_INTF_LINE_NUM,
       		        TO_ORGANIZATION_CODE,
       		        ITEM_NUM,
       		        ITEM_DESCRIPTION,
       		        ITEM_REVISION,
       		        DOCUMENT_NUM,
       		        DOCUMENT_LINE_NUM,
       		        DOCUMENT_SHIPMENT_LINE_NUM,
       		        DOCUMENT_DISTRIBUTION_NUM,
       		        BUSINESS_UNIT,
       		        SHIPMENT_NUM,
       		        EXPECTED_RECEIPT_DATE,
       		        SUBINVENTORY,
       		        LOCATOR,
       		        QUANTITY,
       		        UNIT_OF_MEASURE,
       		        PRIMARY_QUANTITY,
       		        PRIMARY_UNIT_OF_MEASURE,
       		        SECONDARY_QUANTITY,
       		        SECONDARY_UNIT_OF_MEASURE,
       		        VENDOR_NAME,
       		        VENDOR_NUM,
       		        VENDOR_SITE_CODE,
       		        CUSTOMER_PARTY_NAME,
       		        CUSTOMER_PARTY_NUMBER,
       		        CUSTOMER_ACCOUNT_NUMBER,
       		        SHIP_TO_LOCATION_CODE,
       		        LOCATION_CODE,
       		        REASON_NAME,
       		        DELIVER_TO_PERSON_NAME,
       		        DELIVER_TO_LOCATION_CODE,
       		        RECEIPT_EXCEPTION_FLAG,
       		        ROUTING_HEADER_ID,
       		        DESTINATION_TYPE_CODE,
       		        INTERFACE_SOURCE_CODE,
       		        INTERFACE_SOURCE_LINE_ID,
       		        AMOUNT,
       		        CURRENCY_CODE,
       		        CURRENCY_CONVERSION_TYPE,
       		        CURRENCY_CONVERSION_RATE,
       		        CURRENCY_CONVERSION_DATE,
       		        INSPECTION_STATUS_CODE,
       		        INSPECTION_QUALITY_CODE,
       		        FROM_ORGANIZATION_CODE,
       		        FROM_SUBINVENTORY,
       		        FROM_LOCATOR,
       		        FREIGHT_CARRIER_NAME,
       		        BILL_OF_LADING,
       		        PACKING_SLIP,
       		        SHIPPED_DATE,
       		        NUM_OF_CONTAINERS,
       		        WAYBILL_AIRBILL_NUM,
       		        RMA_REFERENCE,
       		        COMMENTS,
       		        TRUCK_NUM,
       		        CONTAINER_NUM,
       		        SUBSTITUTE_ITEM_NUM,
       		        NOTICE_UNIT_PRICE,
       		        ITEM_CATEGORY,
       		        INTRANSIT_OWNING_ORG_CODE,
       		        ROUTING_CODE,
       		        BARCODE_LABEL,
       		        COUNTRY_OF_ORIGIN_CODE,
       		        CREATE_DEBIT_MEMO_FLAG,
       		        LICENSE_PLATE_NUMBER,
       		        TRANSFER_LICENSE_PLATE_NUMBER,
       		        LPN_GROUP_NUM,
       		        ASN_LINE_NUM,
       		        EMPLOYEE_NAME,
       		        SOURCE_TRANSACTION_NUM,
       		        PARENT_SOURCE_TRANSACTION_NUM,
       		        PARENT_INTERFACE_TXN_ID,
       		        MATCHING_BASIS,
       		        RA_OUTSOURCER_PARTY_NAME,
       		        RA_DOCUMENT_NUMBER,
       		        RA_DOCUMENT_LINE_NUMBER,
       		        RA_NOTE_TO_RECEIVER,
       		        RA_VENDOR_SITE_NAME,
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
					ATTRIBUTE_DATE1,
					ATTRIBUTE_DATE2,
					ATTRIBUTE_DATE3,
					ATTRIBUTE_DATE4,
       		        ATTRIBUTE_DATE5,
       		        ATTRIBUTE_TIMESTAMP1,
       		        ATTRIBUTE_TIMESTAMP2,
       		        ATTRIBUTE_TIMESTAMP3,
       		        ATTRIBUTE_TIMESTAMP4,
       		        ATTRIBUTE_TIMESTAMP5,
       		        CONSIGNED_FLAG,
       		        SOLDTO_LEGAL_ENTITY,
       		        CONSUMED_QUANTITY,
       		        DEFAULT_TAXATION_COUNTRY,
       		        TRX_BUSINESS_CATEGORY,
       		        DOCUMENT_FISCAL_CLASSIFICATION,
       		        USER_DEFINED_FISC_CLASS,
       		        PRODUCT_FISC_CLASS_NAME,
       		        INTENDED_USE,
       		        PRODUCT_CATEGORY,
       		        TAX_CLASSIFICATION_CODE,
       		        PRODUCT_TYPE,
       		        FIRST_PTY_NUMBER,
       		        THIRD_PTY_NUMBER,
       		        TAX_INVOICE_NUMBER,
       		        TAX_INVOICE_DATE,
       		        FINAL_DISCHARGE_LOC_CODE,
       		        ASSESSABLE_VALUE,
       		        PHYSICAL_RETURN_REQD,
       		        EXTERNAL_SYSTEM_PACKING_UNIT,
       		        EWAY_BILL_NUMBER,
       		        EWAY_BILL_DATE,
       		        RECALL_NOTICE_NUMBER,
       		        RECALL_LINE_NUMBER,
       		        EXTERNAL_SYS_TXN_REFERENCE,
       		        DEFAULT_LOTSER_FROM_ASN,					
					FILE_NAME 						,
					ERROR_MESSAGE 					,
					IMPORT_STATUS  					,
					EXECUTION_ID  					,
					FILE_REFERENCE_IDENTIFIER 		,
					SOURCE_SYSTEM   				,
					Batch_ID
					)
					SELECT 
					Interface_Line_Number,
       		        TRANSACTION_TYPE,
       		        AUTO_TRANSACT_CODE,
       		        TRANSACTION_DATE,
       		        SOURCE_DOCUMENT_CODE,
       		        RECEIPT_SOURCE_CODE,
       		        HEADER_INTERFACE_NUM,
       		        PARENT_TRANSACTION_ID,
       		        PARENT_INTF_LINE_NUM,
       		        TO_ORGANIZATION_CODE,
       		        ITEM_NUM,
       		        ITEM_DESCRIPTION,
       		        ITEM_REVISION,
       		        DOCUMENT_NUM,
       		        DOCUMENT_LINE_NUM,
       		        DOCUMENT_SHIPMENT_LINE_NUM,
       		        DOCUMENT_DISTRIBUTION_NUM,
       		        BUSINESS_UNIT,
       		        SHIPMENT_NUM,
       		        EXPECTED_RECEIPT_DATE,
       		        SUBINVENTORY,
       		        LOCATOR,
       		        QUANTITY,
       		        UNIT_OF_MEASURE,
       		        PRIMARY_QUANTITY,
       		        PRIMARY_UNIT_OF_MEASURE,
       		        SECONDARY_QUANTITY,
       		        SECONDARY_UNIT_OF_MEASURE,
       		        VENDOR_NAME,
       		        VENDOR_NUM,
       		        VENDOR_SITE_CODE,
       		        CUSTOMER_PARTY_NAME,
       		        CUSTOMER_PARTY_NUMBER,
       		        CUSTOMER_ACCOUNT_NUMBER,
       		        SHIP_TO_LOCATION_CODE,
       		        LOCATION_CODE,
       		        REASON_NAME,
       		        DELIVER_TO_PERSON_NAME,
       		        DELIVER_TO_LOCATION_CODE,
       		        RECEIPT_EXCEPTION_FLAG,
       		        ROUTING_HEADER_ID,
       		        DESTINATION_TYPE_CODE,
       		        INTERFACE_SOURCE_CODE,
       		        INTERFACE_SOURCE_LINE_ID,
       		        AMOUNT,
       		        CURRENCY_CODE,
       		        CURRENCY_CONVERSION_TYPE,
       		        CURRENCY_CONVERSION_RATE,
       		        CURRENCY_CONVERSION_DATE,
       		        INSPECTION_STATUS_CODE,
       		        INSPECTION_QUALITY_CODE,
       		        FROM_ORGANIZATION_CODE,
       		        FROM_SUBINVENTORY,
       		        FROM_LOCATOR,
       		        FREIGHT_CARRIER_NAME,
       		        BILL_OF_LADING,
       		        PACKING_SLIP,
       		        SHIPPED_DATE,
       		        NUM_OF_CONTAINERS,
       		        WAYBILL_AIRBILL_NUM,
       		        RMA_REFERENCE,
       		        COMMENTS,
       		        TRUCK_NUM,
       		        CONTAINER_NUM,
       		        SUBSTITUTE_ITEM_NUM,
       		        NOTICE_UNIT_PRICE,
       		        ITEM_CATEGORY,
       		        INTRANSIT_OWNING_ORG_CODE,
       		        ROUTING_CODE,
       		        BARCODE_LABEL,
       		        COUNTRY_OF_ORIGIN_CODE,
       		        CREATE_DEBIT_MEMO_FLAG,
       		        LICENSE_PLATE_NUMBER,
       		        TRANSFER_LICENSE_PLATE_NUMBER,
       		        LPN_GROUP_NUM,
       		        ASN_LINE_NUM,
       		        EMPLOYEE_NAME,
       		        SOURCE_TRANSACTION_NUM,
       		        PARENT_SOURCE_TRANSACTION_NUM,
       		        PARENT_INTERFACE_TXN_ID,
       		        MATCHING_BASIS,
       		        RA_OUTSOURCER_PARTY_NAME,
       		        RA_DOCUMENT_NUMBER,
       		        RA_DOCUMENT_LINE_NUMBER,
       		        RA_NOTE_TO_RECEIVER,
       		        RA_VENDOR_SITE_NAME,
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
					ATTRIBUTE_DATE1,
					ATTRIBUTE_DATE2,
					ATTRIBUTE_DATE3,
					ATTRIBUTE_DATE4,
       		        ATTRIBUTE_DATE5,
       		        ATTRIBUTE_TIMESTAMP1,
       		        ATTRIBUTE_TIMESTAMP2,
       		        ATTRIBUTE_TIMESTAMP3,
       		        ATTRIBUTE_TIMESTAMP4,
       		        ATTRIBUTE_TIMESTAMP5,
       		        CONSIGNED_FLAG,
       		        SOLDTO_LEGAL_ENTITY,
       		        CONSUMED_QUANTITY,
       		        DEFAULT_TAXATION_COUNTRY,
       		        TRX_BUSINESS_CATEGORY,
       		        DOCUMENT_FISCAL_CLASSIFICATION,
       		        USER_DEFINED_FISC_CLASS,
       		        PRODUCT_FISC_CLASS_NAME,
       		        INTENDED_USE,
       		        PRODUCT_CATEGORY,
       		        TAX_CLASSIFICATION_CODE,
       		        PRODUCT_TYPE,
       		        FIRST_PTY_NUMBER,
       		        THIRD_PTY_NUMBER,
       		        TAX_INVOICE_NUMBER,
       		        TAX_INVOICE_DATE,
       		        FINAL_DISCHARGE_LOC_CODE,
       		        ASSESSABLE_VALUE,
       		        PHYSICAL_RETURN_REQD,
       		        EXTERNAL_SYSTEM_PACKING_UNIT,
       		        EWAY_BILL_NUMBER,
       		        EWAY_BILL_DATE,
       		        RECALL_NOTICE_NUMBER,
       		        RECALL_LINE_NUMBER,
       		        EXTERNAL_SYS_TXN_REFERENCE,
       		        DEFAULT_LOTSER_FROM_ASN,
					null,
					null,
					null,
					'
                                      || chr(39)
                                      || gv_execution_id
                                      || chr(39)
                                      || ',
					NULL,
                    NULL,
					'
                                      || gv_batch_id
                                      || '
					FROM XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_EXT';

                    p_loading_status := gv_status_success;
                    dbms_output.put_line('Inserted records in xxcnv_po_c009_po_receipts_transactions_stg: ' || SQL%rowcount);
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error creating external table: ' || sqlerrm);
                    p_loading_status := gv_status_failure;
                    RETURN;
            END;

--File3
 ---TABLE2
            BEGIN
                IF gv_oci_file_name_porecmap LIKE '%RcvReceiptQtyMap.csv%' THEN
                    dbms_output.put_line('Creating external table XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_EXT');
                    dbms_output.put_line(' xxcnv_po_c009_po_receipts_transactions_ext : '
                                         || gv_oci_file_path
                                         || '/'
                                         || gv_oci_file_name_porecmap);

        -- Create the external table

                    dbms_cloud.create_external_table(
                        table_name      => 'XXCNV_PO_RECEIPTS_QUANTITY_MAPPING_EXT',
                        credential_name => gv_credential_name,
                        file_uri_list   => gv_oci_file_path
                                         || '/'
                                         || gv_oci_file_name_porecmap,
                        format          =>
                                JSON_OBJECT(
                                    'skipheaders' VALUE '1',
                                    'type' VALUE 'csv',
                                    'rejectlimit' VALUE 'UNLIMITED',
                                    'dateformat' VALUE 'yyyy/mm/dd',
                                    'ignoremissingcolumns' VALUE 'true',
                                            'blankasnull' VALUE 'true',
                                    'conversionerrors' VALUE 'store_null'
                                ),
                        column_list     => 'PO_DOC                 VARCHAR2(30), 
                    PO_DOC_LINE            NUMBER,				
                    RECEIPT_NUMBER         VARCHAR2(30),
                    RECEIPT_QUANTITY       NUMBER,
                    PO_QUANTITY 		   NUMBER,		
                    BILLED_QUANTITY        NUMBER'
                    );

                    EXECUTE IMMEDIATE 'INSERT INTO XXCNV_PO_RECEIPTS_QUANTITY_MAPPING (
					PO_DOC           , 
                    PO_DOC_LINE      ,				
                    RECEIPT_NUMBER   ,
                    RECEIPT_QUANTITY ,
                    PO_QUANTITY,
                    BILLED_QUANTITY  
					)
					SELECT 
					PO_DOC           , 
                    PO_DOC_LINE      ,				
                    RECEIPT_NUMBER   ,
                    RECEIPT_QUANTITY ,
                    PO_QUANTITY,
                    BILLED_QUANTITY 
					FROM XXCNV_PO_RECEIPTS_QUANTITY_MAPPING_EXT';
                    p_loading_status := gv_status_success;
                    dbms_output.put_line('Inserted records in xxcnv_po_c009_po_receipts_transactions_stg: ' || SQL%rowcount);
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error creating external table: ' || sqlerrm);
                    p_loading_status := gv_status_failure;
                    RETURN;
            END;

	   -- Count the number of rows in the external table
            BEGIN
                IF gv_oci_file_name_poheader LIKE '%RcvHeadersInterface%' THEN
                    SELECT
                        COUNT(*)
                    INTO lv_row_count
                    FROM
                        xxcnv_po_c009_po_receipts_headers_stg;

                    dbms_output.put_line('Inserted Records in the xxcnv_po_c009_po_receipts_headers_stg from OCI Source Folder: ' || lv_row_count
                    );
                END IF;

                IF gv_oci_file_name_potrans LIKE '%RcvTransactionsInterface%' THEN
                    SELECT
                        COUNT(*)
                    INTO lv_row_count
                    FROM
                        xxcnv_po_c009_po_receipts_transactions_stg;

                    dbms_output.put_line('Inserted Records in the xxcnv_po_c009_po_receipts_transactions_stg from OCI Source Folder: ' || lv_row_count
                    );
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error counting rows in the external table: ' || sqlerrm);
                    p_loading_status := gv_status_failure;
                    RETURN;
            END;

        -- Count the number of rows in the external table

            BEGIN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_po_c009_po_receipts_headers_stg;

                dbms_output.put_line('Log:Inserted Records in the xxcnv_po_c009_po_receipts_headers_stg from OCI Source Folder: ' || lv_row_count
                );

		 -- Use an implicit cursor in the FOR LOOP to iterate over distinct book_type_code
                FOR rec IN (
                    SELECT DISTINCT
                        batch_id
                    FROM
                        xxcnv_po_c009_po_receipts_headers_stg
                    WHERE
                        execution_id = gv_execution_id
                ) LOOP
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_picked,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_poheader,
                        p_attribute1        => rec.batch_id,
                        p_attribute2        => lv_row_count,
                        p_process_reference => NULL
                    );
                END LOOP;

                p_loading_status := gv_status_success;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error counting rows in xxcnv_po_c009_po_receipts_headers_stg: ' || sqlerrm);
                    p_loading_status := gv_status_failure;
                    RETURN;
            END;

            BEGIN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_po_c009_po_receipts_transactions_stg;

                dbms_output.put_line('Log:Inserted Records in the xxcnv_po_c009_po_receipts_transactions_stg from OCI Source Folder: ' || lv_row_count
                );

		 -- Use an implicit cursor in the FOR LOOP to iterate over distinct book_type_code
                FOR rec IN (
                    SELECT DISTINCT
                        batch_id
                    FROM
                        xxcnv_po_c009_po_receipts_transactions_stg
                    WHERE
                        execution_id = gv_execution_id
                ) LOOP
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_picked,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_poheader,
                        p_attribute1        => rec.batch_id,
                        p_attribute2        => lv_row_count,
                        p_process_reference => NULL
                    );
                END LOOP;

                p_loading_status := gv_status_success;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error counting rows in xxcnv_po_c009_po_receipts_transactions_stg: ' || sqlerrm);
                    p_loading_status := gv_status_failure;
                    RETURN;
            END;

        END;

    END import_data_from_oci_to_stg_prc;

/*=================================================================================================================
-- PROCEDURE : data_validations_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for the validating the mandatory columns and business validations as per lean spec
===================================================================================================================*/
    PROCEDURE data_validations_prc IS

  -- Declaring Local Variables for validation.     
        lv_row_count     NUMBER;
        lv_error_count   NUMBER;
        lv_source_code_1 VARCHAR2(2) := '01';
        lv_source_code_2 VARCHAR2(2) := '02';
        lv_source_code_3 VARCHAR2(2) := '03';
    BEGIN
        BEGIN
            lv_error_count := 0;
            BEGIN
                UPDATE xxcnv_po_c009_po_receipts_headers_stg
                SET
                    execution_id = gv_execution_id,
                    batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END;
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_po_c009_po_receipts_headers_stg
            WHERE
                execution_id = gv_execution_id;

            IF lv_row_count <> 0 THEN 

       -- Initialize ERROR_MESSAGE to an empty string if it is NULL
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        error_message = ''
                    WHERE
                        error_message IS NULL
                        AND execution_id = gv_execution_id;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('An error occurred while initializing ERROR_MESSAGE: '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);
                END;

     -- Set receipt source code, ASN type and Transaction as per mapping doc
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        receipt_source_code = 'VENDOR',
                        asn_type = 'STD',
                        transaction_type = 'NEW'
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Constant values are updated');
                END;

	--validate Receipt number
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        error_message = error_message || '|Receipt number should not be NULL'
                    WHERE
                        receipt_num IS NULL
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Receipt number date is validated');
                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        vendor_name = (
                            SELECT
                                oc_vendor_name
                            FROM
                                xxcnv_ap_supplier_mapping
                            WHERE
                                ns_vendor_num = vendor_num
                            GROUP BY
                                oc_vendor_name
                        )
                    WHERE
                        vendor_num IS NOT NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        vendor_num = (
                            SELECT
                                oc_vendor_num
                            FROM
                                xxcnv_ap_supplier_mapping
                            WHERE
                                ns_vendor_num = vendor_num
                            GROUP BY
                                oc_vendor_num
                        )
                    WHERE
                        vendor_num IS NOT NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        error_message = error_message || '**Supplier number is null or not valid '
                    WHERE
                        vendor_num IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        vendor_name = '"'
                                      || vendor_name
                                      || '"'
                    WHERE
                        vendor_name LIKE '%,%'
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Supplier name With Comma is validated');
                END;

		 --Supplier Site Code dervive
		 --Change 1.2 for the bug LTCI-6622
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg hstg
                    SET
                        vendor_site_code = (
                            SELECT DISTINCT
                                pht.supplier_site_code
                            FROM
                                xxcnv_po_c007_po_headers_stg               pht,
                                xxcnv_po_c009_po_receipts_transactions_stg tstg
                            WHERE
                                    pht.document_num = tstg.document_num
                                AND tstg.header_interface_num = hstg.header_interface_num
                        )
                    WHERE
                        vendor_site_code IS NOT NULL
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('SUPPLIER Site Code is Updated');
                END;

	--validate expected receipt date
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        error_message = error_message || '|expected receipt date should not be NULL'
                    WHERE
                        expected_receipt_date IS NULL
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('expected receipt date is validated');
                END;

	--Update location code
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg stg
                    SET
                        location_code = (
                            SELECT
                                os_location_name
                            FROM
                                xxcnv_po_ship_to_location_mapping lmt
                            WHERE
                                lmt.ns_location_name = nvl(stg.location_code, 'ZZ')
                        )
                    WHERE
                        location_code IS NOT NULL
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('location code is upadted');
                END;
             
			 --V1.3 Change for the jira LTCI - 8107
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg hstg
                    SET
                        employee_name = (
                            SELECT
                                pds.deliver_to_person_full_name
                            FROM
                                xxcnv_po_c009_po_receipts_transactions_stg tstg,
                                xxcnv_po_c007_po_distributions_stg         pds,
                                xxcnv_po_c007_po_line_locations_stg        pll,
                                xxcnv_po_c007_po_lines_stg                 plt,
                                xxcnv_po_c007_po_headers_stg               pht
                            WHERE
                                    tstg.header_interface_num = hstg.header_interface_num
                                AND pht.document_num = tstg.document_num
                                AND plt.interface_header_key = pht.interface_header_key
                                AND plt.interface_line_key = pll.interface_line_key
                                AND pll.interface_line_location_key = pds.interface_line_location_key
                            GROUP BY
                                pds.deliver_to_person_full_name
                        )
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

	 --Update bu
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg stg
                    SET
                        business_unit = (
                            SELECT
                                oc_business_unit_name
                            FROM
                                xxcnv_gl_le_bu_mapping bmt
                            WHERE
                                bmt.ns_legal_entity_name = nvl(stg.business_unit, 'ZZ')
                        )
                    WHERE
                        business_unit IS NOT NULL
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('bu is upadted');
                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        business_unit = '"'
                                        || business_unit
                                        || '"'
                    WHERE
                        business_unit LIKE '%,%'
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Business unit With Comma is validated');
                END;

   -- Update import_status based on error_message
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        import_status =
                            CASE
                                WHEN error_message IS NOT NULL THEN
                                    'ERROR'
                                ELSE
                                    'PROCESSED'
                            END;
			--WHERE execution_id = gv_execution_id;
                    dbms_output.put_line('import_status is validated');
                END;

     -- Final update to set error_message and import_status
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
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
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        source_system = gv_boundary_system
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('source_system is updated');
                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg
                    SET
                        file_name = gv_oci_file_name_poheader
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
                    xxcnv_po_c009_po_receipts_headers_stg
                WHERE
                    error_message IS NOT NULL;

                dbms_output.put_line('file_name column is updated'
                                     || gv_execution_id
                                     || '_'
                                     || gv_status_failure);
                UPDATE xxcnv_po_c009_po_receipts_headers_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    error_message IS NOT NULL
                    AND file_reference_identifier IS NULL
	     --and execution_id = gv_execution_id 
                    ;

                dbms_output.put_line('file_reference_identifier column is updated');
                UPDATE xxcnv_po_c009_po_receipts_headers_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success
                WHERE
                    error_message IS NULL
                    AND file_reference_identifier IS NULL
		--and execution_id = gv_execution_id 
                    ;

                dbms_output.put_line('file_reference_identifier column is updated');
                IF lv_error_count > 0 THEN
                    dbms_output.put_line('file_reference_identifier column is updated');

	    -- Logging the message If data is not validated
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_failed,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_poheader,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                END IF;

                IF
                    lv_error_count = 0
                    AND gv_oci_file_name_poheader IS NOT NULL
                THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_validated,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_poheader,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

                IF gv_oci_file_name_poheader IS NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_file_not_found,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_poheader,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

            ELSE
                dbms_output.put_line('No Data is found in interface tables. Data is not loaded from ext to stg ');
            END IF;

        END;

----FILE 2
        BEGIN
            lv_error_count := 0;
            BEGIN
                UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                SET
                    execution_id = gv_execution_id,
                    batch_id = gv_batch_id
                WHERE
                    file_reference_identifier IS NULL;

            END;
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_po_c009_po_receipts_transactions_stg
            WHERE
                execution_id = gv_execution_id;

            IF lv_row_count <> 0 THEN 

        -- Initialize ERROR_MESSAGE to an empty string if it is NULL
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        error_message = ''
                    WHERE
                        error_message IS NULL
                        AND execution_id = gv_execution_id;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('An error occurred while initializing ERROR_MESSAGE: '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);
                END;

		--Erroring out the record in child table as it errored out in parent table
                BEGIN
              -- Update the import_status in XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG to 'ERROR' where the interface_header_key IN XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG  has import_status 'ERROR'
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        error_message = error_message || '|Parent Record failed at validation',
                        import_status = 'ERROR'
                    WHERE
                        header_interface_num IN (
                            SELECT
                                header_interface_num
                            FROM
                                xxcnv_po_c009_po_receipts_headers_stg
                            WHERE
                                    import_status = 'ERROR'
                                AND execution_id = gv_execution_id
                        )
                        AND execution_id = gv_execution_id
                        AND error_message IS NULL;
			 -- and file_reference_identifier is null
                END;

		--Error out po indirect receipts		
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg stg
                    SET
                        error_message = error_message || '**Receipt Lines not eligible for the load'
                    WHERE
                        ( item_num IS NULL
                          OR item_num NOT IN (
                            SELECT
                                oc_item
                            FROM
                                xxcnv_po_receipts_item_list_mapping
                        ) )--Updated Mapping table name for v1.1
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

		--V1.2 Change for the bug LTCI - 6593
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg stg
                    SET
                        error_message = error_message || '**Line Error : Receipt Headers not eligible for the load',
                        import_status = 'ERROR',
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                        header_interface_num IS NOT NULL
                        AND header_interface_num = (
                            SELECT DISTINCT
                                header_interface_num
                            FROM
                                xxcnv_po_c009_po_receipts_transactions_stg
                            WHERE
                                error_message IS NOT NULL
                                AND error_message LIKE '%**Receipt Lines not eligible for the load%'
                                AND header_interface_num = stg.header_interface_num
                            GROUP BY
                                header_interface_num
                            HAVING
                                COUNT(*) = (
                                    SELECT
                                        COUNT(*)
                                    FROM
                                        xxcnv_po_c009_po_receipts_transactions_stg
                                    WHERE
                                        header_interface_num = stg.header_interface_num
                                )
                        )
                        AND execution_id = gv_execution_id;

                END;

                UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                SET
                    transaction_type = 'RECEIVE',
                    auto_transact_code = 'DELIVER',
                    source_document_code = 'PO',
                    receipt_source_code = 'VENDOR',
                    interface_source_code = 'RCV',
                    unit_of_measure = 'Each',
                    subinventory = ''
                WHERE
                        execution_id = gv_execution_id
                    AND file_reference_identifier IS NULL;

        --Update Organization code from po lines
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg stg
                    SET
                        to_organization_code = (
                            SELECT
                                pll.ship_to_organization_code
                            FROM
                                xxcnv_po_c007_po_line_locations_stg pll,
                                xxcnv_po_c007_po_lines_stg          plt,
                                xxcnv_po_c007_po_headers_stg        pht
                            WHERE
                                    pht.document_num = stg.document_num
                                AND plt.line_num = stg.document_line_num
                                AND plt.interface_header_key = pht.interface_header_key
                                AND plt.interface_line_key = pll.interface_line_key
                        )
                    WHERE
                            execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('ship to location code is validated');
                END;	   

		--added new derivation logic for v1.1
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg stg
                    SET
                        item_description = (
                            SELECT
                                substr(plt.item_description, 1, 240)
                            FROM
                                xxcnv_po_c007_po_lines_stg   plt,
                                xxcnv_po_c007_po_headers_stg pht
                            WHERE
                                    pht.document_num = stg.document_num
                                AND plt.line_num = stg.document_line_num
                                AND plt.interface_header_key = pht.interface_header_key
                        )
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

		--added new derivation logic for v1.1
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg stg
                    SET
                        item_num = (
                            SELECT
                                plt.item
                            FROM
                                xxcnv_po_c007_po_lines_stg   plt,
                                xxcnv_po_c007_po_headers_stg pht
                            WHERE
                                    pht.document_num = stg.document_num
                                AND plt.line_num = stg.document_line_num
                                AND plt.interface_header_key = pht.interface_header_key
                        )
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        error_message = error_message || '**PO Number is not valid. '
                    WHERE
                        document_num IS NOT NULL
                        AND document_num NOT IN (
                            SELECT
                                document_num
                            FROM
                                xxcnv_po_c009_po_headers_mapping
                        )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id
                        AND error_message IS NULL;

                    dbms_output.put_line('PO number is validated');
                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        error_message = error_message || '**PO Number Should not be null '
                    WHERE
                        document_num IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id
                        AND error_message IS NULL;

                    dbms_output.put_line('PO number is validated for Null');
                END;

        --V1.2 Change for the bug LTCI - 6593
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg stg
                    SET
                        error_message = error_message || '**Line Error : PO Number is not valid.',
                        import_status = 'ERROR',
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                        header_interface_num IS NOT NULL
                        AND header_interface_num = (
                            SELECT
                                header_interface_num
                            FROM
                                xxcnv_po_c009_po_receipts_transactions_stg
                            WHERE
                                error_message IS NOT NULL
                                AND header_interface_num = stg.header_interface_num
                            GROUP BY
                                header_interface_num
                            HAVING
                                COUNT(*) = (
                                    SELECT
                                        COUNT(*)
                                    FROM
                                        xxcnv_po_c009_po_receipts_transactions_stg
                                    WHERE
                                        header_interface_num = stg.header_interface_num
                                )
                        )
                        AND execution_id = gv_execution_id
                        AND error_message IS NULL;

                    dbms_output.put_line('PO number is validated');
                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg stg
                    SET
                        error_message = error_message || '**PO line Number is not valid. '
                    WHERE
                        document_line_num IS NOT NULL
                        AND document_line_num NOT IN (
                            SELECT
                                plt.line_num
                            FROM
                                xxcnv_po_c009_po_lines_mapping plt
                            WHERE
                                plt.document_num = stg.document_num
                        )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id
                        AND error_message IS NULL;

                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        error_message = error_message || '**PO Number should not be null'
                    WHERE
                        document_line_num IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id
                        AND error_message IS NULL;

                    dbms_output.put_line('PO line number is validated for Null');
                END;

        --V1.2 Change for the bug LTCI - 6593		
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg stg
                    SET
                        error_message = error_message || '**Line Error : PO line Number is not valid.',
                        import_status = 'ERROR',
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                        header_interface_num IS NOT NULL
                        AND header_interface_num = (
                            SELECT
                                header_interface_num
                            FROM
                                xxcnv_po_c009_po_receipts_transactions_stg
                            WHERE
                                error_message IS NOT NULL
                                AND header_interface_num = stg.header_interface_num
                            GROUP BY
                                header_interface_num
                            HAVING
                                COUNT(*) = (
                                    SELECT
                                        COUNT(*)
                                    FROM
                                        xxcnv_po_c009_po_receipts_transactions_stg
                                    WHERE
                                        header_interface_num = stg.header_interface_num
                                )
                        )
                        AND execution_id = gv_execution_id
                        AND error_message IS NULL;

                    dbms_output.put_line('PO number is validated');
                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        document_shipment_line_num = document_line_num,
                        document_distribution_num = document_line_num
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('PO line number is validated for Null');
                END;

		--Update bu
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg stg
                    SET
                        business_unit = (
                            SELECT
                                oc_business_unit_name
                            FROM
                                xxcnv_gl_le_bu_mapping bmt
                            WHERE
                                bmt.ns_legal_entity_name = stg.business_unit
                        )
                    WHERE
                        business_unit IS NOT NULL
                        AND file_reference_identifier IS NULL
                        AND error_message IS NULL;

                    dbms_output.put_line('bu is upadted');
                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        business_unit = '"'
                                        || business_unit
                                        || '"'
                    WHERE
                        business_unit LIKE '%,%'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        error_message = error_message || '**BU Should not be null '
                    WHERE
                        business_unit IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id
                        AND error_message IS NULL;

                    dbms_output.put_line('BU is validated');
                END;

	   --validate expected receipt date
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        error_message = error_message || '**expected receipt date should not be NULL'
                    WHERE
                        expected_receipt_date IS NULL
                        AND file_reference_identifier IS NULL
                        AND error_message IS NULL;

                    dbms_output.put_line('expected receipt date is validated');
                END;

		--Update Organization code
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg stg
                    SET
                        ship_to_location_code = (
                            SELECT
                                pll.ship_to_location
                            FROM
                                xxcnv_po_c007_po_line_locations_stg pll,
                                xxcnv_po_c007_po_lines_stg          plt,
                                xxcnv_po_c007_po_headers_stg        pht
                            WHERE
                                    pht.document_num = stg.document_num
                                AND plt.line_num = stg.document_line_num
                                AND plt.interface_header_key = pht.interface_header_key
                                AND plt.interface_line_key = pll.interface_line_key
                        )
                    WHERE
                            execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('ship to location code is validated');
                END;
             
			 --V1.3 Change for the jira LTCI - 8107
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg stg
                    SET
                        deliver_to_person_name = (
                            SELECT
                                pds.deliver_to_person_full_name
                            FROM
                                xxcnv_po_c007_po_distributions_stg  pds,
                                xxcnv_po_c007_po_line_locations_stg pll,
                                xxcnv_po_c007_po_lines_stg          plt,
                                xxcnv_po_c007_po_headers_stg        pht
                            WHERE
                                    pht.document_num = stg.document_num
                                AND plt.line_num = stg.document_line_num
                                AND plt.interface_header_key = pht.interface_header_key
                                AND plt.interface_line_key = pll.interface_line_key
                                AND pll.interface_line_location_key = pds.interface_line_location_key
                        )
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg stg
                    SET
                        deliver_to_location_code = (
                            SELECT
                                os_location_name
                            FROM
                                xxcnv_po_ship_to_location_mapping lmt
                            WHERE
                                lmt.ns_location_name = nvl(stg.deliver_to_location_code, 'ZZ')
                        )
                    WHERE
                            execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('delivery to location code is updated');
                END;

               --V1.3 Change for the jira LTCI - 8107
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg stg
                    SET
                        subinventory = (
                            SELECT
                                pds.destination_subinventory
                            FROM
                                xxcnv_po_c007_po_distributions_stg  pds,
                                xxcnv_po_c007_po_line_locations_stg pll,
                                xxcnv_po_c007_po_lines_stg          plt,
                                xxcnv_po_c007_po_headers_stg        pht
                            WHERE
                                    pht.document_num = stg.document_num
                                AND plt.line_num = stg.document_line_num
                                AND plt.interface_header_key = pht.interface_header_key
                                AND plt.interface_line_key = pll.interface_line_key
                                AND pll.interface_line_location_key = pds.interface_line_location_key
                        )
                    WHERE
                            execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('sub Inventory is updated');
                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        vendor_name = (
                            SELECT
                                oc_vendor_name
                            FROM
                                xxcnv_ap_supplier_mapping
                            WHERE
                                ns_vendor_num = vendor_num
                            GROUP BY
                                oc_vendor_name
                        )
                    WHERE
                        vendor_num IS NOT NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        vendor_num = (
                            SELECT
                                oc_vendor_num
                            FROM
                                xxcnv_ap_supplier_mapping
                            WHERE
                                ns_vendor_num = vendor_num
                            GROUP BY
                                oc_vendor_num
                        )
                    WHERE
                        vendor_num IS NOT NULL
                        AND error_message IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        error_message = error_message || '**Supplier Number is null or not valid '
                    WHERE
                        vendor_num IS NULL
                        AND error_message IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        vendor_name = '"'
                                      || vendor_name
                                      || '"'
                    WHERE
                        vendor_name LIKE '%,%'
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_headers_stg stg
                    SET
                        error_message = error_message || '**Line Error : Supplier Number is null or not valid',
                        import_status = 'ERROR',
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                        header_interface_num IS NOT NULL
                        AND header_interface_num = (
                            SELECT
                                header_interface_num
                            FROM
                                xxcnv_po_c009_po_receipts_transactions_stg
                            WHERE
                                error_message IS NOT NULL
                                AND header_interface_num = stg.header_interface_num
                            GROUP BY
                                header_interface_num
                            HAVING
                                COUNT(*) = (
                                    SELECT
                                        COUNT(*)
                                    FROM
                                        xxcnv_po_c009_po_receipts_transactions_stg
                                    WHERE
                                        header_interface_num = stg.header_interface_num
                                )
                        )
                        AND error_message IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Quantity netting is done');
                END;

       -- Update import_status based on error_message
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        import_status =
                            CASE
                                WHEN error_message IS NOT NULL THEN
                                    'ERROR'
                                ELSE
                                    'PROCESSED'
                            END;
			--WHERE execution_id = gv_execution_id;
                    dbms_output.put_line('import_status is validated');
                END;

     -- Final update to set error_message and import_status
                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
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
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        file_name = gv_oci_file_name_potrans
                    WHERE
                        file_reference_identifier IS NULL;

                    dbms_output.put_line('file_name column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        source_system = gv_boundary_system
                    WHERE
                        execution_id = gv_execution_id;

                    dbms_output.put_line('source_system is updated');
                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                        file_reference_identifier IS NULL
                        AND error_message IS NOT NULL;

                    dbms_output.put_line('file_reference_identifier column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_po_c009_po_receipts_transactions_stg
                    SET
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success
                    WHERE
                        error_message IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('file_reference_identifier column is updated');
                END;

-- Check if there are any error messages
                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_po_c009_po_receipts_transactions_stg
                WHERE
                    error_message IS NOT NULL
                    AND execution_id = gv_execution_id;

                IF lv_error_count > 0 THEN

	       -- Logging the message If data is not validated
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_failed,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_potrans,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

                IF --lv_error_count = 0 AND 

                 gv_oci_file_name_potrans IS NOT NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_validated,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_potrans,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

	 -- commit;
--	 
                IF gv_oci_file_name_potrans IS NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_file_not_found,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_potrans,
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

/*=================================================================================================================
-- PROCEDURE : rec_qty_update_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for the validating the mandatory columns and business validations as per lean spec
===================================================================================================================*/
    PROCEDURE rec_qty_update_prc IS
        CURSOR qty_update IS
        SELECT
            po_doc,
            po_doc_line,
            receipt_number,
            ( receipt_quantity - billed_quantity ) AS new_qty
        FROM
            xxcnv_po_receipts_quantity_mapping;

    BEGIN
        FOR g_id IN qty_update LOOP
            UPDATE xxcnv_po_c009_po_receipts_transactions_stg
            SET
                billed_flag = 'Y',
                new_quantity = g_id.new_qty
            WHERE
                    header_interface_num = (
                        SELECT
                            header_interface_num
                        FROM
                            xxcnv_po_c009_po_receipts_headers_stg
                        WHERE
                            receipt_num = g_id.receipt_number
                    )
                AND document_num = g_id.po_doc
                AND document_line_num = g_id.po_doc_line
                AND execution_id = gv_execution_id;

        END LOOP;

        BEGIN
            UPDATE xxcnv_po_c009_po_receipts_transactions_stg
            SET
                error_message = error_message || '**Quantity is zero',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND new_quantity <= 0;

        END;

        BEGIN
            UPDATE xxcnv_po_c009_po_receipts_headers_stg stg
            SET
                error_message = error_message || '**Line Error : Quantity is zero',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure
            WHERE
                header_interface_num IS NOT NULL
                AND header_interface_num = (
                    SELECT
                        header_interface_num
                    FROM
                        xxcnv_po_c009_po_receipts_transactions_stg
                    WHERE
                        error_message IS NOT NULL
                        AND header_interface_num = stg.header_interface_num
                    GROUP BY
                        header_interface_num
                    HAVING
                        COUNT(*) = (
                            SELECT
                                COUNT(*)
                            FROM
                                xxcnv_po_c009_po_receipts_transactions_stg
                            WHERE
                                header_interface_num = stg.header_interface_num
                        )
                )
                AND error_message IS NULL
                AND execution_id = gv_execution_id;

        END;

        BEGIN
            UPDATE xxcnv_po_c009_po_receipts_transactions_stg
            SET
                error_message = error_message || '**Loading the Latest receipt and Errored out other Lines',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure
            WHERE
                    execution_id = gv_execution_id
                AND new_quantity IS NULL;

        END;

	--V1.2 Change for the bug LTCI - 6593
        BEGIN
            UPDATE xxcnv_po_c009_po_receipts_headers_stg stg
            SET
                error_message = error_message || '**Line Error : Loading the Latest receipt and Errored out other Lines',
                import_status = 'ERROR',
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure
            WHERE
                header_interface_num IS NOT NULL
                AND header_interface_num = (
                    SELECT
                        header_interface_num
                    FROM
                        xxcnv_po_c009_po_receipts_transactions_stg
                    WHERE
                        error_message IS NOT NULL
                        AND header_interface_num = stg.header_interface_num
                    GROUP BY
                        header_interface_num
                    HAVING
                        COUNT(*) = (
                            SELECT
                                COUNT(*)
                            FROM
                                xxcnv_po_c009_po_receipts_transactions_stg
                            WHERE
                                header_interface_num = stg.header_interface_num
                        )
                )
                AND error_message IS NULL
                AND execution_id = gv_execution_id;

            dbms_output.put_line('Quantity netting is done');
        END;

    END rec_qty_update_prc;

/*==============================================================================================================================
-- PROCEDURE : create_fbdi_file_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the FBDI CSV file by using the data in the po Receipt stage tables after all validations.
================================================================================================================================= */
    PROCEDURE create_fbdi_file_prc IS

        CURSOR batch_id_cursor IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_po_c009_po_receipts_headers_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_success;

        CURSOR batch_id_cursor_lines IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_po_c009_po_receipts_transactions_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_success;

        lv_success_count NUMBER := 0;
        lv_batch_id      VARCHAR(200);
    BEGIN
--table 1
        BEGIN
            FOR g_id IN batch_id_cursor LOOP
                lv_batch_id := g_id.batch_id;
                dbms_output.put_line('Creating FBDI file for batch_id: ' || lv_batch_id);
                BEGIN
                -- Count the success record count for the current batch_id
                    SELECT
                        COUNT(*)
                    INTO lv_success_count
                    FROM
                        xxcnv_po_c009_po_receipts_headers_stg
                    WHERE
                            batch_id = lv_batch_id
                        AND file_reference_identifier = gv_execution_id
                                                        || '_'
                                                        || gv_status_success;

                    dbms_output.put_line('Success record count for XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG for batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || lv_success_count);
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG for batch_id: ' || lv_batch_id)
                        ;
                        RETURN; --
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error checking success record count for XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG for batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                        RETURN; --
                END;

                IF lv_success_count > 0 THEN
                    BEGIN
                        dbms_cloud.export_data(
                            credential_name => gv_credential_name,
                            file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                             || '/'
                                             || lv_batch_id
                                             || gv_oci_file_name_poheader,
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv',
                                        'trimspaces' VALUE 'rtrim',
                                        'maxfilesize' VALUE '629145600',
                                        'header' VALUE FALSE
                                    ),
                            query           => 'SELECT 
                                              HEADER_INTERFACE_NUM,
									RECEIPT_SOURCE_CODE,
									ASN_TYPE,
									TRANSACTION_TYPE,
									TO_CHAR(TO_DATE(NOTICE_CREATION_DATE,''MM/DD/YYYY''), ''YYYY/MM/DD'') AS     NOTICE_CREATION_DATE,
									SHIPMENT_NUM,
									RECEIPT_NUM,
									VENDOR_NAME,
									VENDOR_NUM,
									VENDOR_SITE_CODE,
									FROM_ORGANIZATION_CODE,
									SHIP_TO_ORGANIZATION_CODE,
									LOCATION_CODE,
									BILL_OF_LADING,
									PACKING_SLIP,
									SHIPPED_DATE,
									FREIGHT_CARRIER_NAME,
									TO_CHAR(TO_DATE(EXPECTED_RECEIPT_DATE,''MM/DD/YYYY''), ''YYYY/MM/DD'') AS     EXPECTED_RECEIPT_DATE,
									NUM_OF_CONTAINERS,
									WAYBILL_AIRBILL_NUM,
									COMMENTS,
									GROSS_WEIGHT,
									GROSS_WEIGHT_UNIT_OF_MEASURE,
									NET_WEIGHT,
									NET_WEIGHT_UNIT_OF_MEASURE,
									TAR_WEIGHT,
									TAR_WEIGHT_UNIT_OF_MEASURE,
									PACKAGING_CODE,
									CARRIER_METHOD,
									CARRIER_EQUIPMENT,
									SPECIAL_HANDLING_CODE,
									HAZARD_CODE,
									HAZARD_CLASS,
									HAZARD_DESCRIPTION,
									FREIGHT_TERMS,
									FREIGHT_BILL_NUMBER,
									INVOICE_NUM,
									INVOICE_DATE,
									TOTAL_INVOICE_AMOUNT,
									TAX_NAME,
									TAX_AMOUNT,
									FREIGHT_AMOUNT,
									CURRENCY_CODE,
									CONVERSION_RATE_TYPE,
									CONVERSION_RATE,
									CONVERSION_RATE_DATE,
									PAYMENT_TERMS_NAME,
									EMPLOYEE_NAME,
									TRANSACTION_DATE,
									CUSTOMER_ACCOUNT_NUMBER,
									CUSTOMER_PARTY_NAME,
									CUSTOMER_PARTY_NUMBER,
									BUSINESS_UNIT,
									RA_OUTSOURCER_PARTY_NAME,
									RECEIPT_ADVICE_NUMBER,
									RA_DOCUMENT_CODE,
									RA_DOCUMENT_NUMBER,
									RA_DOC_REVISION_NUMBER,
									RA_DOC_REVISION_DATE,
									RA_DOC_CREATION_DATE,
									RA_DOC_LAST_UPDATE_DATE,
									RA_OUTSOURCER_CONTACT_NAME,
									RA_VENDOR_SITE_NAME,
									RA_NOTE_TO_RECEIVER,
									RA_DOO_SOURCE_SYSTEM_NAME,
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
									ATTRIBUTE_DATE1,
									ATTRIBUTE_DATE2,
									ATTRIBUTE_DATE3,
									ATTRIBUTE_DATE4,
									ATTRIBUTE_DATE5,
									ATTRIBUTE_TIMESTAMP1,
									ATTRIBUTE_TIMESTAMP2,
									ATTRIBUTE_TIMESTAMP3,
									ATTRIBUTE_TIMESTAMP4,
									ATTRIBUTE_TIMESTAMP5,
									GL_DATE,
									RECEIPT_HEADER_ID,
									EXTERNAL_SYS_TXN_REFERENCE   
                                            FROM XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG
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

                        dbms_output.put_line('CSV file for XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG for batch_id '
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
                                           || gv_oci_file_name_poheader,
                            p_attribute1        => lv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );

                    EXCEPTION
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error exporting data to CSV for XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG for batch_id '
                                                 || lv_batch_id
                                                 || ': '
                                                 || sqlerrm);
                            RETURN;
                    END;
                ELSE
                    dbms_output.put_line('Process Stopped for XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG for batch_id '
                                         || lv_batch_id
                                         || ': Error message columns contain data.');
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
        END;

--TABLE 2

        BEGIN
            BEGIN
                lv_success_count := 0;
                BEGIN
                -- Count the success record count for the current batch_id
                    SELECT
                        COUNT(*)
                    INTO lv_success_count
                    FROM
                        xxcnv_po_c009_po_receipts_transactions_stg
                    WHERE
                            batch_id = lv_batch_id
                --AND error_message IS NOT NULL
                        AND file_reference_identifier = gv_execution_id
                                                        || '_'
                                                        || gv_status_success;
               -- AND TRIM(error_message) != '';

                    dbms_output.put_line('Success record count for XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG for batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || lv_success_count);
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG for batch_id: ' || lv_batch_id
                        );
                        RETURN;
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error checking success record count for XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG for batch_id '
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
                                             || gv_oci_file_name_potrans,
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv',
                                        'trimspaces' VALUE 'rtrim',
                                        'maxfilesize' VALUE '629145600',
                                        'header' VALUE FALSE
                                    ),
                            query           => '
					SELECT 
					Interface_Line_Number,
       		        TRANSACTION_TYPE,
       		        AUTO_TRANSACT_CODE,
       		        TO_CHAR(TO_DATE(TRANSACTION_DATE,''MM/DD/YYYY''), ''YYYY/MM/DD'') AS     TRANSACTION_DATE,
       		        SOURCE_DOCUMENT_CODE,
       		        RECEIPT_SOURCE_CODE,
       		        HEADER_INTERFACE_NUM,
       		        PARENT_TRANSACTION_ID,
       		        PARENT_INTF_LINE_NUM,
       		        TO_ORGANIZATION_CODE,
       		        ITEM_NUM,
       		        ITEM_DESCRIPTION,
       		        ITEM_REVISION,
       		        DOCUMENT_NUM,
       		        DOCUMENT_LINE_NUM,
       		        DOCUMENT_SHIPMENT_LINE_NUM,
       		        DOCUMENT_DISTRIBUTION_NUM,
       		        BUSINESS_UNIT,
       		        SHIPMENT_NUM,
       		        TO_CHAR(TO_DATE(EXPECTED_RECEIPT_DATE,''MM/DD/YYYY''), ''YYYY/MM/DD'') AS     EXPECTED_RECEIPT_DATE,
       		        SUBINVENTORY,
       		        LOCATOR,
       		        NEW_QUANTITY,
       		        UNIT_OF_MEASURE,
       		        PRIMARY_QUANTITY,
       		        PRIMARY_UNIT_OF_MEASURE,
       		        SECONDARY_QUANTITY,
       		        SECONDARY_UNIT_OF_MEASURE,
       		        VENDOR_NAME,
       		        VENDOR_NUM,
       		        VENDOR_SITE_CODE,
       		        CUSTOMER_PARTY_NAME,
       		        CUSTOMER_PARTY_NUMBER,
       		        CUSTOMER_ACCOUNT_NUMBER,
       		        SHIP_TO_LOCATION_CODE,
       		        LOCATION_CODE,
       		        REASON_NAME,
       		        DELIVER_TO_PERSON_NAME,
       		        DELIVER_TO_LOCATION_CODE,
       		        RECEIPT_EXCEPTION_FLAG,
       		        ROUTING_HEADER_ID,
       		        DESTINATION_TYPE_CODE,
       		        INTERFACE_SOURCE_CODE,
       		        INTERFACE_SOURCE_LINE_ID,
       		        AMOUNT,
       		        CURRENCY_CODE,
       		        CURRENCY_CONVERSION_TYPE,
       		        CURRENCY_CONVERSION_RATE,
       		        CURRENCY_CONVERSION_DATE,
       		        INSPECTION_STATUS_CODE,
       		        INSPECTION_QUALITY_CODE,
       		        FROM_ORGANIZATION_CODE,
       		        FROM_SUBINVENTORY,
       		        FROM_LOCATOR,
       		        FREIGHT_CARRIER_NAME,
       		        BILL_OF_LADING,
       		        PACKING_SLIP,
       		        SHIPPED_DATE,
       		        NUM_OF_CONTAINERS,
       		        WAYBILL_AIRBILL_NUM,
       		        RMA_REFERENCE,
       		        COMMENTS,
       		        TRUCK_NUM,
       		        CONTAINER_NUM,
       		        SUBSTITUTE_ITEM_NUM,
       		        NOTICE_UNIT_PRICE,
       		        ITEM_CATEGORY,
       		        INTRANSIT_OWNING_ORG_CODE,
       		        ROUTING_CODE,
       		        BARCODE_LABEL,
       		        COUNTRY_OF_ORIGIN_CODE,
       		        CREATE_DEBIT_MEMO_FLAG,
       		        LICENSE_PLATE_NUMBER,
       		        TRANSFER_LICENSE_PLATE_NUMBER,
       		        LPN_GROUP_NUM,
       		        ASN_LINE_NUM,
       		        EMPLOYEE_NAME,
       		        SOURCE_TRANSACTION_NUM,
       		        PARENT_SOURCE_TRANSACTION_NUM,
       		        PARENT_INTERFACE_TXN_ID,
       		        MATCHING_BASIS,
       		        RA_OUTSOURCER_PARTY_NAME,
       		        RA_DOCUMENT_NUMBER,
       		        RA_DOCUMENT_LINE_NUMBER,
       		        RA_NOTE_TO_RECEIVER,
       		        RA_VENDOR_SITE_NAME,
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
       		        TO_CHAR(TO_DATE(ATTRIBUTE_DATE1,''MM/DD/YYYY''), ''YYYY/MM/DD'') AS     ATTRIBUTE_DATE1,
       		        TO_CHAR(TO_DATE(ATTRIBUTE_DATE2,''MM/DD/YYYY''), ''YYYY/MM/DD'') AS     ATTRIBUTE_DATE2,
       		        TO_CHAR(TO_DATE(ATTRIBUTE_DATE3,''MM/DD/YYYY''), ''YYYY/MM/DD'') AS     ATTRIBUTE_DATE3,
       		        ATTRIBUTE_DATE4,
       		        ATTRIBUTE_DATE5,
       		        ATTRIBUTE_TIMESTAMP1,
       		        ATTRIBUTE_TIMESTAMP2,
       		        ATTRIBUTE_TIMESTAMP3,
       		        ATTRIBUTE_TIMESTAMP4,
       		        ATTRIBUTE_TIMESTAMP5,
       		        CONSIGNED_FLAG,
       		        SOLDTO_LEGAL_ENTITY,
       		        CONSUMED_QUANTITY,
       		        DEFAULT_TAXATION_COUNTRY,
       		        TRX_BUSINESS_CATEGORY,
       		        DOCUMENT_FISCAL_CLASSIFICATION,
       		        USER_DEFINED_FISC_CLASS,
       		        PRODUCT_FISC_CLASS_NAME,
       		        INTENDED_USE,
       		        PRODUCT_CATEGORY,
       		        TAX_CLASSIFICATION_CODE,
       		        PRODUCT_TYPE,
       		        FIRST_PTY_NUMBER,
       		        THIRD_PTY_NUMBER,
       		        TAX_INVOICE_NUMBER,
       		        TAX_INVOICE_DATE,
       		        FINAL_DISCHARGE_LOC_CODE,
       		        ASSESSABLE_VALUE,
       		        PHYSICAL_RETURN_REQD,
       		        EXTERNAL_SYSTEM_PACKING_UNIT,
       		        EWAY_BILL_NUMBER,
       		        EWAY_BILL_DATE,
       		        RECALL_NOTICE_NUMBER,
       		        RECALL_LINE_NUMBER,
       		        EXTERNAL_SYS_TXN_REFERENCE,
       		        DEFAULT_LOTSER_FROM_ASN
					FROM XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG
                                            WHERE import_status = '''
                                     || 'PROCESSED'
                                     || '''
											and BILLED_FLAG = '''
                                     || 'Y'
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

                        dbms_output.put_line('CSV file for XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG for batch_id '
                                             || lv_batch_id
                                             || ' exported successfully to AP_INVOICE_LINES OCI Object Storage.');
                        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                            p_conversion_id     => gv_conversion_id,
                            p_execution_id      => gv_execution_id,
                            p_execution_step    => gv_fbdi_export_status,
                            p_boundary_system   => gv_boundary_system,
                            p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                            p_file_name         => lv_batch_id
                                           || '_'
                                           || gv_oci_file_name_potrans,
                            p_attribute1        => lv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );

                    EXCEPTION
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error exporting data to CSV for XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG for batch_id '
                                                 || lv_batch_id
                                                 || ': '
                                                 || sqlerrm);
                            RETURN;
                    END;
                ELSE
                    dbms_output.put_line('Process Stopped for XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG for batch_id '
                                         || lv_batch_id
                                         || ': Error message columns contain data.');
                    RETURN;
                END IF;

                --dbms_output.put_line('FBDI created ' || lv_batch_id);

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    RETURN;
            END;

        END;

    END create_fbdi_file_prc;	

/*==============================================================================================================================
-- PROCEDURE : create_properties_file_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating properties file.
================================================================================================================================= */
    PROCEDURE create_properties_file_prc IS

        CURSOR batch_id_cursor IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_po_c009_po_receipts_headers_stg
        WHERE
            execution_id = gv_execution_id;
		--file_reference_identifier = gv_execution_id||'_'||gv_status_success;

        lv_error_count NUMBER;
        lv_batch_id    VARCHAR(250);
    BEGIN
        FOR g_id IN batch_id_cursor LOOP
            lv_batch_id := g_id.batch_id;
            dbms_output.put_line('Processing BATCH_ID: ' || lv_batch_id);
            BEGIN
                dbms_cloud.export_data(
                    credential_name => gv_credential_name,
                    file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                     || '/'
                                     || gv_batch_id
                                     || lv_batch_id
                                     || 'POReceiptsInt.properties',
                    format          =>
                            JSON_OBJECT(
                                'trimspaces' VALUE 'rtrim'
                            ),
                    query           => 'SELECT ''/oracle/apps/ess/financials/payables/Receipt/transactions/,APXIIMPT,POReceiptssInt,'
                             || lv_batch_id
                             || ',null,300000002224558,N,null,null,null,1000,CONVERSION,null,N,N,300000001891564,null,1''as column1 from dual'
                );

                dbms_output.put_line('Properties file for BATCH_ID '
                                     || lv_batch_id
                                     || ' exported successfully to OCI Object Storage.');
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_staged,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                    p_file_name         => gv_batch_id
                                   || lv_batch_id
                                   || '_'
                                   || 'POReceiptsInt.properties',
                    p_attribute1        => gv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error exporting data to properties for BATCH_ID '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
            END;

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

        CURSOR batch_id_cursor IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_po_c009_po_receipts_headers_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_cursor_lines IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_po_c009_po_receipts_transactions_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        lv_error_count NUMBER;
        lv_batch_id    VARCHAR(200);
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
                                     || 'ATP_Recon_PO_Receipt_Header'
                                     || '_'
                                     || gv_boundary_system
                                     || '_'
                                     || sysdate,
                    format          =>
                            JSON_OBJECT(
                                'type' VALUE 'csv',
                                'trimspaces' VALUE 'rtrim',
                                'maxfilesize' VALUE '629145600',
                                'header' VALUE TRUE
                            ),
                    query           => '
  SELECT 
                                    HEADER_INTERFACE_NUM,
									RECEIPT_SOURCE_CODE,
									ASN_TYPE,
									TRANSACTION_TYPE,
									TO_CHAR(TO_DATE(NOTICE_CREATION_DATE,''MM/DD/YYYY''), ''YYYY/MM/DD'') AS     NOTICE_CREATION_DATE,
									SHIPMENT_NUM,
									RECEIPT_NUM,
									VENDOR_NAME,
									VENDOR_NUM,
									VENDOR_SITE_CODE,
									FROM_ORGANIZATION_CODE,
									SHIP_TO_ORGANIZATION_CODE,
									LOCATION_CODE,
									BILL_OF_LADING,
									PACKING_SLIP,
									SHIPPED_DATE,
									FREIGHT_CARRIER_NAME,
									EXPECTED_RECEIPT_DATE,
									NUM_OF_CONTAINERS,
									WAYBILL_AIRBILL_NUM,
									COMMENTS,
									GROSS_WEIGHT,
									GROSS_WEIGHT_UNIT_OF_MEASURE,
									NET_WEIGHT,
									NET_WEIGHT_UNIT_OF_MEASURE,
									TAR_WEIGHT,
									TAR_WEIGHT_UNIT_OF_MEASURE,
									PACKAGING_CODE,
									CARRIER_METHOD,
									CARRIER_EQUIPMENT,
									SPECIAL_HANDLING_CODE,
									HAZARD_CODE,
									HAZARD_CLASS,
									HAZARD_DESCRIPTION,
									FREIGHT_TERMS,
									FREIGHT_BILL_NUMBER,
									INVOICE_NUM,
									INVOICE_DATE,
									TOTAL_INVOICE_AMOUNT,
									TAX_NAME,
									TAX_AMOUNT,
									FREIGHT_AMOUNT,
									CURRENCY_CODE,
									CONVERSION_RATE_TYPE,
									CONVERSION_RATE,
									CONVERSION_RATE_DATE,
									PAYMENT_TERMS_NAME,
									EMPLOYEE_NAME,
									TRANSACTION_DATE,
									CUSTOMER_ACCOUNT_NUMBER,
									CUSTOMER_PARTY_NAME,
									CUSTOMER_PARTY_NUMBER,
									BUSINESS_UNIT,
									RA_OUTSOURCER_PARTY_NAME,
									RECEIPT_ADVICE_NUMBER,
									RA_DOCUMENT_CODE,
									RA_DOCUMENT_NUMBER,
									RA_DOC_REVISION_NUMBER,
									RA_DOC_REVISION_DATE,
									RA_DOC_CREATION_DATE,
									RA_DOC_LAST_UPDATE_DATE,
									RA_OUTSOURCER_CONTACT_NAME,
									RA_VENDOR_SITE_NAME,
									RA_NOTE_TO_RECEIVER,
									RA_DOO_SOURCE_SYSTEM_NAME,
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
									ATTRIBUTE_DATE1,
									ATTRIBUTE_DATE2,
									ATTRIBUTE_DATE3,
									ATTRIBUTE_DATE4,
									ATTRIBUTE_DATE5,
									ATTRIBUTE_TIMESTAMP1,
									ATTRIBUTE_TIMESTAMP2,
									ATTRIBUTE_TIMESTAMP3,
									ATTRIBUTE_TIMESTAMP4,
									ATTRIBUTE_TIMESTAMP5,
									GL_DATE,
									RECEIPT_HEADER_ID,
									EXTERNAL_SYS_TXN_REFERENCE,  
									file_name,
                               import_status,
                               error_message,
                               file_reference_identifier,
                               batch_id,
							   EXECUTION_ID  					,
                               source_system							   
                                    FROM XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG 
                                    where import_status = '''
                             || 'ERROR'
                             || '''
									and execution_id  =  '''
                             || gv_execution_id
                             || ''''
                );

                dbms_output.put_line('CSV file for XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG for batch_id '
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
                                   || gv_oci_file_name_poheader,
                    p_attribute1        => lv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error exporting data to CSV for XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG for batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    RETURN;
            END;

        END LOOP;

----Table 2

        BEGIN
            FOR g_id IN batch_id_cursor_lines LOOP
                lv_batch_id := g_id.batch_id;
        --dbms_output.put_line('Processing batch_id: ' || lv_batch_id);
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
                                         || 'ATP_Recon_PO_Receipt_Transactions'
                                         || sysdate,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600',
                                    'header' VALUE TRUE
                                ),
                        query           => '

       SELECT       Interface_Line_Number,
       		        TRANSACTION_TYPE,
       		        AUTO_TRANSACT_CODE,
       		        TRANSACTION_DATE,
       		        SOURCE_DOCUMENT_CODE,
       		        RECEIPT_SOURCE_CODE,
       		        HEADER_INTERFACE_NUM,
       		        PARENT_TRANSACTION_ID,
       		        PARENT_INTF_LINE_NUM,
       		        TO_ORGANIZATION_CODE,
       		        ITEM_NUM,
       		        ITEM_DESCRIPTION,
       		        ITEM_REVISION,
       		        DOCUMENT_NUM,
       		        DOCUMENT_LINE_NUM,
       		        DOCUMENT_SHIPMENT_LINE_NUM,
       		        DOCUMENT_DISTRIBUTION_NUM,
       		        BUSINESS_UNIT,
       		        SHIPMENT_NUM,
       		        EXPECTED_RECEIPT_DATE,
       		        SUBINVENTORY,
       		        LOCATOR,
       		        NEW_QUANTITY,
       		        UNIT_OF_MEASURE,
       		        PRIMARY_QUANTITY,
       		        PRIMARY_UNIT_OF_MEASURE,
       		        SECONDARY_QUANTITY,
       		        SECONDARY_UNIT_OF_MEASURE,
       		        VENDOR_NAME,
       		        VENDOR_NUM,
       		        VENDOR_SITE_CODE,
       		        CUSTOMER_PARTY_NAME,
       		        CUSTOMER_PARTY_NUMBER,
       		        CUSTOMER_ACCOUNT_NUMBER,
       		        SHIP_TO_LOCATION_CODE,
       		        LOCATION_CODE,
       		        REASON_NAME,
       		        DELIVER_TO_PERSON_NAME,
       		        DELIVER_TO_LOCATION_CODE,
       		        RECEIPT_EXCEPTION_FLAG,
       		        ROUTING_HEADER_ID,
       		        DESTINATION_TYPE_CODE,
       		        INTERFACE_SOURCE_CODE,
       		        INTERFACE_SOURCE_LINE_ID,
       		        AMOUNT,
       		        CURRENCY_CODE,
       		        CURRENCY_CONVERSION_TYPE,
       		        CURRENCY_CONVERSION_RATE,
       		        CURRENCY_CONVERSION_DATE,
       		        INSPECTION_STATUS_CODE,
       		        INSPECTION_QUALITY_CODE,
       		        FROM_ORGANIZATION_CODE,
       		        FROM_SUBINVENTORY,
       		        FROM_LOCATOR,
       		        FREIGHT_CARRIER_NAME,
       		        BILL_OF_LADING,
       		        PACKING_SLIP,
       		        SHIPPED_DATE,
       		        NUM_OF_CONTAINERS,
       		        WAYBILL_AIRBILL_NUM,
       		        RMA_REFERENCE,
       		        COMMENTS,
       		        TRUCK_NUM,
       		        CONTAINER_NUM,
       		        SUBSTITUTE_ITEM_NUM,
       		        NOTICE_UNIT_PRICE,
       		        ITEM_CATEGORY,
       		        INTRANSIT_OWNING_ORG_CODE,
       		        ROUTING_CODE,
       		        BARCODE_LABEL,
       		        COUNTRY_OF_ORIGIN_CODE,
       		        CREATE_DEBIT_MEMO_FLAG,
       		        LICENSE_PLATE_NUMBER,
       		        TRANSFER_LICENSE_PLATE_NUMBER,
       		        LPN_GROUP_NUM,
       		        ASN_LINE_NUM,
       		        EMPLOYEE_NAME,
       		        SOURCE_TRANSACTION_NUM,
       		        PARENT_SOURCE_TRANSACTION_NUM,
       		        PARENT_INTERFACE_TXN_ID,
       		        MATCHING_BASIS,
       		        RA_OUTSOURCER_PARTY_NAME,
       		        RA_DOCUMENT_NUMBER,
       		        RA_DOCUMENT_LINE_NUMBER,
       		        RA_NOTE_TO_RECEIVER,
       		        RA_VENDOR_SITE_NAME,
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
       		        ATTRIBUTE_DATE1,
       		        ATTRIBUTE_DATE2,
       		        ATTRIBUTE_DATE3,
       		        ATTRIBUTE_DATE4,
       		        ATTRIBUTE_DATE5,
       		        ATTRIBUTE_TIMESTAMP1,
       		        ATTRIBUTE_TIMESTAMP2,
       		        ATTRIBUTE_TIMESTAMP3,
       		        ATTRIBUTE_TIMESTAMP4,
       		        ATTRIBUTE_TIMESTAMP5,
       		        CONSIGNED_FLAG,
       		        SOLDTO_LEGAL_ENTITY,
       		        CONSUMED_QUANTITY,
       		        DEFAULT_TAXATION_COUNTRY,
       		        TRX_BUSINESS_CATEGORY,
       		        DOCUMENT_FISCAL_CLASSIFICATION,
       		        USER_DEFINED_FISC_CLASS,
       		        PRODUCT_FISC_CLASS_NAME,
       		        INTENDED_USE,
       		        PRODUCT_CATEGORY,
       		        TAX_CLASSIFICATION_CODE,
       		        PRODUCT_TYPE,
       		        FIRST_PTY_NUMBER,
       		        THIRD_PTY_NUMBER,
       		        TAX_INVOICE_NUMBER,
       		        TAX_INVOICE_DATE,
       		        FINAL_DISCHARGE_LOC_CODE,
       		        ASSESSABLE_VALUE,
       		        PHYSICAL_RETURN_REQD,
       		        EXTERNAL_SYSTEM_PACKING_UNIT,
       		        EWAY_BILL_NUMBER,
       		        EWAY_BILL_DATE,
       		        RECALL_NOTICE_NUMBER,
       		        RECALL_LINE_NUMBER,
       		        EXTERNAL_SYS_TXN_REFERENCE,
       		        DEFAULT_LOTSER_FROM_ASN,					
					FILE_NAME 						,
					ERROR_MESSAGE 					,
					IMPORT_STATUS  					,
					EXECUTION_ID  					,
					FILE_REFERENCE_IDENTIFIER 		,
					SOURCE_SYSTEM   				,
					Batch_ID
                                    FROM XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG   
                                      where (import_status = '''
                                 || 'ERROR'
                                 || '''
									  or BILLED_FLAG IS NULL)
									and execution_id  =  '''
                                 || gv_execution_id
                                 || ''''
                    );

                    dbms_output.put_line('CSV file for XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG for batch_id '
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
                                       || gv_oci_file_name_potrans,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG for batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);
               -- RETURN;
                END;

            END LOOP;

        END;

    END create_recon_report_prc;

END xxcnv_po_c009_po_receipts_conversion_pkg;