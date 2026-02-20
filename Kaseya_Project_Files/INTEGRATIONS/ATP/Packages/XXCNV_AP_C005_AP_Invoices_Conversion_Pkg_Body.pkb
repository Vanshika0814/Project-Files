create or replace PACKAGE BODY       XXCNV.XXCNV_AP_C005_AP_INVOICES_CONVERSION_PKG 
 IS
	/*************************************************************************************
    NAME              :     AP_INVOICES_CONVERSION_Package BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -----------------------------------------
	-- Bhargavi.K	  24-Oct-2025  	    1.0         Initial Development
        -- Bhargavi.K     26-Jul-2025       1.1         Removed XXCNV. at line 3420  
        -- Bhargavi.K     29-Jul-2025       1.2         Added changes for JIRA ID-6261   
	****************************************************************************************/

---Declaring global Variables

      gv_import_status	    			    VARCHAR2(256)   := NULL;
      gv_error_message 	    			    VARCHAR2(500)   := NULL;
      gv_file_name            			VARCHAR2(256)   := NULL;
	  gv_oci_file_name                      VARCHAR2(4000)  := NULL; 
	  gv_oci_file_path                      VARCHAR2(200)   := NULL;
	  gv_oci_file_name_apinv             VARCHAR2(2000)    := NULL;
      gv_oci_file_name_apinvlines          VARCHAR2(2000)  := NULL;
	  gv_execution_id                       VARCHAR2(100)    := NULL;
	  gv_book_type_code                     VARCHAR2(50)    := NULL;
      gv_interface_line_number              VARCHAR2(50)    := NULL;
	 -- gv_group_id                         NUMBER(18)      := NULL;
	  gv_batch_id                           VARCHAR2(200)   := replace (to_char (sysdate,'yyyymmdd hhmmss'), ' ' );
	  gv_credential_name        CONSTANT 	VARCHAR2(30)	:= 'OCI$RESOURCE_PRINCIPAL';                
	  gv_status_success         CONSTANT    VARCHAR2(100)    := 'Success';
	  gv_status_failure         CONSTANT    VARCHAR2(100)    := 'Failure';
	  gv_conversion_id                      VARCHAR2(100)    := NULL;
	  gv_boundary_system	      	        VARCHAR2(100)	:=  NULL;
     gv_coa_transformation 	CONSTANT 	VARCHAR2(50)	:= 'COA_TRANSFORMATION';
    gv_coa_transformation_failed 	CONSTANT 	VARCHAR2(50)	:= 'COA_TRANSFORMATION_FAILED';  
      gv_status_picked		    CONSTANT 	VARCHAR2(100)	:= 'File_Picked_From_OCI_And_Loaded_To_Stg';
	  gv_status_picked_for_tr	CONSTANT 	VARCHAR2(100)	:= 'Transformed_Data_From_Ext_To_Stg' ;
	  gv_status_validated		CONSTANT 	VARCHAR2(100)	:= 'Validated';
	  gv_status_failed   	    CONSTANT 	VARCHAR2(100)	:= 'Failed_At_Validation';	 
	  gv_fbdi_export_status 	CONSTANT 	VARCHAR2(100)	:= 'Exported_To_Fbdi';
	  gv_fbdi_export_status_fail CONSTANT   VARCHAR2(100)	:= 'Exported_To_Fbdi_Failed';
	  gv_status_staged		    CONSTANT 	VARCHAR2(100)	:= 'Staged_For_Import';	
	  gv_transformed_folder 	CONSTANT 	VARCHAR2(100)	:= 'Transformed_FBDI_Files' ;
	  gv_source_folder          CONSTANT    VARCHAR2(100)    := 'Source_FBDI_Files';
	  gv_properties       	    CONSTANT 	VARCHAR2(100)	:= 'properties' ;
	  gv_file_picked      	        	    VARCHAR2(100)	:= 'File_Picked_From_OCI_Server' ;
      gv_file_not_found       CONSTANT    VARCHAR2(100)    := 'File_not_found';
      gv_recon_folder         CONSTANT    VARCHAR2(50)    := 'ATP_Validation_Error_Files';
	  gv_recon_report         CONSTANT    VARCHAR2(50)    := 'Recon_Report_Created';

/*===========================================================================================================
-- PROCEDURE : MAIN_PRC
-- PARAMETERS:
-- COMMENT   : This procedure is used to call all the procedures under a single procedure
==============================================================================================================*/
PROCEDURE MAIN_PRC ( p_RICE_ID 	            IN  		VARCHAR2,
                 p_execution_id 		IN  	    VARCHAR2,
                 p_boundary_system      IN  		VARCHAR2,
			     p_file_name 		    IN  		VARCHAR2) AS
p_loading_status VARCHAR2(30) := NULL;
lv_start_pos NUMBER := 1;
lv_end_pos NUMBER;
lv_file_name VARCHAR2(4000);

    BEGIN 
	dbms_output.put_line('----------------------MAIN_PRC started-----------------');
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
            AND ce1.STATUS = gv_file_picked)
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
                WHEN lv_file_name LIKE '%ApInvoicesInterface%.csv' THEN gv_oci_file_name_apinv := lv_file_name;
                WHEN lv_file_name LIKE '%ApInvoiceLinesInterface%.csv' THEN gv_oci_file_name_apinvlines := lv_file_name;


			ELSE
							dbms_output.put_line('No match found for file name: ' || lv_file_name); -- Debugging output
            END CASE;

            lv_start_pos := lv_end_pos + 1;
        END LOOP;

        -- Output the results for debugging
        dbms_output.put_line('lv_File Name: ' || lv_file_name);
        dbms_output.put_line('AP Invoice File Name: ' || gv_oci_file_name_apinv);
        dbms_output.put_line('AP Invoice Lines File Name: ' || gv_oci_file_name_apinvlines);

EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('Error fetching execution details: ' || SQLERRM);
		--RETURN;
END;	

   -- Call to import data from OCI to Stage table
    BEGIN
		dbms_output.put_line('----------------------IMPORT_DATA_FROM_OCI_TO_STG_PRC started-----------------');
        IMPORT_DATA_FROM_OCI_TO_STG_PRC(p_loading_status);
        IF p_loading_status = gv_status_failure THEN
            dbms_output.put_line('Error in IMPORT_DATA_FROM_OCI_TO_STG_PRC');
            RETURN;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling IMPORT_DATA_FROM_OCI_TO_STG_PRC: ' || SQLERRM);
            RETURN;
		dbms_output.put_line('----------------------IMPORT_DATA_FROM_OCI_TO_STG_PRC ended-----------------');
    END;

    -- Call to perform data and business validations in staging table
    BEGIN
		dbms_output.put_line('----------------------DATA_VALIDATIONS_PRC started-----------------');
        data_validations_prc;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling data_validations_prc: ' || SQLERRM);
             RETURN;
		dbms_output.put_line('----------------------DATA_VALIDATIONS_PRC ended-----------------');

    END;

    -- Call to create a CSV file from xxcnv_ap_c005_ap_invoices_stg after all validations
    BEGIN
		dbms_output.put_line('----------------------CREATE_FBDI_FILE_PRC started-----------------');

        create_fbdi_file_prc;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling create_fbdi_file_prc: ' || SQLERRM);
            RETURN;
		dbms_output.put_line('----------------------CREATE_FBDI_FILE_PRC ended-----------------');
    END;

        ---create a atp recon report
	 BEGIN
		dbms_output.put_line('----------------------CREATE_ATP_VALIDATION_RECON_REPORT_PRC started-----------------');
    create_atp_validation_recon_report_prc;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling create_atp_validation_recon_report_prc: ' ||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
            RETURN;
		dbms_output.put_line('----------------------CREATE_ATP_VALIDATION_RECON_REPORT_PRC ended-----------------');
    END; 

	dbms_output.put_line('----------------------MAIN_PRC ended-----------------');

 END MAIN_PRC;

/*=================================================================================================================
-- PROCEDURE : IMPORT_DATA_FROM_OCI_TO_STG_PRC
-- PARAMETERS: p_loading_status
-- COMMENT   : This procedure is used to create an external table and transfer that data from external to stg table.
===================================================================================================================*/

 PROCEDURE IMPORT_DATA_FROM_OCI_TO_STG_PRC (p_loading_status    OUT VARCHAR2) IS

    lv_table_count NUMBER := 0;
    lv_row_count   NUMBER := 0;

BEGIN	

BEGIN
    BEGIN
        lv_table_count := 0;
       -- Check if the external table exists and drop it if it does
        SELECT COUNT(*)
        INTO lv_table_count
        FROM all_objects
        WHERE UPPER(object_name) = 'XXCNV_AP_C005_AP_INVOICES_EXT'
        AND object_type = 'TABLE';

        IF lv_table_count > 0 THEN
            EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c005_ap_invoices_ext';
			 EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c005_ap_invoices_stg';
            dbms_output.put_line('Table xxcnv_ap_c005_ap_invoices_ext dropped');
        END IF;
		EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error dropping table xxcnv_ap_c005_ap_invoices_ext: ' ||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
            p_loading_status := gv_status_failure;

    END;



    BEGIN
	    lv_table_count := 0;	     
	    SELECT COUNT(*)
        INTO lv_table_count
        FROM all_objects
        WHERE UPPER(object_name) = 'XXCNV_AP_C005_AP_INVOICES_LINES_EXT'
        AND object_type = 'TABLE';

        IF lv_table_count > 0 THEN
            EXECUTE IMMEDIATE 'DROP TABLE xxcnv_ap_c005_ap_invoices_lines_ext';
					 EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_ap_c005_ap_invoice_lines_stg';
            dbms_output.put_line('Table xxcnv_ap_c005_ap_invoices_lines_ext dropped');
        END IF;
		EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error dropping table xxcnv_ap_c005_ap_invoices_lines_ext: ' ||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
            p_loading_status := gv_status_failure;
    END;
END;	


-- Create the external table
	BEGIN

        IF gv_oci_file_name_apinv LIKE '%ApInvoicesInterface.csv%' THEN

			dbms_output.put_line('Creating external table xxcnv_ap_c005_ap_invoices_ext');
            dbms_output.put_line(' xxcnv_ap_c005_ap_invoices_ext : '|| gv_oci_file_path||'/'||gv_oci_file_name_apinv);

			DBMS_CLOUD.CREATE_EXTERNAL_TABLE(

		   table_name => 'xxcnv_ap_c005_ap_invoices_ext',
           credential_name => gv_credential_name,
		   file_uri_list   =>  gv_oci_file_path||'/'||gv_oci_file_name_apinv,

           format => json_object('skipheaders' VALUE '1','type' value 'csv', 'dateformat' value 'yyyy/mm/dd','rejectlimit' value 'UNLIMITED','ignoremissingcolumns' value 'true','blankasnull' value 'true','conversionerrors' VALUE 'store_null' ),
           column_list => 
	             	'   INVOICE_ID						NUMBER(15),
						OPERATING_UNIT 	 				VARCHAR2(240), 
						SOURCE							VARCHAR2(80),
						INVOICE_NUM						VARCHAR2(50),
						INVOICE_AMOUNT					NUMBER,
						INVOICE_DATE					DATE,
						VENDOR_NAME       				VARCHAR2(240),	
						VENDOR_NUM   					VARCHAR2(30),
						VENDOR_SITE_CODE				VARCHAR2(240),
						INVOICE_CURRENCY_CODE 			VARCHAR2(15),
						PAYMENT_CURRENCY_CODE  			VARCHAR2(15),
						DESCRIPTION 					VARCHAR2(500) ,
						GROUP_ID						VARCHAR2(80) ,
						INVOICE_TYPE_LOOKUP_CODE 		VARCHAR2(25),
						LEGAL_ENTITY_NAME				VARCHAR2(50),
						CUST_REGISTRATION_NUMBER 		VARCHAR2(30),
						CUST_REGISTRATION_CODE			VARCHAR2(30),
						FIRST_PARTY_REGISTRATION_NUM	VARCHAR2(60),
						THIRD_PARTY_REGISTRATION_NUM	VARCHAR2(60),
						TERMS_NAME						VARCHAR2(50),
						TERMS_DATE						DATE,
						GOODS_RECEIVED_DATE 			DATE,
						INVOICE_RECEIVED_DATE 			DATE,
						GL_DATE 						DATE,
						PAYMENT_METHOD_CODE   			VARCHAR2(30),
						PAY_GROUP_LOOKUP_CODE 			VARCHAR2(25),
						EXCLUSIVE_PAYMENT_FLAG 			VARCHAR2(1),
						AMOUNT_APPLICABLE_TO_DISCOUNT	NUMBER,
						PREPAY_NUM						VARCHAR2(50),
						PREPAY_LINE_NUM					NUMBER,
						PREPAY_APPLY_AMOUNT				NUMBER,
						PREPAY_GL_DATE					DATE,
						INVOICE_INCLUDES_PREPAY_FLAG 	VARCHAR2(1),
						EXCHANGE_RATE_TYPE				VARCHAR2(30),
						EXCHANGE_DATE 					DATE,
						EXCHANGE_RATE					NUMBER,

						ACCTS_PAY_CODE_CONCATENATED		VARCHAR2(250),
						DOC_CATEGORY_CODE				VARCHAR2(30),
						VOUCHER_NUM						VARCHAR2(50),
						REQUESTER_FIRST_NAME			VARCHAR2(150),
						REQUESTER_LAST_NAME				VARCHAR2(150),
						REQUESTER_EMPLOYEE_NUM			VARCHAR2(30),
						DELIVERY_CHANNEL_CODE			VARCHAR2(30),
						BANK_CHARGE_BEARER				VARCHAR2(30),
						REMIT_TO_SUPPLIER_NAME			VARCHAR2(240),
						REMIT_TO_SUPPLIER_NUM			VARCHAR2(30),
						REMIT_TO_ADDRESS_NAME			VARCHAR2(240),
						PAYMENT_PRIORITY				NUMBER(2),
						SETTLEMENT_PRIORITY				VARCHAR2(30),
						UNIQUE_REMITTANCE_IDENTIFIER	VARCHAR2(256),	
						URI_CHECK_DIGIT                     VARCHAR2(2),     
						PAYMENT_REASON_CODE				VARCHAR2(30),
						PAYMENT_REASON_COMMENTS			VARCHAR2(240),
						REMITTANCE_MESSAGE_1			VARCHAR2(150),
						REMITTANCE_MESSAGE_2			VARCHAR2(150),
						REMITTANCE_MESSAGE_3			VARCHAR2(150),
						AWT_GROUP_NAME					VARCHAR2(25),
						SHIP_TO_LOCATION				VARCHAR2(40),
						TAXATION_COUNTRY				VARCHAR2(30),
						DOCUMENT_SUB_TYPE				VARCHAR2(150),
						TAX_INVOICE_INTERNAL_SEQ		VARCHAR2(150),
						SUPPLIER_TAX_INVOICE_NUMBER		VARCHAR2(150),
						TAX_INVOICE_RECORDING_DATE		DATE,
						SUPPLIER_TAX_INVOICE_DATE		DATE,
						SUPPLIER_TAX_EXCHANGE_RATE		NUMBER,
						PORT_OF_ENTRY_CODE				VARCHAR2(30),
						CORRECTION_YEAR					NUMBER,
						CORRECTION_PERIOD				VARCHAR2(15),
						IMPORT_DOCUMENT_NUMBER			VARCHAR2(50),
						IMPORT_DOCUMENT_DATE			DATE,
						CONTROL_AMOUNT					NUMBER,
						CALC_TAX_DURING_IMPORT_FLAG		VARCHAR2(1),
						ADD_TAX_TO_INV_AMT_FLAG			VARCHAR2(1),
						ATTRIBUTE_CATEGORY				VARCHAR2(150),
						ATTRIBUTE1						VARCHAR2(150),
						ATTRIBUTE2						VARCHAR2(150),
						ATTRIBUTE3						VARCHAR2(150),
						ATTRIBUTE4						VARCHAR2(150),
						ATTRIBUTE5						VARCHAR2(150),
						ATTRIBUTE6						VARCHAR2(150),
						ATTRIBUTE7						VARCHAR2(150),
						ATTRIBUTE8						VARCHAR2(150),
						ATTRIBUTE9						VARCHAR2(150),
						ATTRIBUTE10						VARCHAR2(150),
						ATTRIBUTE11						VARCHAR2(150),
						ATTRIBUTE12						VARCHAR2(150),
						ATTRIBUTE13						VARCHAR2(150),
						ATTRIBUTE14						VARCHAR2(150),
						ATTRIBUTE15						VARCHAR2(1000),
						ATTRIBUTE_NUMBER1				NUMBER,
						ATTRIBUTE_NUMBER2				NUMBER,
						ATTRIBUTE_NUMBER3				NUMBER,
						ATTRIBUTE_NUMBER4				NUMBER,
						ATTRIBUTE_NUMBER5				NUMBER,
						ATTRIBUTE_DATE1					DATE,
						ATTRIBUTE_DATE2					DATE,
						ATTRIBUTE_DATE3					DATE,
						ATTRIBUTE_DATE4					DATE,
						ATTRIBUTE_DATE5					DATE,
						GLOBAL_ATTRIBUTE_CATEGORY		VARCHAR2(150),
						GLOBAL_ATTRIBUTE1				VARCHAR2(150),
						GLOBAL_ATTRIBUTE2				VARCHAR2(150),
						GLOBAL_ATTRIBUTE3				VARCHAR2(150),
						GLOBAL_ATTRIBUTE4				VARCHAR2(150),
						GLOBAL_ATTRIBUTE5				VARCHAR2(150),
						GLOBAL_ATTRIBUTE6				VARCHAR2(150),
						GLOBAL_ATTRIBUTE7				VARCHAR2(150),
						GLOBAL_ATTRIBUTE8				VARCHAR2(150),
						GLOBAL_ATTRIBUTE9				VARCHAR2(150),
						GLOBAL_ATTRIBUTE10				VARCHAR2(150),
						GLOBAL_ATTRIBUTE11				VARCHAR2(150),
						GLOBAL_ATTRIBUTE12				VARCHAR2(150),
						GLOBAL_ATTRIBUTE13				VARCHAR2(150),
						GLOBAL_ATTRIBUTE14				VARCHAR2(150),
						GLOBAL_ATTRIBUTE15				VARCHAR2(150),
						GLOBAL_ATTRIBUTE16				VARCHAR2(150),
						GLOBAL_ATTRIBUTE17				VARCHAR2(150),
						GLOBAL_ATTRIBUTE18				VARCHAR2(150),
						GLOBAL_ATTRIBUTE19				VARCHAR2(150),
						GLOBAL_ATTRIBUTE20				VARCHAR2(150),
						GLOBAL_ATTRIBUTE_NUMBER1		NUMBER,
						GLOBAL_ATTRIBUTE_NUMBER2		NUMBER,
						GLOBAL_ATTRIBUTE_NUMBER3		NUMBER,
						GLOBAL_ATTRIBUTE_NUMBER4		NUMBER,
						GLOBAL_ATTRIBUTE_NUMBER5		NUMBER,
						GLOBAL_ATTRIBUTE_DATE1			DATE,
						GLOBAL_ATTRIBUTE_DATE2			DATE,
						GLOBAL_ATTRIBUTE_DATE3			DATE,
						GLOBAL_ATTRIBUTE_DATE4			DATE,
						GLOBAL_ATTRIBUTE_DATE5			DATE,
						IMAGE_DOCUMENT_URI				VARCHAR2(4000)'
                        );
			dbms_output.put_line('External table is created');
			EXECUTE IMMEDIATE  'INSERT INTO xxcnv_ap_c005_ap_invoices_stg (

									INVOICE_ID						,
									OPERATING_UNIT 	 				, 
									SOURCE							,
									INVOICE_NUM						,
									INVOICE_AMOUNT					,
									INVOICE_DATE					,
									VENDOR_NAME       				,	
									VENDOR_NUM   					,
									VENDOR_SITE_CODE				,
									INVOICE_CURRENCY_CODE 			,
									PAYMENT_CURRENCY_CODE  			,
									DESCRIPTION 					,
									GROUP_ID						,
									INVOICE_TYPE_LOOKUP_CODE 		,
									LEGAL_ENTITY_NAME				,
									CUST_REGISTRATION_NUMBER 		,
									CUST_REGISTRATION_CODE			,
									FIRST_PARTY_REGISTRATION_NUM	,
									THIRD_PARTY_REGISTRATION_NUM	,
									TERMS_NAME						,
									TERMS_DATE						,
									GOODS_RECEIVED_DATE 				,
									INVOICE_RECEIVED_DATE 			,
									GL_DATE						,
									PAYMENT_METHOD_CODE   			,
									PAY_GROUP_LOOKUP_CODE 			,
									EXCLUSIVE_PAYMENT_FLAG 			,
									AMOUNT_APPLICABLE_TO_DISCOUNT	,
									PREPAY_NUM						,
									PREPAY_LINE_NUM					,
									PREPAY_APPLY_AMOUNT				,
									PREPAY_GL_DATE						,
									INVOICE_INCLUDES_PREPAY_FLAG 	,
									EXCHANGE_RATE_TYPE				,
									EXCHANGE_DATE 					,
									EXCHANGE_RATE					,

									ACCTS_PAY_CODE_CONCATENATED		,
									DOC_CATEGORY_CODE				,
									VOUCHER_NUM						,
									REQUESTER_FIRST_NAME			,
									REQUESTER_LAST_NAME				,
									REQUESTER_EMPLOYEE_NUM			,
									DELIVERY_CHANNEL_CODE			,
									BANK_CHARGE_BEARER				,
									REMIT_TO_SUPPLIER_NAME			,
									REMIT_TO_SUPPLIER_NUM			,
									REMIT_TO_ADDRESS_NAME			,
									PAYMENT_PRIORITY				,
									SETTLEMENT_PRIORITY				,
									UNIQUE_REMITTANCE_IDENTIFIER	,	
									URI_CHECK_DIGIT                     ,     
									PAYMENT_REASON_CODE				,
									PAYMENT_REASON_COMMENTS			,
									REMITTANCE_MESSAGE_1			,
									REMITTANCE_MESSAGE_2			,
									REMITTANCE_MESSAGE_3			,
									AWT_GROUP_NAME					,
									SHIP_TO_LOCATION				,
									TAXATION_COUNTRY				,
									DOCUMENT_SUB_TYPE				,
									TAX_INVOICE_INTERNAL_SEQ		,
									SUPPLIER_TAX_INVOICE_NUMBER		,
									TAX_INVOICE_RECORDING_DATE		,
									SUPPLIER_TAX_INVOICE_DATE		,
									SUPPLIER_TAX_EXCHANGE_RATE		,
									PORT_OF_ENTRY_CODE				,
									CORRECTION_YEAR					,
									CORRECTION_PERIOD				,
									IMPORT_DOCUMENT_NUMBER			,
									IMPORT_DOCUMENT_DATE			,
									CONTROL_AMOUNT					,
									CALC_TAX_DURING_IMPORT_FLAG		,
									ADD_TAX_TO_INV_AMT_FLAG			,
									ATTRIBUTE_CATEGORY				,
									ATTRIBUTE1						,
									ATTRIBUTE2						,
									ATTRIBUTE3						,
									ATTRIBUTE4						,
									ATTRIBUTE5						,
									ATTRIBUTE6						,
									ATTRIBUTE7						,
									ATTRIBUTE8						,
									ATTRIBUTE9						,
									ATTRIBUTE10						,
									ATTRIBUTE11						,
									ATTRIBUTE12						,
									ATTRIBUTE13						,
									ATTRIBUTE14						,
									ATTRIBUTE15						,
									ATTRIBUTE_NUMBER1				,
									ATTRIBUTE_NUMBER2				,
									ATTRIBUTE_NUMBER3				,
									ATTRIBUTE_NUMBER4				,
									ATTRIBUTE_NUMBER5				,
									ATTRIBUTE_DATE1					,
									ATTRIBUTE_DATE2					,
									ATTRIBUTE_DATE3					,
									ATTRIBUTE_DATE4					,
									ATTRIBUTE_DATE5					,
									GLOBAL_ATTRIBUTE_CATEGORY		,
									GLOBAL_ATTRIBUTE1				,
									GLOBAL_ATTRIBUTE2				,
									GLOBAL_ATTRIBUTE3				,
									GLOBAL_ATTRIBUTE4				,
									GLOBAL_ATTRIBUTE5				,
									GLOBAL_ATTRIBUTE6				,
									GLOBAL_ATTRIBUTE7				,
									GLOBAL_ATTRIBUTE8				,
									GLOBAL_ATTRIBUTE9				,
									GLOBAL_ATTRIBUTE10				,
									GLOBAL_ATTRIBUTE11				,
									GLOBAL_ATTRIBUTE12				,
									GLOBAL_ATTRIBUTE13				,
									GLOBAL_ATTRIBUTE14				,
									GLOBAL_ATTRIBUTE15				,
									GLOBAL_ATTRIBUTE16				,
									GLOBAL_ATTRIBUTE17				,
									GLOBAL_ATTRIBUTE18				,
									GLOBAL_ATTRIBUTE19				,
									GLOBAL_ATTRIBUTE20				,
									GLOBAL_ATTRIBUTE_NUMBER1		,
									GLOBAL_ATTRIBUTE_NUMBER2		,
									GLOBAL_ATTRIBUTE_NUMBER3		,
									GLOBAL_ATTRIBUTE_NUMBER4		,
									GLOBAL_ATTRIBUTE_NUMBER5		,
									GLOBAL_ATTRIBUTE_DATE1			,
									GLOBAL_ATTRIBUTE_DATE2			,
									GLOBAL_ATTRIBUTE_DATE3			,
									GLOBAL_ATTRIBUTE_DATE4			,
									GLOBAL_ATTRIBUTE_DATE5			,
									IMAGE_DOCUMENT_URI				,
									Target_VENDOR_NUM               ,
					                Target_VENDOR_SITE_CODE         ,
									TARGET_OPERATING_UNIT  ,
									TARGET_LEGAL_ENTITY_NAME,
									FILE_NAME 						,
									ERROR_MESSAGE 					,
									IMPORT_STATUS  					,
									EXECUTION_ID  					,
									FILE_REFERENCE_IDENTIFIER		,
									SOURCE_SYSTEM   				,
									BATCH_ID
									) 
									SELECT 
									INVOICE_ID						,
									OPERATING_UNIT 	 				, 
									SOURCE							,
									INVOICE_NUM						,
									INVOICE_AMOUNT					,
									INVOICE_DATE					,
									VENDOR_NAME       				,	
									VENDOR_NUM   					,
									VENDOR_SITE_CODE				,
									INVOICE_CURRENCY_CODE 			,
									PAYMENT_CURRENCY_CODE  			,
									DESCRIPTION 					,
									GROUP_ID						,
									INVOICE_TYPE_LOOKUP_CODE 		,
									LEGAL_ENTITY_NAME				,
									CUST_REGISTRATION_NUMBER 		,
									CUST_REGISTRATION_CODE			,
									FIRST_PARTY_REGISTRATION_NUM	,
									THIRD_PARTY_REGISTRATION_NUM	,
									TERMS_NAME						,
									TERMS_DATE						,
									GOODS_RECEIVED_DATE 				,
									INVOICE_RECEIVED_DATE 			,
									GL_DATE							,
									PAYMENT_METHOD_CODE   			,
									PAY_GROUP_LOOKUP_CODE 			,
									EXCLUSIVE_PAYMENT_FLAG 			,
									AMOUNT_APPLICABLE_TO_DISCOUNT	,
									PREPAY_NUM						,
									PREPAY_LINE_NUM					,
									PREPAY_APPLY_AMOUNT				,
									PREPAY_GL_DATE						,
									INVOICE_INCLUDES_PREPAY_FLAG 	,
									EXCHANGE_RATE_TYPE ,
									EXCHANGE_DATE 					,
									EXCHANGE_RATE					,
									ACCTS_PAY_CODE_CONCATENATED		,
									DOC_CATEGORY_CODE				,
									VOUCHER_NUM						,
									REQUESTER_FIRST_NAME			,
									REQUESTER_LAST_NAME				,
									REQUESTER_EMPLOYEE_NUM			,
									DELIVERY_CHANNEL_CODE			,
									BANK_CHARGE_BEARER				,
									REMIT_TO_SUPPLIER_NAME			,
									REMIT_TO_SUPPLIER_NUM			,
									REMIT_TO_ADDRESS_NAME			,
									PAYMENT_PRIORITY				,
									SETTLEMENT_PRIORITY				,
									UNIQUE_REMITTANCE_IDENTIFIER	,
                                    URI_CHECK_DIGIT                 ,     									
									PAYMENT_REASON_CODE				,
									PAYMENT_REASON_COMMENTS			,
									REMITTANCE_MESSAGE_1			,
									REMITTANCE_MESSAGE_2			,
									REMITTANCE_MESSAGE_3			,
									AWT_GROUP_NAME					,
									SHIP_TO_LOCATION				,
									TAXATION_COUNTRY				,
									DOCUMENT_SUB_TYPE				,
									TAX_INVOICE_INTERNAL_SEQ		,
									SUPPLIER_TAX_INVOICE_NUMBER		,
									TAX_INVOICE_RECORDING_DATE		,
									SUPPLIER_TAX_INVOICE_DATE		,
									SUPPLIER_TAX_EXCHANGE_RATE		,
									PORT_OF_ENTRY_CODE				,
									CORRECTION_YEAR					,
									CORRECTION_PERIOD				,
									IMPORT_DOCUMENT_NUMBER			,
									IMPORT_DOCUMENT_DATE			,
									CONTROL_AMOUNT					,
									CALC_TAX_DURING_IMPORT_FLAG		,
									ADD_TAX_TO_INV_AMT_FLAG			,
									ATTRIBUTE_CATEGORY				,
									ATTRIBUTE1						,
									ATTRIBUTE2						,
									ATTRIBUTE3						,
									ATTRIBUTE4						,
									ATTRIBUTE5						,
									ATTRIBUTE6						,
									ATTRIBUTE7						,
									ATTRIBUTE8						,
									ATTRIBUTE9						,
									ATTRIBUTE10						,
									ATTRIBUTE11						,
									ATTRIBUTE12						,
									ATTRIBUTE13						,
									ATTRIBUTE14						,
									ATTRIBUTE15						,
									ATTRIBUTE_NUMBER1				,
									ATTRIBUTE_NUMBER2				,
									ATTRIBUTE_NUMBER3				,
									ATTRIBUTE_NUMBER4				,
									ATTRIBUTE_NUMBER5				,
									ATTRIBUTE_DATE1					,
									ATTRIBUTE_DATE2					,
									ATTRIBUTE_DATE3					,
									ATTRIBUTE_DATE4					,
									ATTRIBUTE_DATE5					,
									GLOBAL_ATTRIBUTE_CATEGORY		,
									GLOBAL_ATTRIBUTE1				,
									GLOBAL_ATTRIBUTE2				,
									GLOBAL_ATTRIBUTE3				,
									GLOBAL_ATTRIBUTE4				,
									GLOBAL_ATTRIBUTE5				,
									GLOBAL_ATTRIBUTE6				,
									GLOBAL_ATTRIBUTE7				,
									GLOBAL_ATTRIBUTE8				,
									GLOBAL_ATTRIBUTE9				,
									GLOBAL_ATTRIBUTE10				,
									GLOBAL_ATTRIBUTE11				,
									GLOBAL_ATTRIBUTE12				,
									GLOBAL_ATTRIBUTE13				,
									GLOBAL_ATTRIBUTE14				,
									GLOBAL_ATTRIBUTE15				,
									GLOBAL_ATTRIBUTE16				,
									GLOBAL_ATTRIBUTE17				,
									GLOBAL_ATTRIBUTE18				,
									GLOBAL_ATTRIBUTE19				,
									GLOBAL_ATTRIBUTE20				,
									GLOBAL_ATTRIBUTE_NUMBER1		,
									GLOBAL_ATTRIBUTE_NUMBER2		,
									GLOBAL_ATTRIBUTE_NUMBER3		,
									GLOBAL_ATTRIBUTE_NUMBER4		,
									GLOBAL_ATTRIBUTE_NUMBER5		,
									GLOBAL_ATTRIBUTE_DATE1			,
									GLOBAL_ATTRIBUTE_DATE2			,
									GLOBAL_ATTRIBUTE_DATE3			,
									GLOBAL_ATTRIBUTE_DATE4			,
									GLOBAL_ATTRIBUTE_DATE5			,
									IMAGE_DOCUMENT_URI				,
									NULL,
									NULL,
									NULL,
									NULL,
									null
									,null
									,null
									,'||CHR(39)||gv_execution_id||CHR(39)||'
									,null	
									,null
									,'|| gv_batch_id ||'								

									FROM xxcnv_ap_c005_ap_invoices_ext ';

            p_loading_status := gv_status_success;
			dbms_output.put_line('Inserted Records in the xxcnv_ap_c005_ap_invoices_stg: ' || SQL%ROWCOUNT);
			--commit;






	   END IF;				


---TABLE2
BEGIN
			IF gv_oci_file_name_apinvlines LIKE '%ApInvoiceLinesInterface.csv%' THEN

            dbms_output.put_line('Creating external table xxcnv_ap_c005_ap_invoice_lines_ext');
            dbms_output.put_line(' xxcnv_ap_c005_ap_invoices_lines_ext : '|| gv_oci_file_path||'/'||gv_oci_file_name_apinvlines);

        -- Create the external table

			DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
                table_name => 'xxcnv_ap_c005_ap_invoices_lines_ext',
                credential_name => gv_credential_name,
				file_uri_list   =>  gv_oci_file_path||'/'||gv_oci_file_name_apinvlines,
                format => json_object('skipheaders' VALUE '1','type' VALUE 'csv', 'rejectlimit' VALUE 'UNLIMITED', 'dateformat' VALUE 'yyyy/mm/dd','ignoremissingcolumns' value 'true','blankasnull' value 'true','conversionerrors' VALUE 'store_null'), 

                column_list => 
				   'INVOICE_ID						NUMBER(15),
					LINE_NUMBER						NUMBER(18),			
					LINE_TYPE_LOOKUP_CODE			VARCHAR2(25),
					AMOUNT							NUMBER,
					QUANTITY_INVOICED				NUMBER,
					UNIT_PRICE						NUMBER,
					UNIT_OF_MEAS_LOOKUP_CODE		VARCHAR2(25),
					DESCRIPTION						VARCHAR2(500),  
					PO_NUMBER						NUMBER,
					PO_LINE_NUMBER					NUMBER,	
					PO_SHIPMENT_NUM					NUMBER,
					PO_DISTRIBUTION_NUM				NUMBER,
					ITEM_DESCRIPTION				VARCHAR2(240), 
					RELEASE_NUM						NUMBER,			
					PURCHASING_CATEGORY				VARCHAR2(2000),
					RECEIPT_NUMBER					VARCHAR2(30),
					RECEIPT_LINE_NUMBER				VARCHAR2(25),
					CONSUMPTION_ADVICE_NUMBER		VARCHAR2(20),
					CONSUMPTION_ADVICE_LINE_NUMBER	NUMBER,
					PACKAGING_SLIP					VARCHAR2(25),		
					FINAL_MATCH_FLAG				VARCHAR2(1),	
					DIST_CODE_CONCATENATED			VARCHAR2(250),
					DISTRIBUTION_SET_NAME			VARCHAR2(50),
					ACCOUNTING_DATE					DATE,
					ACCOUNT_SEGMENT					VARCHAR2(25),
					BALANCING_SEGMENT				VARCHAR2(25),
					COST_CENTER_SEGMENT				VARCHAR2(25),
					TAX_CLASSIFICATION_CODE			VARCHAR2(30),
					SHIP_TO_LOCATION_CODE			VARCHAR2(60),
					SHIP_FROM_LOCATION_CODE			VARCHAR2(60),
					FINAL_DISCHARGE_LOCATION_CODE	VARCHAR2(60),	
					TRX_BUSINESS_CATEGORY			VARCHAR2(240),
					PRODUCT_FISC_CLASSIFICATION		VARCHAR2(240),
					PRIMARY_INTENDED_USE			VARCHAR2(30),
					USER_DEFINED_FISC_CLASS			VARCHAR2(240),
					PRODUCT_TYPE					VARCHAR2(240),
					ASSESSABLE_VALUE				NUMBER,
					PRODUCT_CATEGORY				VARCHAR2(240),
					CONTROL_AMOUNT					NUMBER,
					TAX_REGIME_CODE					VARCHAR2(30),
					TAX								VARCHAR2(30),
					TAX_STATUS_CODE					VARCHAR2(30),
					TAX_JURISDICTION_CODE			VARCHAR2(30),
					TAX_RATE_CODE					VARCHAR2(150),
					TAX_RATE						NUMBER,
					AWT_GROUP_NAME					VARCHAR2(25),
					TYPE_1099						VARCHAR2(10),
					INCOME_TAX_REGION				VARCHAR2(10),
					PRORATE_ACROSS_FLAG				VARCHAR2(1),
					LINE_GROUP_NUMBER				NUMBER,
					COST_FACTOR_NAME				VARCHAR2(80),
					STAT_AMOUNT						NUMBER,
					ASSETS_TRACKING_FLAG			VARCHAR2(1),
					ASSET_BOOK_TYPE_CODE			VARCHAR2(30),
					ASSET_CATEGORY_ID				NUMBER(18),
					SERIAL_NUMBER					VARCHAR2(35),
					MANUFACTURER					VARCHAR2(30),
					MODEL_NUMBER					VARCHAR2(40),
					WARRANTY_NUMBER					VARCHAR2(15),
					PRICE_CORRECTION_FLAG			VARCHAR2(1),
					PRICE_CORRECTION_INV_NUM		VARCHAR2(50),
					PRICE_CORRECTION_INV_LINE_NUM	NUMBER,
					REQUESTER_FIRST_NAME			VARCHAR2(150),
					REQUESTER_LAST_NAME				VARCHAR2(150),
					REQUESTER_EMPLOYEE_NUM			VARCHAR2(30),
					ATTRIBUTE_CATEGORY				VARCHAR2(150),
					ATTRIBUTE1						VARCHAR2(150),
					ATTRIBUTE2						VARCHAR2(150),
					ATTRIBUTE3						VARCHAR2(150),
					ATTRIBUTE4						VARCHAR2(150),
					ATTRIBUTE5						VARCHAR2(150),
					ATTRIBUTE6						VARCHAR2(150),
					ATTRIBUTE7						VARCHAR2(150),
					ATTRIBUTE8						VARCHAR2(150),
					ATTRIBUTE9						VARCHAR2(150),
					ATTRIBUTE10						VARCHAR2(150),
					ATTRIBUTE11						VARCHAR2(150),
					ATTRIBUTE12						VARCHAR2(150),
					ATTRIBUTE13						VARCHAR2(150),
					ATTRIBUTE14						VARCHAR2(150),
					ATTRIBUTE15						VARCHAR2(150),
					ATTRIBUTE_NUMBER1				NUMBER,
					ATTRIBUTE_NUMBER2				NUMBER,
					ATTRIBUTE_NUMBER3				NUMBER,
					ATTRIBUTE_NUMBER4				NUMBER,
					ATTRIBUTE_NUMBER5				NUMBER,
					ATTRIBUTE_DATE1					DATE,
					ATTRIBUTE_DATE2					DATE,
					ATTRIBUTE_DATE3					DATE,
					ATTRIBUTE_DATE4					DATE,
					ATTRIBUTE_DATE5					DATE,
					GLOBAL_ATTRIBUTE_CATEGORY		VARCHAR2(150),
					GLOBAL_ATTRIBUTE1				VARCHAR2(150),
					GLOBAL_ATTRIBUTE2				VARCHAR2(150),
					GLOBAL_ATTRIBUTE3				VARCHAR2(150),
					GLOBAL_ATTRIBUTE4				VARCHAR2(150),
					GLOBAL_ATTRIBUTE5				VARCHAR2(150),
					GLOBAL_ATTRIBUTE6				VARCHAR2(150),
					GLOBAL_ATTRIBUTE7				VARCHAR2(150),
					GLOBAL_ATTRIBUTE8				VARCHAR2(150),
					GLOBAL_ATTRIBUTE9				VARCHAR2(150),
					GLOBAL_ATTRIBUTE10				VARCHAR2(150),
					GLOBAL_ATTRIBUTE11				VARCHAR2(150),
					GLOBAL_ATTRIBUTE12				VARCHAR2(150),
					GLOBAL_ATTRIBUTE13				VARCHAR2(150),
					GLOBAL_ATTRIBUTE14				VARCHAR2(150),
					GLOBAL_ATTRIBUTE15				VARCHAR2(150),
					GLOBAL_ATTRIBUTE16				VARCHAR2(150),
					GLOBAL_ATTRIBUTE17				VARCHAR2(150),
					GLOBAL_ATTRIBUTE18				VARCHAR2(150),
					GLOBAL_ATTRIBUTE19				VARCHAR2(150),
					GLOBAL_ATTRIBUTE20				VARCHAR2(150),
					GLOBAL_ATTRIBUTE_NUMBER1		NUMBER,
					GLOBAL_ATTRIBUTE_NUMBER2		NUMBER,
					GLOBAL_ATTRIBUTE_NUMBER3		NUMBER,
					GLOBAL_ATTRIBUTE_NUMBER4		NUMBER,
					GLOBAL_ATTRIBUTE_NUMBER5		NUMBER,
					GLOBAL_ATTRIBUTE_DATE1			DATE,
					GLOBAL_ATTRIBUTE_DATE2			DATE,
					GLOBAL_ATTRIBUTE_DATE3			DATE,
					GLOBAL_ATTRIBUTE_DATE4			DATE,
					GLOBAL_ATTRIBUTE_DATE5			DATE,
					PJC_PROJECT_ID					NUMBER(18),
					PJC_TASK_ID						NUMBER(18),
					PJC_EXPENDITURE_TYPE_ID			NUMBER(18),
					PJC_EXPENDITURE_ITEM_DATE		DATE,
					PJC_ORGANIZATION_ID				NUMBER(18),
					PJC_PROJECT_NUMBER				VARCHAR2(25),
					PJC_TASK_NUMBER					VARCHAR2(100),
					PJC_EXPENDITURE_TYPE_NAME		VARCHAR2(240),
					PJC_ORGANIZATION_NAME			VARCHAR2(240),
					PJC_RESERVED_ATTRIBUTE1			VARCHAR2(150),	
					PJC_RESERVED_ATTRIBUTE2			VARCHAR2(150),
					PJC_RESERVED_ATTRIBUTE3			VARCHAR2(150),
					PJC_RESERVED_ATTRIBUTE4			VARCHAR2(150),
					PJC_RESERVED_ATTRIBUTE5			VARCHAR2(150),
					PJC_RESERVED_ATTRIBUTE6			VARCHAR2(150),
					PJC_RESERVED_ATTRIBUTE7			VARCHAR2(150),
					PJC_RESERVED_ATTRIBUTE8			VARCHAR2(150),
					PJC_RESERVED_ATTRIBUTE9			VARCHAR2(150),
					PJC_RESERVED_ATTRIBUTE10		VARCHAR2(150),
					PJC_USER_DEF_ATTRIBUTE1			VARCHAR2(150),
					PJC_USER_DEF_ATTRIBUTE2			VARCHAR2(150),
					PJC_USER_DEF_ATTRIBUTE3			VARCHAR2(150),
					PJC_USER_DEF_ATTRIBUTE4			VARCHAR2(150),
					PJC_USER_DEF_ATTRIBUTE5			VARCHAR2(150),
					PJC_USER_DEF_ATTRIBUTE6			VARCHAR2(150),
					PJC_USER_DEF_ATTRIBUTE7			VARCHAR2(150),
					PJC_USER_DEF_ATTRIBUTE8			VARCHAR2(150),
					PJC_USER_DEF_ATTRIBUTE9			VARCHAR2(150),
					PJC_USER_DEF_ATTRIBUTE10		VARCHAR2(150),
					FISCAL_CHARGE_TYPE				VARCHAR2(30),
					DEF_ACCTG_START_DATE			DATE,
					DEF_ACCTG_END_DATE				DATE,
					DEF_ACCRUAL_CODE_CONCATENATED	VARCHAR2(800),
					PJC_PROJECT_NAME				VARCHAR2(240),
					PJC_TASK_NAME					VARCHAR2(255),
					PJC_WORK_TYPE					VARCHAR2(240),
					PJC_CONTRACT_NAME				VARCHAR2(300),
					PJC_CONTRACT_NUMBER				VARCHAR2(120),	
					PJC_FUNDING_SOURCE_NAME			VARCHAR2(360),
					PJC_FUNDING_SOURCE_NUMBER		VARCHAR2(50),
					REQUESTER_EMAIL_ADDRESS			VARCHAR2(240),
					RCV_TRANSACTION_ID       NUMBER'
					);

			EXECUTE IMMEDIATE  'INSERT INTO xxcnv_ap_c005_ap_invoice_lines_stg (
					INVOICE_ID						,
					LINE_NUMBER						,			
					LINE_TYPE_LOOKUP_CODE			,
					AMOUNT							,
					QUANTITY_INVOICED				,
					UNIT_PRICE						,
					UNIT_OF_MEAS_LOOKUP_CODE		,
					DESCRIPTION						,  
					PO_NUMBER						,
					PO_LINE_NUMBER						,	
					PO_SHIPMENT_NUM					,
					PO_DISTRIBUTION_NUM				,
					ITEM_DESCRIPTION				, 
					RELEASE_NUM						,			
					PURCHASING_CATEGORY				,
					RECEIPT_NUMBER					,
					RECEIPT_LINE_NUMBER				,
					CONSUMPTION_ADVICE_NUMBER		,
					CONSUMPTION_ADVICE_LINE_NUMBER	,
					PACKAGING_SLIP					,		
					FINAL_MATCH_FLAG				,	
					DIST_CODE_CONCATENATED			,
					DISTRIBUTION_SET_NAME			,
					ACCOUNTING_DATE					,
					ACCOUNT_SEGMENT					,
					BALANCING_SEGMENT				,
					COST_CENTER_SEGMENT				,
					TAX_CLASSIFICATION_CODE			,
					SHIP_TO_LOCATION_CODE			,
					SHIP_FROM_LOCATION_CODE			,
					FINAL_DISCHARGE_LOCATION_CODE	,	
					TRX_BUSINESS_CATEGORY			,
					PRODUCT_FISC_CLASSIFICATION		,
					PRIMARY_INTENDED_USE			,
					USER_DEFINED_FISC_CLASS			,
					PRODUCT_TYPE					,
					ASSESSABLE_VALUE				,
					PRODUCT_CATEGORY				,
					CONTROL_AMOUNT					,
					TAX_REGIME_CODE					,
					TAX								,
					TAX_STATUS_CODE					,
					TAX_JURISDICTION_CODE			,
					TAX_RATE_CODE					,
					TAX_RATE						,
					AWT_GROUP_NAME					,
					TYPE_1099						,
					INCOME_TAX_REGION				,
					PRORATE_ACROSS_FLAG				,
					LINE_GROUP_NUMBER				,
					COST_FACTOR_NAME				,
					STAT_AMOUNT						,
					ASSETS_TRACKING_FLAG			,
					ASSET_BOOK_TYPE_CODE			,
					ASSET_CATEGORY_ID				,
					SERIAL_NUMBER					,
					MANUFACTURER					,
					MODEL_NUMBER					,
					WARRANTY_NUMBER					,
					PRICE_CORRECTION_FLAG			,
					PRICE_CORRECTION_INV_NUM		,
					PRICE_CORRECTION_INV_LINE_NUM	,
					REQUESTER_FIRST_NAME			,
					REQUESTER_LAST_NAME				,
					REQUESTER_EMPLOYEE_NUM			,
					ATTRIBUTE_CATEGORY				,
					ATTRIBUTE1						,
					ATTRIBUTE2						,
					ATTRIBUTE3						,
					ATTRIBUTE4						,
					ATTRIBUTE5						,
					ATTRIBUTE6						,
					ATTRIBUTE7						,
					ATTRIBUTE8						,
					ATTRIBUTE9						,
					ATTRIBUTE10						,
					ATTRIBUTE11						,
					ATTRIBUTE12						,
					ATTRIBUTE13						,
					ATTRIBUTE14						,
					ATTRIBUTE15						,
					ATTRIBUTE_NUMBER1				,
					ATTRIBUTE_NUMBER2				,
					ATTRIBUTE_NUMBER3				,
					ATTRIBUTE_NUMBER4				,
					ATTRIBUTE_NUMBER5				,
					ATTRIBUTE_DATE1					,
					ATTRIBUTE_DATE2					,
					ATTRIBUTE_DATE3					,
					ATTRIBUTE_DATE4					,
					ATTRIBUTE_DATE5					,
					GLOBAL_ATTRIBUTE_CATEGORY		,
					GLOBAL_ATTRIBUTE1				,
					GLOBAL_ATTRIBUTE2				,
					GLOBAL_ATTRIBUTE3				,
					GLOBAL_ATTRIBUTE4				,
					GLOBAL_ATTRIBUTE5				,
					GLOBAL_ATTRIBUTE6				,
					GLOBAL_ATTRIBUTE7				,
					GLOBAL_ATTRIBUTE8				,
					GLOBAL_ATTRIBUTE9				,
					GLOBAL_ATTRIBUTE10				,
					GLOBAL_ATTRIBUTE11				,
					GLOBAL_ATTRIBUTE12				,
					GLOBAL_ATTRIBUTE13				,
					GLOBAL_ATTRIBUTE14				,
					GLOBAL_ATTRIBUTE15				,
					GLOBAL_ATTRIBUTE16				,
					GLOBAL_ATTRIBUTE17				,
					GLOBAL_ATTRIBUTE18				,
					GLOBAL_ATTRIBUTE19				,
					GLOBAL_ATTRIBUTE20				,
					GLOBAL_ATTRIBUTE_NUMBER1		,
					GLOBAL_ATTRIBUTE_NUMBER2		,
					GLOBAL_ATTRIBUTE_NUMBER3		,
					GLOBAL_ATTRIBUTE_NUMBER4		,
					GLOBAL_ATTRIBUTE_NUMBER5		,
					GLOBAL_ATTRIBUTE_DATE1			,
					GLOBAL_ATTRIBUTE_DATE2			,
					GLOBAL_ATTRIBUTE_DATE3			,
					GLOBAL_ATTRIBUTE_DATE4			,
					GLOBAL_ATTRIBUTE_DATE5			,
					PJC_PROJECT_ID					,
					PJC_TASK_ID						,
					PJC_EXPENDITURE_TYPE_ID			,
					PJC_EXPENDITURE_ITEM_DATE		,
					PJC_ORGANIZATION_ID				,
					PJC_PROJECT_NUMBER				,
					PJC_TASK_NUMBER					,
					PJC_EXPENDITURE_TYPE_NAME		,
					PJC_ORGANIZATION_NAME			,
					PJC_RESERVED_ATTRIBUTE1			,	
					PJC_RESERVED_ATTRIBUTE2			,
					PJC_RESERVED_ATTRIBUTE3			,
					PJC_RESERVED_ATTRIBUTE4			,
					PJC_RESERVED_ATTRIBUTE5			,
					PJC_RESERVED_ATTRIBUTE6			,
					PJC_RESERVED_ATTRIBUTE7			,
					PJC_RESERVED_ATTRIBUTE8			,
					PJC_RESERVED_ATTRIBUTE9			,
					PJC_RESERVED_ATTRIBUTE10		,
					PJC_USER_DEF_ATTRIBUTE1			,
					PJC_USER_DEF_ATTRIBUTE2			,
					PJC_USER_DEF_ATTRIBUTE3			,
					PJC_USER_DEF_ATTRIBUTE4			,
					PJC_USER_DEF_ATTRIBUTE5			,
					PJC_USER_DEF_ATTRIBUTE6			,
					PJC_USER_DEF_ATTRIBUTE7			,
					PJC_USER_DEF_ATTRIBUTE8			,
					PJC_USER_DEF_ATTRIBUTE9			,
					PJC_USER_DEF_ATTRIBUTE10		,
					FISCAL_CHARGE_TYPE				,
					DEF_ACCTG_START_DATE			,
					DEF_ACCTG_END_DATE				,
					DEF_ACCRUAL_CODE_CONCATENATED	,
					PJC_PROJECT_NAME				,
					PJC_TASK_NAME					,
                    PJC_WORK_TYPE					,
					PJC_CONTRACT_NAME				,
					PJC_CONTRACT_NUMBER				,	
					PJC_FUNDING_SOURCE_NAME			,
					PJC_FUNDING_SOURCE_NUMBER		,
					REQUESTER_EMAIL_ADDRESS			,
                    RCV_TRANSACTION_ID              ,
					FILE_NAME 						,
					ERROR_MESSAGE 					,
					IMPORT_STATUS  					,
					EXECUTION_ID  					,
					FILE_REFERENCE_IDENTIFIER 		,
					SOURCE_SYSTEM   				,
					Batch_ID
					)
					SELECT 
					INVOICE_ID						,
					LINE_NUMBER						,			
					LINE_TYPE_LOOKUP_CODE			,
					AMOUNT							,
					QUANTITY_INVOICED				,
					UNIT_PRICE						,
					UNIT_OF_MEAS_LOOKUP_CODE		,
					DESCRIPTION						,  
					PO_NUMBER						,
					PO_LINE_NUMBER					,	
					PO_SHIPMENT_NUM					,
					PO_DISTRIBUTION_NUM				,
					ITEM_DESCRIPTION				, 
					RELEASE_NUM						,			
					PURCHASING_CATEGORY				,
					RECEIPT_NUMBER					,
					RECEIPT_LINE_NUMBER				,
					CONSUMPTION_ADVICE_NUMBER		,
					CONSUMPTION_ADVICE_LINE_NUMBER	,
					PACKAGING_SLIP					,		
					FINAL_MATCH_FLAG				,	
					DIST_CODE_CONCATENATED			,
					DISTRIBUTION_SET_NAME			,
					ACCOUNTING_DATE					,
					ACCOUNT_SEGMENT					,
					BALANCING_SEGMENT				,
					COST_CENTER_SEGMENT				,
					TAX_CLASSIFICATION_CODE			,
					SHIP_TO_LOCATION_CODE			,
					SHIP_FROM_LOCATION_CODE			,
					FINAL_DISCHARGE_LOCATION_CODE	,	
					TRX_BUSINESS_CATEGORY			,
					PRODUCT_FISC_CLASSIFICATION		,
					PRIMARY_INTENDED_USE			,
					USER_DEFINED_FISC_CLASS			,
					PRODUCT_TYPE					,
					ASSESSABLE_VALUE				,
					PRODUCT_CATEGORY				,
					CONTROL_AMOUNT					,
					TAX_REGIME_CODE					,
					TAX								,
					TAX_STATUS_CODE					,
					TAX_JURISDICTION_CODE			,
					TAX_RATE_CODE					,
					TAX_RATE						,
					AWT_GROUP_NAME					,
					TYPE_1099						,
					INCOME_TAX_REGION				,
					PRORATE_ACROSS_FLAG				,
					LINE_GROUP_NUMBER				,
					COST_FACTOR_NAME				,
					STAT_AMOUNT						,
					ASSETS_TRACKING_FLAG			,
					ASSET_BOOK_TYPE_CODE			,
					ASSET_CATEGORY_ID				,
					SERIAL_NUMBER					,
					MANUFACTURER					,
					MODEL_NUMBER					,
					WARRANTY_NUMBER					,
					PRICE_CORRECTION_FLAG			,
					PRICE_CORRECTION_INV_NUM		,
					PRICE_CORRECTION_INV_LINE_NUM	,
					REQUESTER_FIRST_NAME			,
					REQUESTER_LAST_NAME				,
					REQUESTER_EMPLOYEE_NUM			,
					ATTRIBUTE_CATEGORY				,
					ATTRIBUTE1						,
					ATTRIBUTE2						,
					ATTRIBUTE3						,
					ATTRIBUTE4						,
					ATTRIBUTE5						,
					ATTRIBUTE6						,
					ATTRIBUTE7						,
					ATTRIBUTE8						,
					ATTRIBUTE9						,
					ATTRIBUTE10						,
					ATTRIBUTE11						,
					ATTRIBUTE12						,
					ATTRIBUTE13						,
					ATTRIBUTE14						,
					ATTRIBUTE15						,
					ATTRIBUTE_NUMBER1				,
					ATTRIBUTE_NUMBER2				,
					ATTRIBUTE_NUMBER3				,
					ATTRIBUTE_NUMBER4				,
					ATTRIBUTE_NUMBER5				,
					ATTRIBUTE_DATE1					,
					ATTRIBUTE_DATE2					,
					ATTRIBUTE_DATE3					,
					ATTRIBUTE_DATE4					,
					ATTRIBUTE_DATE5					,
					GLOBAL_ATTRIBUTE_CATEGORY		,
					GLOBAL_ATTRIBUTE1				,
					GLOBAL_ATTRIBUTE2				,
					GLOBAL_ATTRIBUTE3				,
					GLOBAL_ATTRIBUTE4				,
					GLOBAL_ATTRIBUTE5				,
					GLOBAL_ATTRIBUTE6				,
					GLOBAL_ATTRIBUTE7				,
					GLOBAL_ATTRIBUTE8				,
					GLOBAL_ATTRIBUTE9				,
					GLOBAL_ATTRIBUTE10				,
					GLOBAL_ATTRIBUTE11				,
					GLOBAL_ATTRIBUTE12				,
					GLOBAL_ATTRIBUTE13				,
					GLOBAL_ATTRIBUTE14				,
					GLOBAL_ATTRIBUTE15				,
					GLOBAL_ATTRIBUTE16				,
					GLOBAL_ATTRIBUTE17				,
					GLOBAL_ATTRIBUTE18				,
					GLOBAL_ATTRIBUTE19				,
					GLOBAL_ATTRIBUTE20				,
					GLOBAL_ATTRIBUTE_NUMBER1		,
					GLOBAL_ATTRIBUTE_NUMBER2		,
					GLOBAL_ATTRIBUTE_NUMBER3		,
					GLOBAL_ATTRIBUTE_NUMBER4		,
					GLOBAL_ATTRIBUTE_NUMBER5		,
					GLOBAL_ATTRIBUTE_DATE1			,
					GLOBAL_ATTRIBUTE_DATE2			,
					GLOBAL_ATTRIBUTE_DATE3			,
					GLOBAL_ATTRIBUTE_DATE4			,
					GLOBAL_ATTRIBUTE_DATE5			,
					PJC_PROJECT_ID					,
					PJC_TASK_ID						,
					PJC_EXPENDITURE_TYPE_ID			,
					PJC_EXPENDITURE_ITEM_DATE		,
					PJC_ORGANIZATION_ID				,
					PJC_PROJECT_NUMBER				,
					PJC_TASK_NUMBER					,
					PJC_EXPENDITURE_TYPE_NAME		,
					PJC_ORGANIZATION_NAME			,
					PJC_RESERVED_ATTRIBUTE1			,	
					PJC_RESERVED_ATTRIBUTE2			,
					PJC_RESERVED_ATTRIBUTE3			,
					PJC_RESERVED_ATTRIBUTE4			,
					PJC_RESERVED_ATTRIBUTE5			,
					PJC_RESERVED_ATTRIBUTE6			,
					PJC_RESERVED_ATTRIBUTE7			,
					PJC_RESERVED_ATTRIBUTE8			,
					PJC_RESERVED_ATTRIBUTE9			,
					PJC_RESERVED_ATTRIBUTE10		,
					PJC_USER_DEF_ATTRIBUTE1			,
					PJC_USER_DEF_ATTRIBUTE2			,
					PJC_USER_DEF_ATTRIBUTE3			,
					PJC_USER_DEF_ATTRIBUTE4			,
					PJC_USER_DEF_ATTRIBUTE5			,
					PJC_USER_DEF_ATTRIBUTE6			,
					PJC_USER_DEF_ATTRIBUTE7			,
					PJC_USER_DEF_ATTRIBUTE8			,
					PJC_USER_DEF_ATTRIBUTE9			,
					PJC_USER_DEF_ATTRIBUTE10		,
					FISCAL_CHARGE_TYPE				,
					DEF_ACCTG_START_DATE			,
					DEF_ACCTG_END_DATE				,
					DEF_ACCRUAL_CODE_CONCATENATED	,
					PJC_PROJECT_NAME				,
					PJC_TASK_NAME					,
					PJC_WORK_TYPE					,
					PJC_CONTRACT_NAME				,
					PJC_CONTRACT_NUMBER				,	
					PJC_FUNDING_SOURCE_NAME			,
					PJC_FUNDING_SOURCE_NUMBER		,
					REQUESTER_EMAIL_ADDRESS			,
                    RCV_TRANSACTION_ID              ,
					null,
					null,
					null,
					'||CHR(39)||gv_execution_id||CHR(39)||',
					NULL,
                    NULL,
					'|| gv_batch_id ||'



					FROM xxcnv_ap_c005_ap_invoices_lines_ext';

			                   p_loading_status := gv_status_success;					
				               dbms_output.put_line('Inserted records in xxcnv_ap_c005_ap_invoice_lines_stg: '||SQL%ROWCOUNT);



 END IF;

 EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error creating external table: ' || SQLERRM);
            p_loading_status := gv_status_failure;
            RETURN;
    END;


	   -- Count the number of rows in the external table
	    BEGIN
        IF gv_oci_file_name_apinv LIKE '%ApInvoicesInterface%' THEN
            SELECT COUNT(*)
            INTO lv_row_count
            FROM xxcnv_ap_c005_ap_invoices_stg;
            dbms_output.put_line('Inserted Records in the xxcnv_ap_c005_ap_invoices_stg from OCI Source Folder: ' || lv_row_count);
		END IF;	

		IF gv_oci_file_name_apinvlines LIKE '%ApInvoiceLinesInterface%' THEN
            SELECT COUNT(*)
            INTO lv_row_count
            FROM xxcnv_ap_c005_ap_invoice_lines_stg;
            dbms_output.put_line('Inserted Records in the xxcnv_ap_c005_ap_invoice_lines_stg from OCI Source Folder: ' || lv_row_count);
        END IF;	

    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error counting rows in the external table: ' || SQLERRM);
            p_loading_status := gv_status_failure;
            RETURN;
    END;


        -- Count the number of rows in the external table

   BEGIN
        SELECT COUNT(*)
        INTO lv_row_count
        FROM xxcnv_ap_c005_ap_invoices_stg;

        dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c005_ap_invoices_stg from OCI Source Folder: ' || lv_row_count);

		 -- Use an implicit cursor in the FOR LOOP to iterate over distinct book_type_code
        FOR rec IN (SELECT DISTINCT batch_id FROM xxcnv_ap_c005_ap_invoices_stg where execution_id = gv_execution_id) LOOP
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id 		=> gv_conversion_id,
                   	p_execution_id		    => gv_execution_id,
                    p_execution_step		=> gv_status_picked,
                    p_boundary_system       => gv_boundary_system,
                    p_file_path 			=> gv_oci_file_path,
                    p_file_name				=> gv_oci_file_name_apinv,
					P_attribute1            => rec.batch_id,
                    P_attribute2            => lv_row_count,
                    p_process_reference     => NULL
                );
                END LOOP;
                  p_loading_status := gv_status_success;				 

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error counting rows in xxcnv_ap_c005_ap_invoices_stg: ' || SQLERRM);
                    p_loading_status := gv_status_failure;
                    RETURN;
            END;  

   BEGIN
        SELECT COUNT(*)
        INTO lv_row_count
        FROM xxcnv_ap_c005_ap_invoice_lines_stg;

        dbms_output.put_line('Log:Inserted Records in the xxcnv_ap_c005_ap_invoice_lines_stg from OCI Source Folder: ' || lv_row_count);

		 -- Use an implicit cursor in the FOR LOOP to iterate over distinct book_type_code
        FOR rec IN (SELECT DISTINCT batch_id FROM xxcnv_ap_c005_ap_invoice_lines_stg where execution_id = gv_execution_id) LOOP
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id 		=> gv_conversion_id,
                   	p_execution_id		    => gv_execution_id,
                    p_execution_step		=> gv_status_picked,
                    p_boundary_system       => gv_boundary_system,
                    p_file_path 			=> gv_oci_file_path,
                    p_file_name				=> gv_oci_file_name_apinvlines,
					P_attribute1            => rec.batch_id,
                    P_attribute2            => lv_row_count,
                    p_process_reference     => NULL
                );
                END LOOP;
                  p_loading_status := gv_status_success;				 

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error counting rows in xxcnv_ap_c005_ap_invoice_lines_stg: ' || SQLERRM);
                    p_loading_status := gv_status_failure;
                    RETURN;
            END;			

         END;
      END IMPORT_DATA_FROM_OCI_TO_STG_PRC;


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
  lv_error_count:=0;

     BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoices_stg
          SET execution_id = gv_execution_id
		  WHERE file_reference_identifier is null;
          END;

		SELECT COUNT(*) INTO lv_row_count 
		FROM xxcnv_ap_c005_ap_invoices_stg
		WHERE EXECUTION_ID = gv_execution_id ;

		IF lv_row_count <> 0 then 


       -- Initialize ERROR_MESSAGE to an empty string if it is NULL
		  BEGIN 
			UPDATE xxcnv_ap_c005_ap_invoices_stg
			SET ERROR_MESSAGE = ''
			WHERE error_message is null
            and EXECUTION_ID = gv_execution_id ;
 EXCEPTION
    WHEN OTHERS THEN 
      dbms_output.put_line('An error occurred while initializing ERROR_MESSAGE: ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);
 		  END;
    ---  validate INVOICE_ID
  BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|INVOICE_ID should not be NULL'
	where INVOICE_ID is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('INVOICE_ID is validated');
  END;


	  -- Validate Unique Invoice IDs in xxcnv_ap_c005_ap_invoices_stg

-- Step 1: Check for duplicate INVOICE_IDs in xxcnv_ap_c005_ap_invoices_stg
BEGIN

UPDATE xxcnv_ap_c005_ap_invoices_stg
SET ERROR_MESSAGE = ERROR_MESSAGE || '|Duplicate INVOICE_IDs found in xxcnv_ap_c005_ap_invoices_stg. '
WHERE INVOICE_ID IN (
    SELECT INVOICE_ID
    FROM xxcnv_ap_c005_ap_invoices_stg
    WHERE execution_id = gv_execution_id
    GROUP BY INVOICE_ID
    HAVING COUNT(*) > 1
)
AND execution_id = gv_execution_id;
end;
--FUT
-- Step 2: Check for missing INVOICE_IDs in xxcnv_ap_c005_ap_invoice_lines_stg
BEGIN
UPDATE xxcnv_ap_c005_ap_invoices_stg h
SET ERROR_MESSAGE = ERROR_MESSAGE || '|Some INVOICE_IDs in Header Table do not have corresponding entries in Lines Table. '
WHERE NOT EXISTS (
    SELECT 1
    FROM xxcnv_ap_c005_ap_invoice_lines_stg l
    WHERE l.INVOICE_ID = h.INVOICE_ID
    AND l.execution_id = h.execution_id
)
AND h.execution_id = gv_execution_id;

    dbms_output.put_line('Validation of unique INVOICE_IDs and consistency completed');
END;


--  validate Business_Unit
  BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|BUSINESS_UNIT should not be NULL'
	where OPERATING_UNIT is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('BUSINESS_UNIT is validated');
  END;
  --  validate Business_Unit
  BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET    TARGET_operating_unit = (SELECT oc_business_unit_name FROM xxcnv_gl_le_bu_mapping WHERE ns_legal_entity_name = operating_unit)
	WHERE operating_unit is NOT NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('BUSINESS_UNIT is updated');
  END;
 --  validate Business_Unit after transformation 
   BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '| Corresponding BUSINESS_UNIT not found'
	where TARGET_operating_unit is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('BUSINESS_UNIT is validated');
  END;

     BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoices_stg 
          SET OPERATING_UNIT= '"' ||(replace( OPERATING_UNIT,'"','') )|| '"' 
          WHERE OPERATING_UNIT LIKE '%,%'
           and execution_id = gv_execution_id 
           AND file_reference_identifier IS NULL;
                dbms_output.put_line('OPERATING_UNIT With Comma is validated');
            END;

   -- Updating Source 

	BEGIN
		UPDATE xxcnv_ap_c005_ap_invoices_stg
		SET source = 'NetSuite Conversion'
		,vendor_name = NULL
		,ATTRIBUTE_CATEGORY ='NETSUITE CONV'
		,invoice_type_lookup_code = 'STANDARD';
		dbms_output.put_line('Source and Invoice Lookup code is updated');
	END;

  --------Validate INVOICE_NUM-------
  BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|INVOICE_NUM should not be NULL'
	where INVOICE_NUM is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('INVOICE_NUM is validated');
  END;

   BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoices_stg 
          SET INVOICE_NUM= '"' ||(replace( INVOICE_NUM,'"','') )|| '"' 
          WHERE INVOICE_NUM LIKE '%,%'
           and execution_id = gv_execution_id 
           AND file_reference_identifier IS NULL;
                dbms_output.put_line('INVOICE_NUM With Comma is validated');
            END;

 ---Validate INVOICE_AMOUNT----
  BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|INVOICE_AMOUNT should not be NULL'
	where INVOICE_AMOUNT is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('INVOICE_AMOUNT is validated');
  END;

  -- Validate Invocie AMOUNT and Line Amount

BEGIN
 UPDATE xxcnv_ap_c005_ap_invoices_stg l
 SET ERROR_MESSAGE = ERROR_MESSAGE || '|Header AMOUNT must be equal to Sum of Line Amount'
 where l.INVOICE_AMOUNT != (select sum(h.amount) from  xxcnv_ap_c005_ap_invoice_lines_stg h where h.INVOICE_ID = L.INVOICE_ID
                          )
 and file_reference_identifier IS NULL;
 dbms_output.put_line('Header Amount and Line Amounts are validated');
 END;

 ---  Validate INVOICE_DATE
  BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|INVOICE_DATE should not be NULL'
	where INVOICE_DATE is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('INVOICE_DATE is validated');
  END;


 ---validate SUPPLIER_NUM
  BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|SUPPLIER_NUM should not be NULL'
	where vendor_num is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('SUPPLIER_NUM is validated');
  END;

 --Update Supplier Site
 BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg a
    SET    a.Target_VENDOR_SITE_CODE = (SELECT oc_vendor_site FROM xxcnv_ap_supplier_mapping WHERE ns_vendor_num = a.vendor_num and a.target_operating_unit = bill_to_bu_name )
	WHERE 1=1
	and a.VENDOR_SITE_CODE is NOT NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('Target VENDOR_SITE_CODE is updated');
  END;


--Validate Supplier Site with comma's

          BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoices_stg 
          SET Target_VENDOR_SITE_CODE='"' || (replace( Target_VENDOR_SITE_CODE,'"','') ) || '"' 
          WHERE Target_VENDOR_SITE_CODE LIKE '%,%'
           and execution_id = gv_execution_id 
           AND file_reference_identifier IS NULL;
                dbms_output.put_line('Target Supplier site With Comma is validated');
            END;

-- Validate Supplier Site for Null values
		BEGIN
              UPDATE xxcnv_ap_c005_ap_invoices_stg
              SET ERROR_MESSAGE = ERROR_MESSAGE || '|Correspondin Supplier Site Not Found In Oracle'
              WHERE UPPER(Target_VENDOR_SITE_CODE) IS NULL
			  AND file_reference_identifier IS NULL
              AND execution_id = gv_execution_id
			  ;
        END;

		  BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoices_stg 
          SET VENDOR_SITE_CODE='"' || (replace(VENDOR_SITE_CODE,'"','') ) || '"' 
          WHERE VENDOR_SITE_CODE LIKE '%,%'
           and execution_id = gv_execution_id 
           AND file_reference_identifier IS NULL;
                dbms_output.put_line('Supplier site With Comma is validated');
            END;



 --  validate INVOICE_CURRENCY
  BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|INVOICE_CURRENCY should not be NULL'
	where INVOICE_CURRENCY_CODE is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('INVOICE_CURRENCY is validated');
  END;

   BEGIN 
     UPDATE xxcnv_ap_c005_ap_invoices_stg 
     SET description = (replace( description,'"','') ) 
     WHERE 1=1 
    --  AND execution_id = gv_execution_id 
      AND file_reference_identifier IS NULL;
           dbms_output.put_line('Description With Comma is validated');
end;

  --Validate Description Length		
			  BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoices_stg  
          SET description = substr((replace( description,'"','') ),1,230)
          WHERE LENGTH(description) > 240
           and execution_id = gv_execution_id 
           AND file_reference_identifier IS NULL;
                dbms_output.put_line(' Header Description Length is validated');
            END;

  --Validate DESCRIPTION for comma separated values
  BEGIN 
     UPDATE xxcnv_ap_c005_ap_invoices_stg 
     SET description = '"' || (replace( description,'"','') ) || '"' 
     WHERE description LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
      AND file_reference_identifier IS NULL;
           dbms_output.put_line('Description With Comma is validated');
end;



BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|description should not be NULL'
	where description is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('description is validated');
  END;
 --- create Import Set-Group_id 
BEGIN

      UPDATE xxcnv_ap_c005_ap_invoices_stg
        SET group_id = 'AP_INVOICE_CONVERSION_'|| TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS')
        WHERE execution_id = gv_execution_id;
    DBMS_OUTPUT.PUT_LINE('Import Set column populated with AP_Invoice_Conversion concatenated with DateTime Stamp for all rows'); 
END;


 BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|Legal_entity_name should not be null'
	where legal_entity_name is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('legal_entity_name is validated');
  END;
-- Update Legal Entity 
BEGIN

      UPDATE xxcnv_ap_c005_ap_invoices_stg
      SET TARGET_legal_entity_name = (SELECT oc_legal_entity_name FROM xxcnv_gl_le_bu_mapping WHERE ns_legal_entity_name = legal_entity_name)
      WHERE 1=1
	  AND   file_reference_identifier is NULL;
    DBMS_OUTPUT.PUT_LINE('Legal Entity is updated'); 
END;

  BEGIN 
     UPDATE xxcnv_ap_c005_ap_invoices_stg 
     SET legal_entity_name = '"' || (replace( legal_entity_name,'"','') )  || '"' 
     WHERE legal_entity_name LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
      AND file_reference_identifier IS NULL;
           dbms_output.put_line('legal_entity_name With Comma is validated');
 END;

  BEGIN 
     UPDATE xxcnv_ap_c005_ap_invoices_stg 
     SET TARGET_legal_entity_name = '"' || (replace( TARGET_legal_entity_name,'"','') )  || '"' 
     WHERE TARGET_legal_entity_name LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
      AND file_reference_identifier IS NULL;
           dbms_output.put_line('legal_entity_name With Comma is validated');
 END;

 BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|Correspomding Legal_entity_name not found'
	where TARGET_legal_entity_name is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('legal_entity_name is validated');
  END;



  --  update PAYMENT_TERMS
  BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET terms_name = (SELECT oc_value FROM xxcnv_ap_payment_terms_mapping WHERE ns_value = terms_name)
	where terms_name IS NOT NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('PAYMENT_TERMS is updated');
  END;



   --VALIDATE ACCOUNTING_DATE
BEGIN

      UPDATE xxcnv_ap_c005_ap_invoices_stg
        SET gl_date = sysdate
		--SET GL_DATE = TRUNC(ADD_MONTHS(SYSDATE, 1), 'MM')
        WHERE execution_id = gv_execution_id;
    DBMS_OUTPUT.PUT_LINE('ACCOUNTING_DATE is updated to sysdate'); 
END;


--- VALIDATE PAYMENT METHOD ------
  BEGIN

      UPDATE xxcnv_ap_c005_ap_invoices_stg
        SET payment_method_code = NULL
        WHERE execution_id = gv_execution_id;
    DBMS_OUTPUT.PUT_LINE('Payment_Method is updated to NULL'); 
END;



-- Validate SHIP_TO_LOCATION

BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg
    SET    SHIP_TO_LOCATION = (SELECT OS_LOCATION_NAME FROM XXCNV_PO_SHIP_TO_LOCATION_MAPPING WHERE NS_LOCATION_NAME = SHIP_TO_LOCATION)
	WHERE 1=1
	and file_reference_identifier is NULL;
    dbms_output.put_line('SHIP_TO_LOCATION is updated');
  END;

   BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoices_stg  
          SET SHIP_TO_LOCATION ='"' || (replace( SHIP_TO_LOCATION,'"',null) ) || '"' 
          WHERE SHIP_TO_LOCATION LIKE '%,%'
           and execution_id = gv_execution_id 
           AND file_reference_identifier IS NULL;
                dbms_output.put_line(' SHIP_TO_LOCATION With Comma is validated');
            END;

--Supplier Num update
	BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg 
    SET Target_VENDOR_NUM = (SELECT oc_vendor_num FROM xxcnv_ap_supplier_mapping WHERE ns_vendor_num = vendor_num
	group by oc_vendor_num)
	where vendor_num is NOT NULL
	and file_reference_identifier is NUll;
    dbms_output.put_line('Target SUPPLIER_NUM is Updated');
  END;

  BEGIN
    UPDATE xxcnv_ap_c005_ap_invoices_stg
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|Corresponding Supplier not found in Oracle'
	where Target_VENDOR_NUM is NULL
	and file_reference_identifier is NULL;
    --dbms_output.put_line(' is validated');
  END;


--Supplier Num update
	BEGIN
    UPDATE xxcnv_AP_C040_ap_invoices_stg 
    SET Target_VENDOR_NUM = (SELECT oc_vendor_num FROM xxcnv_ap_supplier_mapping WHERE ns_vendor_num = vendor_num
	group by oc_vendor_num)
	where vendor_num is NOT NULL
	and file_reference_identifier is NUll;
    dbms_output.put_line('SUPPLIER_NUM is Updated');
  END;

  BEGIN
    UPDATE xxcnv_AP_C040_ap_invoices_stg
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|Corresponding Supplier not found in oracle'
	where Target_VENDOR_NUM is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('Target_VENDOR_NUM is validated');
  END;

  --CALLING COA Procedure
  BEGIN
  coa_target_segments_header_prc;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling COA Procedure in Header: ' || SQLERRM);
            -- RETURN;
    END;

	-- Liability Account Segment4 update		
 BEGIN 
	UPDATE xxcnv_ap_c005_ap_invoices_stg
	SET ACCTS_PAY_CODE_CONCATENATED = 	replace(accts_pay_code_concatenated,
(substr(accts_pay_code_concatenated,
instr(accts_pay_code_concatenated,'-',1,2)+1,
instr(accts_pay_code_concatenated,'-',1,4)
-instr(accts_pay_code_concatenated,'-',1,2)-1)),'99999-211001')
WHERE 1=1
     and execution_id = gv_execution_id 
           AND file_reference_identifier IS NULL;
                dbms_output.put_line('Liability Account Segment4 is validated');
            END;



   -- Update import_status based on error_message
		  BEGIN 
			UPDATE xxcnv_ap_c005_ap_invoices_stg
			SET import_status = CASE WHEN error_message IS NOT NULL THEN 'ERROR' ELSE 'PROCESSED' END;
			--WHERE execution_id = gv_execution_id;
			dbms_output.put_line('import_status is validated');
		  END;

     -- Final update to set error_message and import_status
      BEGIN      
	       UPDATE xxcnv_ap_c005_ap_invoices_stg
           SET  error_message = LTRIM(error_message, ','),  import_status = CASE 
	       WHEN error_message IS NOT NULL THEN 'ERROR' ELSE 'PROCESSED' END
		  where  execution_id = gv_execution_id 
		  AND file_reference_identifier IS NULL;
		   dbms_output.put_line('import_status column is updated');
           END;   

         BEGIN 
            UPDATE xxcnv_ap_c005_ap_invoices_stg
            SET SOURCE_SYSTEM = gv_boundary_system
			WHERE file_reference_identifier is null
             and execution_id = gv_execution_id ;
            dbms_output.put_line('source_system is updated');
          END;

		    BEGIN
			UPDATE  xxcnv_ap_c005_ap_invoices_stg
			SET FILE_NAME = gv_oci_file_name_apinv
			WHERE file_reference_identifier is null
			and execution_id = gv_execution_id ;
			dbms_output.put_line('file_name column is updated');
		  END;

           -- Check if there are any error messages
	  SELECT COUNT(*) INTO lv_error_count 
	  FROM xxcnv_ap_c005_ap_invoices_stg 
	  WHERE error_message is not null ;

	   UPDATE xxcnv_ap_c005_ap_invoices_stg
       SET file_reference_identifier = gv_execution_id || '_' || gv_status_failure
	   where error_message is NOT null
	   and file_reference_identifier is null
	   and execution_id = gv_execution_id ;
       dbms_output.put_line('file_reference_identifier column is updated');

		UPDATE xxcnv_ap_c005_ap_invoices_stg
		SET file_reference_identifier = gv_execution_id||'_'||gv_status_success
		where error_message is null
		and file_reference_identifier is null
		and execution_id = gv_execution_id;
		dbms_output.put_line('file_reference_identifier column is updated');

	  IF lv_error_count > 0 THEN
	    -- Logging the message If data is not validated
	  xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
					p_conversion_id 		=> gv_conversion_id,
					p_execution_id		    => gv_execution_id,
					p_execution_step 		=> gv_status_failed,
					p_boundary_system 		=> gv_boundary_system,
					p_file_path				=> gv_oci_file_path,
					p_file_name 			=> gv_oci_file_name_apinv,
					P_attribute1            => gv_batch_id,
					P_attribute2            => NULL,
					p_process_reference 	=> NULL
     	  );

	 END IF;

	  IF lv_error_count = 0 AND gv_oci_file_name_apinv is NOT NULL THEN
		xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
				p_conversion_id 		=> gv_conversion_id,
				p_execution_id		    => gv_execution_id,
				p_execution_step 		=> gv_status_validated,
				p_boundary_system 		=> gv_boundary_system,
				p_file_path				=> gv_oci_file_path,
				p_file_name 			=> gv_oci_file_name_apinv,
				P_attribute1            => gv_batch_id,
				P_attribute2            => NULL,
				p_process_reference 	=> NULL );

	 END IF;

	    IF gv_oci_file_name_apinv is null THEN
		xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
				p_conversion_id 		=> gv_conversion_id,
				p_execution_id		    => gv_execution_id,
				p_execution_step 		=> gv_file_not_found,
				p_boundary_system 		=> gv_boundary_system,
				p_file_path				=> gv_oci_file_path,
				p_file_name 			=> gv_oci_file_name_apinv,
				P_attribute1            => gv_batch_id,
				P_attribute2            => NULL,
				p_process_reference 	=> NULL );
	  END IF;
	   else 
	 dbms_output.put_line('No Data is found in interface tables. Data is not loaded from ext to stg ');

	 end if;

    END;

----FILE 2
BEGIN
lv_error_count := 0;

 BEGIN

UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
          SET execution_id = gv_execution_id
		  WHERE file_reference_identifier is null;

END;

 SELECT COUNT(*) INTO lv_row_count 
		FROM xxcnv_ap_c005_ap_invoice_lines_stg
		WHERE EXECUTION_ID = gv_execution_id ;

		IF lv_row_count <> 0 then 


        -- Initialize ERROR_MESSAGE to an empty string if it is NULL
            BEGIN
			UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
            SET ERROR_MESSAGE = ''
            WHERE ERROR_MESSAGE IS NULL;


			 EXCEPTION
    WHEN OTHERS THEN 
      dbms_output.put_line('An error occurred while initializing ERROR_MESSAGE: ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);

             END;

	 -- validation for INVOICE_ID 

	    ---  validate INVOICE_ID
  BEGIN
    UPDATE xxcnv_ap_c005_ap_invoice_lines_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|INVOICE_ID should not be NULL'
	where INVOICE_ID is NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('INVOICE_ID is validated');
  END;


 BEGIN	
    UPDATE xxcnv_ap_c005_ap_invoice_lines_stg 
    SET ERROR_MESSAGE = ERROR_MESSAGE || '|INVOICE_ID does not match with any existing invoice in the header'
    WHERE INVOICE_ID NOT IN (SELECT INVOICE_ID 
                             FROM xxcnv_ap_c005_ap_invoices_stg
							  where execution_id = gv_execution_id)
    AND file_reference_identifier IS NULL;
    dbms_output.put_line('INVOICE_ID is validated');
END;


 -- Validate LINE_TYPE_LOOKUP_CODE

  BEGIN
           UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
           SET ERROR_MESSAGE = ERROR_MESSAGE || '|line Type is not valid'
           WHERE LINE_TYPE_LOOKUP_CODE IS NOT NULL 	 
		   AND UPPER(LINE_TYPE_LOOKUP_CODE) NOT IN ('ASSEMBLY', 'INVTPART', 'NONINVTPART','TAXITEM', 'TAXGROUP')	   
           and execution_id = gv_execution_id ;
           dbms_output.put_line('Line Type is validated');
       END;
       BEGIN
           UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
           SET LINE_TYPE_LOOKUP_CODE = 'ITEM'
           WHERE (LINE_TYPE_LOOKUP_CODE IS NULL OR UPPER(LINE_TYPE_LOOKUP_CODE )IN ('ASSEMBLY', 'INVTPART', 'NONINVTPART'))		   
           and execution_id = gv_execution_id ;
           dbms_output.put_line('Line Type is Updated');
       END;

		BEGIN
		 UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
		 SET LINE_TYPE_LOOKUP_CODE ='TAX'
		 WHERE UPPER(LINE_TYPE_LOOKUP_CODE) IN ('TAXITEM', 'TAXGROUP')		   
				   and execution_id = gv_execution_id ;
				   dbms_output.put_line('Line Type is Updated');
	    END;

 BEGIN 
     UPDATE xxcnv_ap_c005_ap_invoice_lines_stg 
     SET description = (replace( description,'"',null) );
           dbms_output.put_line('Description With Comma is validated');
 END;	
	--Validate Description	Length		
			  BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoice_lines_stg  
          SET description = substr((replace( description,'"',null) ),1,230)
          WHERE LENGTH(description) > 240
           and execution_id = gv_execution_id 
           AND file_reference_identifier IS NULL;
                dbms_output.put_line(' Description With Comma is validated');
            END;

--Validate DESCRIPTION
  BEGIN 
     UPDATE xxcnv_ap_c005_ap_invoice_lines_stg 
     SET description = '"' ||(replace( description,'"',null) )|| '"' 
     WHERE description LIKE '%,%'  
    --  AND execution_id = gv_execution_id 
      AND file_reference_identifier IS NULL;
           dbms_output.put_line('Description With Comma is validated');
 END;	


  BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoice_lines_stg 
          SET ITEM_DESCRIPTION= (replace( ITEM_DESCRIPTION,'"','') );
                dbms_output.put_line('ITEM_DESCRIPTION With Comma is validated');
            END;
 -- VALIDATE ITEM_DESCRIPTION

   BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoice_lines_stg  
          SET ITEM_DESCRIPTION = substr((replace( ITEM_DESCRIPTION,'"','') ),1,230)
          WHERE LENGTH(ITEM_DESCRIPTION) > 240
           and execution_id = gv_execution_id 
           AND file_reference_identifier IS NULL;
                dbms_output.put_line(' ITEM_DESCRIPTION length is validated');
            END;

  BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoice_lines_stg 
          SET ITEM_DESCRIPTION= '"' ||(replace( ITEM_DESCRIPTION,'"','') )|| '"' 
          WHERE ITEM_DESCRIPTION LIKE '%,%'
           and execution_id = gv_execution_id 
           AND file_reference_identifier IS NULL;
                dbms_output.put_line('ITEM_DESCRIPTION With Comma is validated');
            END;

-- DIST_CODE_CONCATENATED Validation

 BEGIN                                                                                                                      
 Update xxcnv_ap_c005_ap_invoice_lines_stg
SET DIST_CODE_CONCATENATED = ((SELECT erp_coa_value FROM xxmap.xxmap_gl_e001_kaseya_ns_company WHERE ns_company_attribute_1 =(substr(DIST_CODE_CONCATENATED,1,instr(DIST_CODE_CONCATENATED,'|',1,1)-1)))||('-999-99999-211999-9999-999999-9999-9999-9999-999999'))
where file_reference_identifier IS NULL;
 dbms_output.put_line('DIST_CODE_CONCATENATED is updated');
 END;



-- Update ACCOUNTING DATE
BEGIN

      UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
        SET accounting_date = SYSDATE
		--UNIT_PRICE = null,
		--QUANTITY_INVOICED = null

        WHERE execution_id = gv_execution_id;
    DBMS_OUTPUT.PUT_LINE('ACCOUNTING_DATE is updated to sysdate'); 
END;



-- Validate SHIP_TO_LOCATION

BEGIN
    UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
    SET    SHIP_TO_LOCATION_CODE = (SELECT OS_LOCATION_NAME FROM XXCNV_PO_SHIP_TO_LOCATION_MAPPING WHERE NS_LOCATION_NAME = SHIP_TO_LOCATION_CODE)
	WHERE 1=1
	and file_reference_identifier is NULL;
    dbms_output.put_line('SHIP_TO_LOCATION_CODE is updated');
  END;

   BEGIN 
          UPDATE xxcnv_ap_c005_ap_invoice_lines_stg  
             SET UNIT_OF_MEAS_LOOKUP_CODE = 'Each'
    WHERE ((UNIT_OF_MEAS_LOOKUP_CODE IS NULL) or (UNIT_OF_MEAS_LOOKUP_CODE IN ('ea','each')));
    dbms_output.put_line('UNIT_OF_MEASURE is updated');
END;


-- Update TAX fileds
BEGIN 
 UPDATE xxcnv_ap_c005_ap_invoice_lines_stg l
 SET l.TAX_RATE_CODE =(select t.OC_Tax_rate_code FROM XXCNV_AP_Tax_Details_Mapping t, xxcnv_ap_c005_ap_invoices_stg H  WHERE t.OC_BU = H.target_operating_unit AND H.invoice_id= l.invoice_id),
     l.TAX_RATE = '1.00'
	 where 1=1
AND L.LINE_TYPE_LOOKUP_CODE = 'TAX'	 ;
	  DBMS_OUTPUT.PUT_LINE('TAX_RATE fileds are updated'); 
END;


BEGIN
	UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
    SET ATTRIBUTE_CATEGORY = 'NETSUITE_CONV_LINE'

	WHERE 1=1
	and execution_id = gv_execution_id;
	DBMS_OUTPUT.PUT_LINE('Attribute Category updated');
end;


BEGIN 
 UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
 SET UNIT_OF_MEAS_LOOKUP_CODE = null,
 QUANTITY_INVOICED = null,
 UNIT_PRICE = NULL;
 dbms_output.put_line('Quantity, Unit Price and UOM are updated');
END;
 -- Transformation for Multiple Tax Lines
BEGIN
UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
SET TAX_FLAG ='Y'
WHERE INVOICE_ID IN (select Invoice_id from xxcnv_ap_c005_ap_invoice_lines_stg where line_type_lookup_code ='TAX'
GROUP BY INVOICE_ID
HAVING COUNT(*) >1)
AND line_type_lookup_code ='TAX';
END;

BEGIN
UPDATE xxcnv_ap_c005_ap_invoice_lines_stg t
set amount = (
    SELECT
        SUM(l.amount)
    FROM
        xxcnv_ap_c005_ap_invoice_lines_stg l
    WHERE
            l.invoice_id = t.invoice_id
        AND l.tax_flag = 'Y'
),

DESCRIPTION = (
    SELECT
        LISTAGG (DISTINCT L.DESCRIPTION ,'+') 
    FROM
        xxcnv_ap_c005_ap_invoice_lines_stg l
    WHERE
            l.invoice_id = t.invoice_id
        AND l.tax_flag = 'Y'
),
aTTRIBUTE_NUMBER2 = (
    SELECT
        SUM(l.aTTRIBUTE_NUMBER2 )
    FROM
        xxcnv_ap_c005_ap_invoice_lines_stg l
    WHERE
            l.invoice_id = t.invoice_id
        AND l.tax_flag = 'Y'
),
TAX_FLAG = 'N'
WHERE
    ( t.invoice_id, t.line_number ) IN (
        SELECT
            l.invoice_id, MIN(l.line_number)
        FROM
            xxcnv_ap_c005_ap_invoice_lines_stg l
        WHERE
            l.tax_flag = 'Y'
        GROUP BY
            l.invoice_id
    )
    AND t.tax_flag = 'Y'
	and execution_id = gv_execution_id;
END;

Begin
DELETE FROM xxcnv_ap_c005_ap_invoice_lines_stg WHERE TAX_FLAG = 'Y';
end;


  --Erroring out the record in child table as it errored out in parent table
		  BEGIN
              -- Update the import_status in xxcnv_ap_c005_ap_invoice_lines_stg to 'ERROR' where the PARENT RECORD   has import_status 'ERROR'
              UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
              SET ERROR_MESSAGE = ERROR_MESSAGE || '|Invoice Header failed at validation' 
			  , IMPORT_status = 'ERROR'
              WHERE (INVOICE_ID IN (
                  SELECT INVOICE_ID
                  FROM xxcnv_ap_c005_ap_invoices_stg
                  WHERE IMPORT_status = 'ERROR'
				 and execution_id = gv_execution_id
				 ))
			  and execution_id = gv_execution_id
			  and file_reference_identifier is null;
          END;


       -- Update import_status based on error_message
		  BEGIN 
			UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
			SET import_status = CASE WHEN error_message IS NOT NULL THEN 'ERROR' ELSE 'PROCESSED' END;
			dbms_output.put_line('import_status is validated');
		  END;

     -- Final update to set error_message and import_status
     BEGIN      
	       UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
           SET  error_message = LTRIM(error_message, ','),  
		   import_status = CASE 
	       WHEN error_message IS NOT NULL THEN 'ERROR' ELSE 'PROCESSED' END
		     where execution_id = gv_execution_id ;
		   dbms_output.put_line('import_status column is updated');
           END;   

		   BEGIN
			UPDATE  xxcnv_ap_c005_ap_invoice_lines_stg
			SET FILE_NAME = gv_oci_file_name_apinvlines
			where file_reference_identifier is null;
			dbms_output.put_line('file_name column is updated');
		  END;

		   BEGIN 
            UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
            SET SOURCE_SYSTEM = gv_boundary_system
			WHERE execution_id = gv_execution_id ;
            dbms_output.put_line('source_system is updated');
          END;

-- Check if there are any error messages
	     SELECT COUNT(*) INTO lv_error_count 
		 FROM xxcnv_ap_c005_ap_invoice_lines_stg 
		 WHERE error_message is not null   
		 and execution_id = gv_execution_id ;


	      UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
          SET file_reference_identifier = gv_execution_id || '_' || gv_status_failure
	      where  file_reference_identifier is null
		  and error_message is not null;
          dbms_output.put_line('file_reference_identifier column is updated');

			UPDATE xxcnv_ap_c005_ap_invoice_lines_stg
			SET file_reference_identifier = gv_execution_id||'_'||gv_status_success
			where error_message is null
			AND file_reference_identifier is null
			and execution_id = gv_execution_id;
		    dbms_output.put_line('file_reference_identifier column is updated');

			 BEGIN
              -- Update the import_status in xxcnv_ap_c005_ap_invoice_lines_stg to 'ERROR' where the PARENT RECORD   has import_status 'ERROR'
              UPDATE xxcnv_ap_c005_ap_invoices_stg
              SET ERROR_MESSAGE = ERROR_MESSAGE || '|Invoice Line failed at validation' 
			  , IMPORT_status = 'ERROR'
			  ,file_reference_identifier = gv_execution_id || '_' || gv_status_failure
              WHERE (INVOICE_ID IN (
                  SELECT INVOICE_ID
                  FROM xxcnv_ap_c005_ap_invoice_lines_stg
                  WHERE IMPORT_status = 'ERROR'
				 and execution_id = gv_execution_id
				 ))
			  and execution_id = gv_execution_id;
			--and file_reference_identifier is null;
          END;


	     IF lv_error_count > 0 THEN


	       -- Logging the message If data is not validated
	     xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
	    			p_conversion_id 		=> gv_conversion_id,
	    			p_execution_id		    => gv_execution_id,
					p_execution_step 		=> gv_status_failed,
					p_boundary_system 		=> gv_boundary_system,
					p_file_path				=> gv_oci_file_path,
					p_file_name 			=> gv_oci_file_name_apinvlines,
					P_attribute1            => gv_batch_id,
					P_attribute2            => NULL,
					p_process_reference 	=> NULL);

		END IF;

		IF lv_error_count = 0 AND gv_oci_file_name_apinvlines  is NOT NULL THEN

		xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
				p_conversion_id 		=> gv_conversion_id,
				p_execution_id		    => gv_execution_id,
				p_execution_step 		=> gv_status_validated,
				p_boundary_system 		=> gv_boundary_system,
				p_file_path				=> gv_oci_file_path,
				p_file_name 			=> gv_oci_file_name_apinvlines,
				P_attribute1            => gv_batch_id,
				P_attribute2            => NULL,
				p_process_reference 	=> NULL );
		END IF;

	 -- commit;
--	 
		IF gv_oci_file_name_apinvlines is null THEN

		xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
				p_conversion_id 		=> gv_conversion_id,
				p_execution_id		    => gv_execution_id,
				p_execution_step 		=> gv_file_not_found,
				p_boundary_system 		=> gv_boundary_system,
				p_file_path				=> gv_oci_file_path,
				p_file_name 			=> gv_oci_file_name_apinvlines,
				P_attribute1            => gv_batch_id,
				P_attribute2            => NULL,
				p_process_reference 	=> NULL );



		END IF;

  ELSE
	 dbms_output.put_line('No Data is found in interface tables. Data is not loaded from ext to stg ');

	end if;

END;

END data_validations_prc;


/*==============================================================================================================================
-- PROCEDURE : create_fbdi_file_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the FBDI CSV file by using the data in the ap invoice stage tables after all validations.
================================================================================================================================= */
PROCEDURE create_fbdi_file_prc IS

 CURSOR batch_id_cursor IS

        SELECT DISTINCT batch_id
        FROM xxcnv_ap_c005_ap_invoices_stg
		where execution_id  = gv_execution_id
		AND file_reference_identifier = gv_execution_id||'_'||gv_status_success;

	 CURSOR batch_id_cursor_lines IS

        SELECT DISTINCT batch_id
        FROM xxcnv_ap_c005_ap_invoice_lines_stg
		where execution_id  = gv_execution_id
		AND file_reference_identifier = gv_execution_id||'_'||gv_status_success;

    lv_success_count NUMBER:=0;
    lv_batch_id VARCHAR(200);

BEGIN
--table 1
    BEGIN
         FOR g_id IN batch_id_cursor LOOP

        lv_batch_id := g_id.batch_id;

        dbms_output.put_line('Creating FBDI file for batch_id: ' || lv_batch_id);

            BEGIN
                -- Count the success record count for the current batch_id
                SELECT COUNT(*)
                INTO lv_success_count
                FROM xxcnv_ap_c005_ap_invoices_stg
                WHERE batch_id = lv_batch_id
				and file_reference_identifier = gv_execution_id||'_'||gv_status_success;


                dbms_output.put_line('Success record count for xxcnv_ap_c005_ap_invoices_stg for batch_id ' || lv_batch_id || ': ' || lv_success_count);

            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c005_ap_invoices_stg for batch_id: ' || lv_batch_id);
                    RETURN; --
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking success record count for xxcnv_ap_c005_ap_invoices_stg for batch_id ' || lv_batch_id || ': ' || SQLERRM);
                    RETURN; --
            END;

            IF lv_success_count > 0 THEN
                BEGIN
                    DBMS_CLOUD.EXPORT_DATA (
                        CREDENTIAL_NAME => gv_credential_name,
						FILE_URI_LIST   => REPLACE(gv_oci_file_path, gv_source_folder, gv_transformed_folder) || '/' || lv_batch_id || gv_oci_file_name_apinv,
                        FORMAT          => JSON_OBJECT('type' VALUE 'csv', 'trimspaces' VALUE 'rtrim', 'maxfilesize' value '629145600','header' value false),
                        QUERY           => 'SELECT 
                                              INVOICE_ID						,
									TARGET_OPERATING_UNIT 	 				, 
									SOURCE							,									
                                    CAST(INVOICE_NUM AS VARCHAR2(50)) AS INVOICE_NUM,
									INVOICE_AMOUNT					,
                                    TO_CHAR(INVOICE_DATE, ''YYYY/MM/DD'') AS INVOICE_DATE,
                             		VENDOR_NAME       				,	
									TARGET_VENDOR_NUM   					,
									TARGET_VENDOR_SITE_CODE				,
									INVOICE_CURRENCY_CODE 			,
									PAYMENT_CURRENCY_CODE  			,
									DESCRIPTION 					,
									GROUP_ID						,
									INVOICE_TYPE_LOOKUP_CODE 		,
									TARGET_LEGAL_ENTITY_NAME				,
									CUST_REGISTRATION_NUMBER 		,
									CUST_REGISTRATION_CODE			,
									FIRST_PARTY_REGISTRATION_NUM	,
									THIRD_PARTY_REGISTRATION_NUM	,
									TERMS_NAME						,
									TO_CHAR(TERMS_DATE, ''YYYY/MM/DD'') AS TERMS_DATE,
									TO_CHAR(GOODS_RECEIVED_DATE, ''YYYY/MM/DD'') AS GOODS_RECEIVED_DATE,
                                    TO_CHAR(INVOICE_RECEIVED_DATE, ''YYYY/MM/DD'') AS INVOICE_RECEIVED_DATE,
									TO_CHAR(GL_DATE, ''YYYY/MM/DD'') AS GL_DATE,
									PAYMENT_METHOD_CODE   			,
									PAY_GROUP_LOOKUP_CODE 			,
									EXCLUSIVE_PAYMENT_FLAG 			,
									AMOUNT_APPLICABLE_TO_DISCOUNT	,
									PREPAY_NUM						,
									PREPAY_LINE_NUM					,
									PREPAY_APPLY_AMOUNT				,
									PREPAY_GL_DATE					,
									INVOICE_INCLUDES_PREPAY_FLAG 	,
									EXCHANGE_RATE_TYPE				,
									TO_CHAR(EXCHANGE_DATE, ''YYYY/MM/DD'')	AS EXCHANGE_DATE,				
									EXCHANGE_RATE					,

									ACCTS_PAY_CODE_CONCATENATED		,
									DOC_CATEGORY_CODE				,
									VOUCHER_NUM						,
									REQUESTER_FIRST_NAME			,
									REQUESTER_LAST_NAME				,
									REQUESTER_EMPLOYEE_NUM			,
									DELIVERY_CHANNEL_CODE			,
									BANK_CHARGE_BEARER				,
									REMIT_TO_SUPPLIER_NAME			,
									REMIT_TO_SUPPLIER_NUM			,
									REMIT_TO_ADDRESS_NAME			,
									PAYMENT_PRIORITY				,
									SETTLEMENT_PRIORITY				,
									UNIQUE_REMITTANCE_IDENTIFIER	,
                                    URI_CHECK_DIGIT                 ,     									
									PAYMENT_REASON_CODE				,
									PAYMENT_REASON_COMMENTS			,
									REMITTANCE_MESSAGE_1			,
									REMITTANCE_MESSAGE_2			,
									REMITTANCE_MESSAGE_3			,
									AWT_GROUP_NAME					,
									SHIP_TO_LOCATION				,
									TAXATION_COUNTRY				,
									DOCUMENT_SUB_TYPE				,
									TAX_INVOICE_INTERNAL_SEQ		,
									SUPPLIER_TAX_INVOICE_NUMBER		,
									TAX_INVOICE_RECORDING_DATE		,
									SUPPLIER_TAX_INVOICE_DATE		,
									SUPPLIER_TAX_EXCHANGE_RATE		,
									PORT_OF_ENTRY_CODE				,
									CORRECTION_YEAR					,
									CORRECTION_PERIOD				,
									IMPORT_DOCUMENT_NUMBER			,
									IMPORT_DOCUMENT_DATE			,
									CONTROL_AMOUNT					,
									CALC_TAX_DURING_IMPORT_FLAG		,
									ADD_TAX_TO_INV_AMT_FLAG			,
									ATTRIBUTE_CATEGORY				,
									ATTRIBUTE1						,
									ATTRIBUTE2						,
									ATTRIBUTE3						,
									ATTRIBUTE4						,
									ATTRIBUTE5						,
									ATTRIBUTE6						,
									ATTRIBUTE7						,
									ATTRIBUTE8						,
									ATTRIBUTE9						,
									ATTRIBUTE10						,
									ATTRIBUTE11						,
									ATTRIBUTE12						,
									ATTRIBUTE13						,
									ATTRIBUTE14						,
									ATTRIBUTE15						,
									ATTRIBUTE_NUMBER1				,
									ATTRIBUTE_NUMBER2				,
									ATTRIBUTE_NUMBER3				,
									ATTRIBUTE_NUMBER4				,
									ATTRIBUTE_NUMBER5				,
									ATTRIBUTE_DATE1					,
									ATTRIBUTE_DATE2					,
									ATTRIBUTE_DATE3					,
									ATTRIBUTE_DATE4					,
									ATTRIBUTE_DATE5					,
									GLOBAL_ATTRIBUTE_CATEGORY		,
									GLOBAL_ATTRIBUTE1				,
									GLOBAL_ATTRIBUTE2				,
									GLOBAL_ATTRIBUTE3				,
									GLOBAL_ATTRIBUTE4				,
									GLOBAL_ATTRIBUTE5				,
									GLOBAL_ATTRIBUTE6				,
									GLOBAL_ATTRIBUTE7				,
									GLOBAL_ATTRIBUTE8				,
									GLOBAL_ATTRIBUTE9				,
									GLOBAL_ATTRIBUTE10				,
									GLOBAL_ATTRIBUTE11				,
									GLOBAL_ATTRIBUTE12				,
									GLOBAL_ATTRIBUTE13				,
									GLOBAL_ATTRIBUTE14				,
									GLOBAL_ATTRIBUTE15				,
									GLOBAL_ATTRIBUTE16				,
									GLOBAL_ATTRIBUTE17				,
									GLOBAL_ATTRIBUTE18				,
									GLOBAL_ATTRIBUTE19				,
									GLOBAL_ATTRIBUTE20				,
									GLOBAL_ATTRIBUTE_NUMBER1		,
									GLOBAL_ATTRIBUTE_NUMBER2		,
									GLOBAL_ATTRIBUTE_NUMBER3		,
									GLOBAL_ATTRIBUTE_NUMBER4		,
									GLOBAL_ATTRIBUTE_NUMBER5		,
									GLOBAL_ATTRIBUTE_DATE1			,
									GLOBAL_ATTRIBUTE_DATE2			,
									GLOBAL_ATTRIBUTE_DATE3			,
									GLOBAL_ATTRIBUTE_DATE4			,
									GLOBAL_ATTRIBUTE_DATE5			,
									IMAGE_DOCUMENT_URI				      
                                            FROM xxcnv_ap_c005_ap_invoices_stg
										  WHERE import_status = '''||'PROCESSED'||'''
											and batch_id ='''||lv_batch_id||'''
											AND file_reference_identifier= '''|| gv_execution_id|| '_' || gv_status_success||''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c005_ap_invoices_stg for batch_id ' || lv_batch_id || ' exported successfully to OCI Object Storage.');

                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id       => gv_conversion_id,
                        p_execution_id        => gv_execution_id,
                        p_execution_step      => gv_fbdi_export_status,
                        p_boundary_system     => gv_boundary_system,
                        p_file_path           => REPLACE(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                        p_file_name           => lv_batch_id || '_' || gv_oci_file_name_apinv,
                        P_attribute1          => lv_batch_id,
                        P_attribute2          => NULL,
                        p_process_reference   => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c005_ap_invoices_stg for batch_id ' || lv_batch_id || ': ' || SQLERRM);

                        RETURN;
                END;
           ELSE
                dbms_output.put_line('Process Stopped for xxcnv_ap_c005_ap_invoices_stg for batch_id ' || lv_batch_id || ': Error message columns contain data.');
			RETURN;
            END IF;
            END LOOP;

         EXCEPTION
          WHEN OTHERS THEN
		   dbms_output.put_line('An error occurred: ' ||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
				RETURN;
		 END;

--TABLE 2




  BEGIN
		BEGIN

			lv_success_count:=0;

			BEGIN
                -- Count the success record count for the current batch_id
                SELECT COUNT(*)
                INTO lv_success_count
                FROM xxcnv_ap_c005_ap_invoice_lines_stg
                WHERE batch_id = lv_batch_id
                --AND error_message IS NOT NULL
				and file_reference_identifier = gv_execution_id||'_'||gv_status_success;
               -- AND TRIM(error_message) != '';

                dbms_output.put_line('Success record count for xxcnv_ap_c005_ap_invoice_lines_stg for batch_id ' || lv_batch_id || ': ' || lv_success_count);

            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    dbms_output.put_line('No data found for xxcnv_ap_c005_ap_invoice_lines_stg for batch_id: ' || lv_batch_id);
                    RETURN;
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking success record count for xxcnv_ap_c005_ap_invoice_lines_stg for batch_id ' || lv_batch_id || ': ' || SQLERRM);
                    RETURN;
            END;

            IF lv_success_count > 0 THEN
                BEGIN
			            DBMS_CLOUD.EXPORT_DATA (
                        CREDENTIAL_NAME => gv_credential_name,
                        FILE_URI_LIST   => REPLACE(gv_oci_file_path, gv_source_folder, gv_transformed_folder) || '/' || lv_batch_id || gv_oci_file_name_apinvlines,
                        FORMAT          => JSON_OBJECT('type' VALUE 'csv', 'trimspaces' VALUE 'rtrim','maxfilesize' value '629145600','header' value false),
                        QUERY           => 'SELECT 

			INVOICE_ID						,
					LINE_NUMBER						,			
					LINE_TYPE_LOOKUP_CODE			,
					AMOUNT							,
					QUANTITY_INVOICED				,
					UNIT_PRICE						,
					UNIT_OF_MEAS_LOOKUP_CODE		,
					DESCRIPTION						,  
					PO_NUMBER						,
					PO_LINE_NUMBER						,	
					PO_SHIPMENT_NUM					,
					PO_DISTRIBUTION_NUM				,
					ITEM_DESCRIPTION				, 
					RELEASE_NUM						,			
					PURCHASING_CATEGORY				,
					RECEIPT_NUMBER					,
					RECEIPT_LINE_NUMBER				,
					CONSUMPTION_ADVICE_NUMBER		,
					CONSUMPTION_ADVICE_LINE_NUMBER	,
					PACKAGING_SLIP					,		
					FINAL_MATCH_FLAG				,	
					DIST_CODE_CONCATENATED			,
					DISTRIBUTION_SET_NAME			,
                    TO_CHAR(ACCOUNTING_DATE, ''YYYY/MM/DD'') AS ACCOUNTING_DATE,
					ACCOUNT_SEGMENT					,
					BALANCING_SEGMENT				,
					COST_CENTER_SEGMENT				,
					TAX_CLASSIFICATION_CODE			,
					SHIP_TO_LOCATION_CODE			,
					SHIP_FROM_LOCATION_CODE			,
					FINAL_DISCHARGE_LOCATION_CODE	,	
					TRX_BUSINESS_CATEGORY			,
					PRODUCT_FISC_CLASSIFICATION		,
					PRIMARY_INTENDED_USE			,
					USER_DEFINED_FISC_CLASS			,
					PRODUCT_TYPE					,
					ASSESSABLE_VALUE				,
					PRODUCT_CATEGORY				,
					CONTROL_AMOUNT					,
					TAX_REGIME_CODE					,
					TAX								,
					TAX_STATUS_CODE					,
					TAX_JURISDICTION_CODE			,
					TAX_RATE_CODE					,
					TAX_RATE						,
					AWT_GROUP_NAME					,
					TYPE_1099						,
					INCOME_TAX_REGION				,
					PRORATE_ACROSS_FLAG				,
					LINE_GROUP_NUMBER				,
					COST_FACTOR_NAME				,
					STAT_AMOUNT						,
					ASSETS_TRACKING_FLAG			,
					ASSET_BOOK_TYPE_CODE			,
					ASSET_CATEGORY_ID				,
					SERIAL_NUMBER					,
					MANUFACTURER					,
					MODEL_NUMBER					,
					WARRANTY_NUMBER					,
					PRICE_CORRECTION_FLAG			,
					PRICE_CORRECTION_INV_NUM		,
					PRICE_CORRECTION_INV_LINE_NUM	,
					REQUESTER_FIRST_NAME			,
					REQUESTER_LAST_NAME				,
					REQUESTER_EMPLOYEE_NUM			,
					ATTRIBUTE_CATEGORY				,
					ATTRIBUTE1						,
					ATTRIBUTE2						,
					ATTRIBUTE3						,
					ATTRIBUTE4						,
					ATTRIBUTE5						,
					ATTRIBUTE6						,
					ATTRIBUTE7						,
					ATTRIBUTE8						,
					ATTRIBUTE9						,
					ATTRIBUTE10						,
					ATTRIBUTE11						,
					ATTRIBUTE12						,
					ATTRIBUTE13						,
					ATTRIBUTE14						,
					ATTRIBUTE15						,
					ATTRIBUTE_NUMBER1				,
					ATTRIBUTE_NUMBER2				,
					ATTRIBUTE_NUMBER3				,
					ATTRIBUTE_NUMBER4				,
					ATTRIBUTE_NUMBER5				,
					ATTRIBUTE_DATE1					,
					ATTRIBUTE_DATE2					,
					ATTRIBUTE_DATE3					,
					ATTRIBUTE_DATE4					,
					ATTRIBUTE_DATE5					,
					GLOBAL_ATTRIBUTE_CATEGORY		,
					GLOBAL_ATTRIBUTE1				,
					GLOBAL_ATTRIBUTE2				,
					GLOBAL_ATTRIBUTE3				,
					GLOBAL_ATTRIBUTE4				,
					GLOBAL_ATTRIBUTE5				,
					GLOBAL_ATTRIBUTE6				,
					GLOBAL_ATTRIBUTE7				,
					GLOBAL_ATTRIBUTE8				,
					GLOBAL_ATTRIBUTE9				,
					GLOBAL_ATTRIBUTE10				,
					GLOBAL_ATTRIBUTE11				,
					GLOBAL_ATTRIBUTE12				,
					GLOBAL_ATTRIBUTE13				,
					GLOBAL_ATTRIBUTE14				,
					GLOBAL_ATTRIBUTE15				,
					GLOBAL_ATTRIBUTE16				,
					GLOBAL_ATTRIBUTE17				,
					GLOBAL_ATTRIBUTE18				,
					GLOBAL_ATTRIBUTE19				,
					GLOBAL_ATTRIBUTE20				,
					GLOBAL_ATTRIBUTE_NUMBER1		,
					GLOBAL_ATTRIBUTE_NUMBER2		,
					GLOBAL_ATTRIBUTE_NUMBER3		,
					GLOBAL_ATTRIBUTE_NUMBER4		,
					GLOBAL_ATTRIBUTE_NUMBER5		,
					GLOBAL_ATTRIBUTE_DATE1			,
					GLOBAL_ATTRIBUTE_DATE2			,
					GLOBAL_ATTRIBUTE_DATE3			,
					GLOBAL_ATTRIBUTE_DATE4			,
					GLOBAL_ATTRIBUTE_DATE5			,
					PJC_PROJECT_ID					,
					PJC_TASK_ID						,
					PJC_EXPENDITURE_TYPE_ID			,
					PJC_EXPENDITURE_ITEM_DATE		,
					PJC_ORGANIZATION_ID				,
					PJC_PROJECT_NUMBER				,
					PJC_TASK_NUMBER					,
					PJC_EXPENDITURE_TYPE_NAME		,
					PJC_ORGANIZATION_NAME			,
					PJC_RESERVED_ATTRIBUTE1			,	
					PJC_RESERVED_ATTRIBUTE2			,
					PJC_RESERVED_ATTRIBUTE3			,
					PJC_RESERVED_ATTRIBUTE4			,
					PJC_RESERVED_ATTRIBUTE5			,
					PJC_RESERVED_ATTRIBUTE6			,
					PJC_RESERVED_ATTRIBUTE7			,
					PJC_RESERVED_ATTRIBUTE8			,
					PJC_RESERVED_ATTRIBUTE9			,
					PJC_RESERVED_ATTRIBUTE10		,
					PJC_USER_DEF_ATTRIBUTE1			,
					PJC_USER_DEF_ATTRIBUTE2			,
					PJC_USER_DEF_ATTRIBUTE3			,
					PJC_USER_DEF_ATTRIBUTE4			,
					PJC_USER_DEF_ATTRIBUTE5			,
					PJC_USER_DEF_ATTRIBUTE6			,
					PJC_USER_DEF_ATTRIBUTE7			,
					PJC_USER_DEF_ATTRIBUTE8			,
					PJC_USER_DEF_ATTRIBUTE9			,
					PJC_USER_DEF_ATTRIBUTE10		,
					FISCAL_CHARGE_TYPE				,
					DEF_ACCTG_START_DATE			,
					DEF_ACCTG_END_DATE				,
					DEF_ACCRUAL_CODE_CONCATENATED	,
					PJC_PROJECT_NAME				,
					PJC_TASK_NAME					,
                    PJC_WORK_TYPE					,
					PJC_CONTRACT_NAME				,
					PJC_CONTRACT_NUMBER				,	
					PJC_FUNDING_SOURCE_NAME			,
					PJC_FUNDING_SOURCE_NUMBER		,
					REQUESTER_EMAIL_ADDRESS			,
                    RCV_TRANSACTION_ID              	


                                            FROM xxcnv_ap_c005_ap_invoice_lines_stg
                                            WHERE import_status = '''||'PROCESSED'||'''
                                            AND batch_id ='''||lv_batch_id||'''
									        AND file_reference_identifier= '''|| gv_execution_id|| '_' || gv_status_success||''''
                    );

                    dbms_output.put_line('CSV file for xxcnv_ap_c005_ap_invoices_stg for batch_id ' || lv_batch_id || ' exported successfully to AP_INVOICE_LINES OCI Object Storage.');

                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id       => gv_conversion_id,
                        p_execution_id        => gv_execution_id,
                        p_execution_step      => gv_fbdi_export_status,
                        p_boundary_system     => gv_boundary_system,
                        p_file_path           => REPLACE(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                        p_file_name           => lv_batch_id || '_' || gv_oci_file_name_apinvlines ,
                        P_attribute1          => lv_batch_id,
                        P_attribute2          => NULL,
                        p_process_reference   => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c005_ap_invoice_lines_stg for batch_id ' || lv_batch_id || ': ' || SQLERRM);

                        RETURN;
                END;
            ELSE
                dbms_output.put_line('Process Stopped for xxcnv_ap_c005_ap_invoice_lines_stg for batch_id ' || lv_batch_id || ': Error message columns contain data.');
                RETURN;
			END IF;

                --dbms_output.put_line('FBDI created ' || lv_batch_id);


    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('An error occurred: ' ||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
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

        SELECT DISTINCT BATCH_ID
        FROM xxcnv_ap_c005_ap_invoices_stg
	    where execution_id  = gv_execution_id;
		--file_reference_identifier = gv_execution_id||'_'||gv_status_success;


    lv_error_count NUMBER;
    lv_BATCH_ID    VARCHAR(250);


 BEGIN

    FOR g_id IN batch_id_cursor LOOP
        lv_BATCH_ID := g_id.BATCH_ID;
        dbms_output.put_line('Processing BATCH_ID: ' || lv_BATCH_ID);




            BEGIN

                DBMS_CLOUD.EXPORT_DATA (
                    CREDENTIAL_NAME => gv_credential_name,
                    FILE_URI_LIST   => REPLACE(gv_oci_file_path, gv_source_folder, gv_transformed_folder) || '/' || gv_batch_id || lv_BATCH_ID || 'APInvoicesInt.properties',
                    FORMAT          => JSON_OBJECT('trimspaces' VALUE 'rtrim'),
          QUERY    =>  'SELECT ''/oracle/apps/ess/financials/payables/invoices/transactions/,APXIIMPT,APInvoicesInt,' || lv_BATCH_ID || ',null,300000002224558,N,null,null,null,1000,CONVERSION,null,N,N,300000001891564,null,1''as column1 from dual'

		     );

                dbms_output.put_line('Properties file for BATCH_ID ' || lv_BATCH_ID || ' exported successfully to OCI Object Storage.');

                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id       => gv_conversion_id,
                    p_execution_id        => gv_execution_id,
                    p_execution_step      => gv_status_staged,
                    p_boundary_system     => gv_boundary_system,
                    p_file_path           => REPLACE(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                    p_file_name           => gv_batch_id || lv_BATCH_ID || '_' || 'APInvoicesInt.properties' ,
                    P_attribute1          => gv_batch_id,
                    P_attribute2          => NULL,
                    p_process_reference   => NULL
                );


            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error exporting data to properties for BATCH_ID ' || lv_BATCH_ID || ': ' || SQLERRM);

            END;

    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('An error occurred: ' || SQLERRM);

END create_properties_file_prc;

/*==============================================================================================================================
-- PROCEDURE : create_atp_validation_recon_report_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating properties file.
================================================================================================================================= */
PROCEDURE 
create_atp_validation_recon_report_prc IS

    CURSOR batch_id_cursor IS
        SELECT DISTINCT batch_ID
        FROM xxcnv_ap_c005_ap_invoices_stg
        WHERE execution_id  = gv_execution_id
		AND file_reference_identifier = gv_execution_id ||'_'||gv_status_failure;

	  CURSOR batch_id_cursor_lines IS
        SELECT DISTINCT BATCH_ID
        FROM xxcnv_ap_c005_ap_invoice_lines_stg
        WHERE execution_id  = gv_execution_id
		AND file_reference_identifier = gv_execution_id ||'_'||gv_status_failure;

    lv_error_count NUMBER;
    lv_batch_id    VARCHAR(200);

BEGIN
    FOR g_id IN batch_id_cursor LOOP
        lv_batch_id := g_id.batch_ID;
        dbms_output.put_line('Processing recon report for batch_id: ' || lv_batch_id || '_' || gv_oci_file_path || '_' || gv_source_folder || '_' || gv_recon_folder );

        BEGIN
            DBMS_CLOUD.EXPORT_DATA (
                CREDENTIAL_NAME => gv_credential_name,
                FILE_URI_LIST   => REPLACE(gv_oci_file_path, gv_source_folder, gv_recon_folder) || '/' || lv_batch_id || 'ATP_Recon_AP_Invoices'|| '_' || gv_boundary_system || '_' || sysdate,
                FORMAT          => JSON_OBJECT('type' VALUE 'csv', 'trimspaces' VALUE 'rtrim', 'maxfilesize' value '629145600', 'header' value true),
                QUERY           => '
  SELECT 
                               INVOICE_ID						,
									OPERATING_UNIT 	 				, 
									SOURCE							,
									CAST(INVOICE_NUM AS VARCHAR2(50)) AS INVOICE_NUM,
									INVOICE_AMOUNT					,
									INVOICE_DATE					,
									VENDOR_NAME       				,	
									VENDOR_NUM   					,
									VENDOR_SITE_CODE				,
									INVOICE_CURRENCY_CODE 			,
									PAYMENT_CURRENCY_CODE  			,
									DESCRIPTION 					,
									GROUP_ID						,
									INVOICE_TYPE_LOOKUP_CODE 		,
									LEGAL_ENTITY_NAME				,
									CUST_REGISTRATION_NUMBER 		,
									CUST_REGISTRATION_CODE			,
									FIRST_PARTY_REGISTRATION_NUM	,
									THIRD_PARTY_REGISTRATION_NUM	,
									TERMS_NAME						,
									TERMS_DATE						,
									GOODS_RECEIVED_DATE 				,
									INVOICE_RECEIVED_DATE 			,
									GL_DATE						,
									PAYMENT_METHOD_CODE   			,
									PAY_GROUP_LOOKUP_CODE 			,
									EXCLUSIVE_PAYMENT_FLAG 			,
									AMOUNT_APPLICABLE_TO_DISCOUNT	,
									PREPAY_NUM						,
									PREPAY_LINE_NUM					,
									PREPAY_APPLY_AMOUNT				,
									PREPAY_GL_DATE						,
									INVOICE_INCLUDES_PREPAY_FLAG 	,
									EXCHANGE_RATE_TYPE				,
									EXCHANGE_DATE 					,
									EXCHANGE_RATE					,

									ACCTS_PAY_CODE_CONCATENATED		,
									DOC_CATEGORY_CODE				,
									VOUCHER_NUM						,
									REQUESTER_FIRST_NAME			,
									REQUESTER_LAST_NAME				,
									REQUESTER_EMPLOYEE_NUM			,
									DELIVERY_CHANNEL_CODE			,
									BANK_CHARGE_BEARER				,
									REMIT_TO_SUPPLIER_NAME			,
									REMIT_TO_SUPPLIER_NUM			,
									REMIT_TO_ADDRESS_NAME			,
									PAYMENT_PRIORITY				,
									SETTLEMENT_PRIORITY				,
									UNIQUE_REMITTANCE_IDENTIFIER	,
                                    URI_CHECK_DIGIT                 ,     									
									PAYMENT_REASON_CODE				,
									PAYMENT_REASON_COMMENTS			,
									REMITTANCE_MESSAGE_1			,
									REMITTANCE_MESSAGE_2			,
									REMITTANCE_MESSAGE_3			,
									AWT_GROUP_NAME					,
									SHIP_TO_LOCATION				,
									TAXATION_COUNTRY				,
									DOCUMENT_SUB_TYPE				,
									TAX_INVOICE_INTERNAL_SEQ		,
									SUPPLIER_TAX_INVOICE_NUMBER		,
									TAX_INVOICE_RECORDING_DATE		,
									SUPPLIER_TAX_INVOICE_DATE		,
									SUPPLIER_TAX_EXCHANGE_RATE		,
									PORT_OF_ENTRY_CODE				,
									CORRECTION_YEAR					,
									CORRECTION_PERIOD				,
									IMPORT_DOCUMENT_NUMBER			,
									IMPORT_DOCUMENT_DATE			,
									CONTROL_AMOUNT					,
									CALC_TAX_DURING_IMPORT_FLAG		,
									ADD_TAX_TO_INV_AMT_FLAG			,
									ATTRIBUTE_CATEGORY				,
									ATTRIBUTE1						,
									ATTRIBUTE2						,
									ATTRIBUTE3						,
									ATTRIBUTE4						,
									ATTRIBUTE5						,
									ATTRIBUTE6						,
									ATTRIBUTE7						,
									ATTRIBUTE8						,
									ATTRIBUTE9						,
									ATTRIBUTE10						,
									ATTRIBUTE11						,
									ATTRIBUTE12						,
									ATTRIBUTE13						,
									ATTRIBUTE14						,
									ATTRIBUTE15						,
									ATTRIBUTE_NUMBER1				,
									ATTRIBUTE_NUMBER2				,
									ATTRIBUTE_NUMBER3				,
									ATTRIBUTE_NUMBER4				,
									ATTRIBUTE_NUMBER5				,
									ATTRIBUTE_DATE1					,
									ATTRIBUTE_DATE2					,
									ATTRIBUTE_DATE3					,
									ATTRIBUTE_DATE4					,
									ATTRIBUTE_DATE5					,
									GLOBAL_ATTRIBUTE_CATEGORY		,
									GLOBAL_ATTRIBUTE1				,
									GLOBAL_ATTRIBUTE2				,
									GLOBAL_ATTRIBUTE3				,
									GLOBAL_ATTRIBUTE4				,
									GLOBAL_ATTRIBUTE5				,
									GLOBAL_ATTRIBUTE6				,
									GLOBAL_ATTRIBUTE7				,
									GLOBAL_ATTRIBUTE8				,
									GLOBAL_ATTRIBUTE9				,
									GLOBAL_ATTRIBUTE10				,
									GLOBAL_ATTRIBUTE11				,
									GLOBAL_ATTRIBUTE12				,
									GLOBAL_ATTRIBUTE13				,
									GLOBAL_ATTRIBUTE14				,
									GLOBAL_ATTRIBUTE15				,
									GLOBAL_ATTRIBUTE16				,
									GLOBAL_ATTRIBUTE17				,
									GLOBAL_ATTRIBUTE18				,
									GLOBAL_ATTRIBUTE19				,
									GLOBAL_ATTRIBUTE20				,
									GLOBAL_ATTRIBUTE_NUMBER1		,
									GLOBAL_ATTRIBUTE_NUMBER2		,
									GLOBAL_ATTRIBUTE_NUMBER3		,
									GLOBAL_ATTRIBUTE_NUMBER4		,
									GLOBAL_ATTRIBUTE_NUMBER5		,
									GLOBAL_ATTRIBUTE_DATE1			,
									GLOBAL_ATTRIBUTE_DATE2			,
									GLOBAL_ATTRIBUTE_DATE3			,
									GLOBAL_ATTRIBUTE_DATE4			,
									GLOBAL_ATTRIBUTE_DATE5			,
									IMAGE_DOCUMENT_URI				,  
									file_name,
                               import_status,
                               error_message,
                               file_reference_identifier,
                               batch_id,
							   EXECUTION_ID  					,
                               source_system							   
                                    FROM xxcnv_ap_c005_ap_invoices_stg 
                                    where import_status = '''||'ERROR'||'''
									and execution_id  =  '''||gv_execution_id||''''      
									);

            dbms_output.put_line('CSV file for xxcnv_ap_c005_ap_invoices_stg for batch_id ' || lv_batch_id || ' exported successfully to OCI Object Storage.');

            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id       => gv_conversion_id,
                p_execution_id        => gv_execution_id,
                p_execution_step      => gv_recon_report,
                p_boundary_system     => gv_boundary_system,
                p_file_path           => REPLACE(gv_oci_file_path, gv_source_folder, gv_recon_folder),
                p_file_name           => lv_batch_id || '_' || gv_oci_file_name_apinv,
                P_attribute1          => lv_batch_id,
                P_attribute2          => NULL,
                p_process_reference   => NULL
            );

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c005_ap_invoices_stg for batch_id ' || lv_batch_id || ': ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);
                RETURN;
        END;
    END LOOP;


----Table 2

BEGIN
  FOR g_id IN batch_id_cursor_lines LOOP
       lv_batch_id := g_id.batch_id;
        --dbms_output.put_line('Processing batch_id: ' || lv_batch_id);
        dbms_output.put_line('Processing recon report for batch_id: ' || lv_batch_id || '_' || gv_oci_file_path || '_' || gv_source_folder || '_' || gv_recon_folder );

        BEGIN
            DBMS_CLOUD.EXPORT_DATA (
                CREDENTIAL_NAME => gv_credential_name,
                FILE_URI_LIST   => REPLACE(gv_oci_file_path, gv_source_folder, gv_recon_folder) || '/' || lv_batch_id || 'ATP_Recon_AP_Invoices_Lines'|| sysdate,
                FORMAT          => JSON_OBJECT('type' VALUE 'csv', 'trimspaces' VALUE 'rtrim', 'maxfilesize' value '629145600', 'header' value true),
                QUERY           => '

       SELECT 

					INVOICE_ID						,
					LINE_NUMBER						,			
					LINE_TYPE_LOOKUP_CODE			,
					AMOUNT							,
					QUANTITY_INVOICED				,
					UNIT_PRICE						,
					UNIT_OF_MEAS_LOOKUP_CODE		,
					DESCRIPTION						,  
					PO_NUMBER						,
					PO_LINE_NUMBER						,	
					PO_SHIPMENT_NUM					,
					PO_DISTRIBUTION_NUM				,
					ITEM_DESCRIPTION				, 
					RELEASE_NUM						,			
					PURCHASING_CATEGORY				,
					RECEIPT_NUMBER					,
					RECEIPT_LINE_NUMBER				,
					CONSUMPTION_ADVICE_NUMBER		,
					CONSUMPTION_ADVICE_LINE_NUMBER	,
					PACKAGING_SLIP					,		
					FINAL_MATCH_FLAG				,	
					DIST_CODE_CONCATENATED			,
					DISTRIBUTION_SET_NAME			,
					ACCOUNTING_DATE					,
					ACCOUNT_SEGMENT					,
					BALANCING_SEGMENT				,
					COST_CENTER_SEGMENT				,
					TAX_CLASSIFICATION_CODE			,
					SHIP_TO_LOCATION_CODE			,
					SHIP_FROM_LOCATION_CODE			,
					FINAL_DISCHARGE_LOCATION_CODE	,	
					TRX_BUSINESS_CATEGORY			,
					PRODUCT_FISC_CLASSIFICATION		,
					PRIMARY_INTENDED_USE			,
					USER_DEFINED_FISC_CLASS			,
					PRODUCT_TYPE					,
					ASSESSABLE_VALUE				,
					PRODUCT_CATEGORY				,
					CONTROL_AMOUNT					,
					TAX_REGIME_CODE					,
					TAX								,
					TAX_STATUS_CODE					,
					TAX_JURISDICTION_CODE			,
					TAX_RATE_CODE					,
					TAX_RATE						,
					AWT_GROUP_NAME					,
					TYPE_1099						,
					INCOME_TAX_REGION				,
					PRORATE_ACROSS_FLAG				,
					LINE_GROUP_NUMBER				,
					COST_FACTOR_NAME				,
					STAT_AMOUNT						,
					ASSETS_TRACKING_FLAG			,
					ASSET_BOOK_TYPE_CODE			,
					ASSET_CATEGORY_ID				,
					SERIAL_NUMBER					,
					MANUFACTURER					,
					MODEL_NUMBER					,
					WARRANTY_NUMBER					,
					PRICE_CORRECTION_FLAG			,
					PRICE_CORRECTION_INV_NUM		,
					PRICE_CORRECTION_INV_LINE_NUM	,
					REQUESTER_FIRST_NAME			,
					REQUESTER_LAST_NAME				,
					REQUESTER_EMPLOYEE_NUM			,
					ATTRIBUTE_CATEGORY				,
					ATTRIBUTE1						,
					ATTRIBUTE2						,
					ATTRIBUTE3						,
					ATTRIBUTE4						,
					ATTRIBUTE5						,
					ATTRIBUTE6						,
					ATTRIBUTE7						,
					ATTRIBUTE8						,
					ATTRIBUTE9						,
					ATTRIBUTE10						,
					ATTRIBUTE11						,
					ATTRIBUTE12						,
					ATTRIBUTE13						,
					ATTRIBUTE14						,
					ATTRIBUTE15						,
					ATTRIBUTE_NUMBER1				,
					ATTRIBUTE_NUMBER2				,
					ATTRIBUTE_NUMBER3				,
					ATTRIBUTE_NUMBER4				,
					ATTRIBUTE_NUMBER5				,
					ATTRIBUTE_DATE1					,
					ATTRIBUTE_DATE2					,
					ATTRIBUTE_DATE3					,
					ATTRIBUTE_DATE4					,
					ATTRIBUTE_DATE5					,
					GLOBAL_ATTRIBUTE_CATEGORY		,
					GLOBAL_ATTRIBUTE1				,
					GLOBAL_ATTRIBUTE2				,
					GLOBAL_ATTRIBUTE3				,
					GLOBAL_ATTRIBUTE4				,
					GLOBAL_ATTRIBUTE5				,
					GLOBAL_ATTRIBUTE6				,
					GLOBAL_ATTRIBUTE7				,
					GLOBAL_ATTRIBUTE8				,
					GLOBAL_ATTRIBUTE9				,
					GLOBAL_ATTRIBUTE10				,
					GLOBAL_ATTRIBUTE11				,
					GLOBAL_ATTRIBUTE12				,
					GLOBAL_ATTRIBUTE13				,
					GLOBAL_ATTRIBUTE14				,
					GLOBAL_ATTRIBUTE15				,
					GLOBAL_ATTRIBUTE16				,
					GLOBAL_ATTRIBUTE17				,
					GLOBAL_ATTRIBUTE18				,
					GLOBAL_ATTRIBUTE19				,
					GLOBAL_ATTRIBUTE20				,
					GLOBAL_ATTRIBUTE_NUMBER1		,
					GLOBAL_ATTRIBUTE_NUMBER2		,
					GLOBAL_ATTRIBUTE_NUMBER3		,
					GLOBAL_ATTRIBUTE_NUMBER4		,
					GLOBAL_ATTRIBUTE_NUMBER5		,
					GLOBAL_ATTRIBUTE_DATE1			,
					GLOBAL_ATTRIBUTE_DATE2			,
					GLOBAL_ATTRIBUTE_DATE3			,
					GLOBAL_ATTRIBUTE_DATE4			,
					GLOBAL_ATTRIBUTE_DATE5			,
					PJC_PROJECT_ID					,
					PJC_TASK_ID						,
					PJC_EXPENDITURE_TYPE_ID			,
					PJC_EXPENDITURE_ITEM_DATE		,
					PJC_ORGANIZATION_ID				,
					PJC_PROJECT_NUMBER				,
					PJC_TASK_NUMBER					,
					PJC_EXPENDITURE_TYPE_NAME		,
					PJC_ORGANIZATION_NAME			,
					PJC_RESERVED_ATTRIBUTE1			,	
					PJC_RESERVED_ATTRIBUTE2			,
					PJC_RESERVED_ATTRIBUTE3			,
					PJC_RESERVED_ATTRIBUTE4			,
					PJC_RESERVED_ATTRIBUTE5			,
					PJC_RESERVED_ATTRIBUTE6			,
					PJC_RESERVED_ATTRIBUTE7			,
					PJC_RESERVED_ATTRIBUTE8			,
					PJC_RESERVED_ATTRIBUTE9			,
					PJC_RESERVED_ATTRIBUTE10		,
					PJC_USER_DEF_ATTRIBUTE1			,
					PJC_USER_DEF_ATTRIBUTE2			,
					PJC_USER_DEF_ATTRIBUTE3			,
					PJC_USER_DEF_ATTRIBUTE4			,
					PJC_USER_DEF_ATTRIBUTE5			,
					PJC_USER_DEF_ATTRIBUTE6			,
					PJC_USER_DEF_ATTRIBUTE7			,
					PJC_USER_DEF_ATTRIBUTE8			,
					PJC_USER_DEF_ATTRIBUTE9			,
					PJC_USER_DEF_ATTRIBUTE10		,
					FISCAL_CHARGE_TYPE				,
					DEF_ACCTG_START_DATE			,
					DEF_ACCTG_END_DATE				,
					DEF_ACCRUAL_CODE_CONCATENATED	,
					PJC_PROJECT_NAME				,
					PJC_TASK_NAME					,					
					FILE_NAME 						,
					ERROR_MESSAGE 					,
					IMPORT_STATUS  					,
					EXECUTION_ID  					,
					FILE_REFERENCE_IDENTIFIER 		,
					SOURCE_SYSTEM   				,
					Batch_ID
                                    FROM xxcnv_ap_c005_ap_invoice_lines_stg   
                                      where import_status = '''||'ERROR'||'''
									and execution_id  =  '''||gv_execution_id||''''      
									);

            dbms_output.put_line('CSV file for xxcnv_ap_c005_ap_invoice_lines_stg for batch_id ' || lv_batch_id || ' exported successfully to OCI Object Storage.');

            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id       => gv_conversion_id,
                p_execution_id        => gv_execution_id,
                p_execution_step      => gv_recon_report,
                p_boundary_system     => gv_boundary_system,
                p_file_path           => REPLACE(gv_oci_file_path, gv_source_folder, gv_recon_folder),
                p_file_name           => lv_batch_id || '_' || gv_oci_file_name_apinvlines,
                P_attribute1          => lv_batch_id,
                P_attribute2          => NULL,
                p_process_reference   => NULL
            );

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error exporting data to CSV for xxcnv_ap_c005_ap_invoice_lines_stg for batch_id ' || lv_batch_id || ': ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);
               -- RETURN;
        END;
    END LOOP;

END;
END create_atp_validation_recon_report_prc;

/*==============================================================================================================================
-- PROCEDURE : coa_target_segments_header_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used .
================================================================================================================================= */
PROCEDURE coa_target_segments_header_prc IS

    lv_status 			VARCHAR2(50);
    lv_message 			VARCHAR2(500);
    lv_target_segment 	VARCHAR2(200);
    lv_error_message 	VARCHAR2(500);
	lv_target_segment1 	VARCHAR2(25);
	lv_target_segment2 	VARCHAR2(25);
	lv_target_segment3 	VARCHAR2(25);
	lv_target_segment4 	VARCHAR2(25);
	lv_target_segment5 	VARCHAR2(25);
	lv_target_segment6 	VARCHAR2(25);
	lv_target_segment7 	VARCHAR2(25);
	lv_target_segment8 	VARCHAR2(25);
	lv_target_segment9 	VARCHAR2(25);
	lv_target_segment10 VARCHAR2(25);
	lv_seg1  VARCHAR2(25);
	lv_seg2  VARCHAR2(25);
	lv_seg3  VARCHAR2(25);
	lv_seg4  VARCHAR2(25);
	lv_seg5  VARCHAR2(25);
	lv_seg6  VARCHAR2(25);
	lv_seg7  VARCHAR2(25);
	lv_seg8  VARCHAR2(25);
	lv_seg9  VARCHAR2(25);
	lv_seg10  VARCHAR2(25);
       lv_pkg_name VARCHAR2(10) :=  'AP';
	--gv_execution_id VARCHAR2(50):= 'yYX3bgTkEfC-WUnVzByPMg'

BEGIN



    FOR rec IN (SELECT rowid as identifier, x.* FROM xxcnv_ap_c005_ap_invoices_stg x
	            where accts_pay_code_concatenated IS NOT NULL
				AND execution_id = gv_execution_id) 
		LOOP
		lv_seg1   := NULL;
		lv_seg2   := NULL;
		lv_seg3   := NULL;
		lv_seg4   := NULL;
		lv_seg5   := NULL;
		lv_seg6   := NULL;
		lv_seg7   := NULL;
		lv_seg8   := NULL;
		lv_seg9   := NULL;
		lv_seg10  := NULL;

		SELECT  substr(rec.accts_pay_code_concatenated,1,instr(rec.accts_pay_code_concatenated,'|',1,1)-1)
				,substr(rec.accts_pay_code_concatenated,instr(rec.accts_pay_code_concatenated,'|',1,1)+1,instr(rec.accts_pay_code_concatenated,'|',1,2)-instr(rec.accts_pay_code_concatenated,'|',1,1)-1) 
				,substr(rec.accts_pay_code_concatenated,instr(rec.accts_pay_code_concatenated,'|',1,2)+1,instr(rec.accts_pay_code_concatenated,'|',1,3)-instr(rec.accts_pay_code_concatenated,'|',1,2)-1) 
				,substr(rec.accts_pay_code_concatenated,instr(rec.accts_pay_code_concatenated,'|',1,3)+1,instr(rec.accts_pay_code_concatenated,'|',1,4)-instr(rec.accts_pay_code_concatenated,'|',1,3)-1) 
				,substr(rec.accts_pay_code_concatenated,instr(rec.accts_pay_code_concatenated,'|',1,4)+1,instr(rec.accts_pay_code_concatenated,'|',1,5)-instr(rec.accts_pay_code_concatenated,'|',1,4)-1) 
				,substr(rec.accts_pay_code_concatenated,instr(rec.accts_pay_code_concatenated,'|',1,5)+1)
				--,substr(rec.accts_pay_code_concatenated,instr(rec.accts_pay_code_concatenated,'|',1,6)+1,instr(rec.accts_pay_code_concatenated,'|',1,7)-instr(rec.accts_pay_code_concatenated,'|',1,6)-1) 
				--,substr(rec.accts_pay_code_concatenated,instr(rec.accts_pay_code_concatenated,'|',1,7)+1,instr(rec.accts_pay_code_concatenated,'|',1,8)-instr(rec.accts_pay_code_concatenated,'|',1,7)-1) 
				--,substr(rec.accts_pay_code_concatenated,instr(rec.accts_pay_code_concatenated,'|',1,8)+1,instr(rec.accts_pay_code_concatenated,'|',1,9)-instr(rec.accts_pay_code_concatenated,'|',1,8)-1) 
				--,substr(rec.accts_pay_code_concatenated,instr(rec.accts_pay_code_concatenated,'|',1,9)+1,instr(rec.accts_pay_code_concatenated,'|',1,9)-instr(rec.accts_pay_code_concatenated,'|',1,9)-1)
		INTO lv_seg1,lv_seg2,lv_seg3,lv_seg4,lv_seg5,lv_seg6
		--,lv_seg7,lv_seg8,lv_seg9,lv_seg10
		FROM dual;


        BEGIN
            -- Call the COA_TRANSFORMATION_PKG for each row
            xxcnv.xxcnv_gl_coa_transformation_pkg.coa_segment_mapping_prc(

                p_in_segment1           => lv_seg1,
                p_in_segment2           => lv_seg2,
                p_in_segment3           => lv_seg3,
                p_in_segment4           => lv_seg4,
                p_in_segment5           => lv_seg5,
                p_in_segment6           => lv_seg6,
                p_in_segment7           => lv_seg7,
                p_in_segment8           => lv_seg8,
                p_in_segment9           => lv_seg9,
                p_in_segment10          => lv_seg10,

                p_out_target_system     => lv_target_segment,
                p_out_status            => lv_status,
                p_out_message           => lv_message,
		p_in_pkg_name           => lv_pkg_name
            );

            DBMS_OUTPUT.PUT_LINE('Coa_segment_mapping_prc executed successfully');
            DBMS_OUTPUT.PUT_LINE('Target Segment: '||lv_target_segment);
            DBMS_OUTPUT.PUT_LINE('Status: '||lv_status);
            DBMS_OUTPUT.PUT_LINE('Message: '||lv_message);

            IF lv_status = 'SUCCESS' THEN

                DBMS_OUTPUT.PUT_LINE('Mapping Target Segments: '|| lv_target_segment);

                UPDATE xxcnv_ap_c005_ap_invoices_stg 
                SET
				accts_pay_code_concatenated = replace(lv_target_segment, '|','-')
                WHERE rowid = rec.identifier;

                DBMS_OUTPUT.PUT_LINE('Successfully transformed segments for record group_id: '|| rec.group_id);

            ELSE 

                DBMS_OUTPUT.PUT_LINE('Source segments are not valid values, so we cannot map the target segments');
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc
							(
							p_conversion_id 	=> gv_conversion_id,
							p_execution_id		=> gv_execution_id,
							p_execution_step 	=> gv_coa_transformation_failed,
							p_boundary_system 	=> gv_boundary_system,
							p_file_path 		=> gv_oci_file_path,
							p_file_name 		=> gv_file_name,
							P_attribute1        => NULL,
							P_attribute2        => lv_message,
							p_process_reference => NULL
							);

				--RETURN;
				update xxcnv_ap_c005_ap_invoices_stg
				set error_message = error_message||lv_message,
				file_reference_identifier = gv_execution_id||'_'||gv_status_failure
				WHERE rowid = rec.identifier;

				BEGIN
				UPDATE xxcnv_ap_c005_ap_invoices_stg
				SET import_status = CASE WHEN error_message IS NOT NULL THEN 'ERROR' ELSE 'PROCESSED'END
				WHERE rowid = rec.identifier;
				dbms_output.put_line('import_status is validated');
				END;

            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                LV_error_message :=  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace;
                DBMS_OUTPUT.PUT_LINE('Completed with error: '||LV_error_message);
                DBMS_OUTPUT.PUT_LINE('Error transforming segments for record group_id: '|| rec.group_id || '- '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
				RETURN;
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Completed mapping target segments');

	 xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
							p_conversion_id 	=> gv_conversion_id,
							p_execution_id		=> gv_execution_id,
							p_execution_step 	=> gv_coa_transformation,
							p_boundary_system 	=> gv_boundary_system,
							p_file_path 		=> gv_oci_file_path,
							p_file_name 		=> gv_file_name,
							P_attribute1        => NULL,
							P_attribute2       => NULL,
							p_process_reference => NULL
        );

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An unexpected error occurred in coa_target_segments_header_prc: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
        RETURN;
END coa_target_segments_header_prc;


	  END XXCNV_AP_C005_AP_INVOICES_CONVERSION_PKG;