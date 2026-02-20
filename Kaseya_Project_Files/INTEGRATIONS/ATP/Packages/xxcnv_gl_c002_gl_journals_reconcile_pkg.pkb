create or replace PACKAGE BODY xxcnv.xxcnv_gl_c002_gl_journals_reconcile_pkg
 IS
	/*************************************************************************************
    NAME              :     GL_RECONCILE_PKG BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
	-- Modification History
	-- Developer				Date				Version     Comments and changes made
	-- -------------			----------			----------  ----------------------------
	-- Chandra Mouli Gupta		11-Aug-2025  	    1.0         Initial Development
	****************************************************************************************/

---Declaring global Variables
    gv_oci_url                          VARCHAR2(20)    := '';
	gv_credential_name      CONSTANT 	VARCHAR2(25)	:= 'OCI$RESOURCE_PRINCIPAL';                
	gv_status_success       CONSTANT    VARCHAR2(15)    := 'Success';
	gv_status_failure       CONSTANT    VARCHAR2(15)    := 'Failure';
	gv_conversion_id                    VARCHAR2(15)    := NULL;
	gv_boundary_system	             	VARCHAR2(25)	:= NULL;
    gv_status_picked		CONSTANT 	VARCHAR2(100)	:= 'File_Picked_From_Oci_And_Loaded_To_Stg'; 
	gv_status_picked_for_tr	CONSTANT 	VARCHAR2(100)	:= 'Transformed_Data_From_Ext_To_Stg'; 
	gv_file_picked      	        	VARCHAR2(50)	:= 'File_Picked_From_OCI_Server'; 
	gv_created_by   CONSTANT VARCHAR2(50) := 'OIC_USER';
    gv_last_updated_by CONSTANT VARCHAR2(50) := 'OIC_USER';



/*=================================================================================================================
-- PROCEDURE : truncate_file_from_oci_prc
-- PARAMETERS: 
-- COMMENT   : This procedure is used to truncate source, transformed and reconciled staging tables from ATP.
===================================================================================================================*/
PROCEDURE truncate_file_from_oci_prc(
p_status OUT VARCHAR2,
p_message OUT VARCHAR2
) IS
BEGIN
            EXECUTE IMMEDIATE 'DELETE XXCNV_GL_C002_GL_SOURCE_INTERFACE_STG'; 
            EXECUTE IMMEDIATE 'DELETE XXCNV_GL_C002_GL_TRANSFORMED_INTERFACE_STG'; 
            EXECUTE IMMEDIATE 'DELETE XXCNV_GL_C002_GL_RECONCILED_INTERFACE_STG';
     COMMIT;
 p_status:=gv_status_success;
EXCEPTION
WHEN OTHERS THEN
        dbms_output.put_line('Error in truncating stage tables: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
			p_status := gv_status_failure;
            p_message := 'Error in truncating stage tables: ->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace;
            RETURN;
END truncate_file_from_oci_prc;



/*=================================================================================================================
-- PROCEDURE : import_source_file_from_oci_to_stg_prc
-- PARAMETERS: p_src_file_path, p_src_file_name
-- COMMENT   : This procedure is used to create an external table and transfer that data from external to stg table.
===================================================================================================================*/

PROCEDURE import_source_file_from_oci_to_stg_prc (
    p_src_file_path IN VARCHAR2,
    p_src_file_name IN VARCHAR2,
    p_status OUT VARCHAR2,
    p_message OUT VARCHAR2
) IS

    lv_table_count NUMBER := 0;
    lv_oci_file_path VARCHAR2(2000) := p_src_file_path;
	lv_oci_file_name VARCHAR2(200) := p_src_file_name;
	lv_source_external_table       CONSTANT    VARCHAR2(200)    := 'xxcnv_gl_c002_gl_journals_source_ext';


BEGIN

    BEGIN
        -- Check if the external table exists and drop it if it does
        SELECT COUNT(*)
        INTO lv_table_count
        FROM all_objects
        WHERE lower(object_name) = lv_source_external_table
        AND object_type = 'TABLE';

        IF lv_table_count > 0 THEN
            EXECUTE IMMEDIATE 'DROP TABLE xxcnv_gl_c002_gl_journals_source_ext';
            COMMIT;
            --EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_GL_C002_GL_source_INTERFACE_STG'; 

            dbms_output.put_line('Table xxcnv_gl_c002_gl_journals_source_ext dropped');
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            p_status := gv_status_failure;
            p_message := 'Error dropping table xxcnv_gl_c002_gl_journals_source_ext: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace;
            dbms_output.put_line('Error dropping table xxcnv_gl_c002_gl_journals_source_ext: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
			RETURN;
    END;



    BEGIN
        dbms_output.put_line('Creating an external table:'|| lv_oci_file_path||lv_oci_file_name );

        -- Create the external table
            lv_oci_file_name:= p_src_file_name;
            /*
                for the curent file, check the name if it contains/exists function NONE/TypeA/TypeB
                based on that we create external table within switch/if
            */

            DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
                table_name => 'xxcnv_gl_c002_gl_journals_source_ext',
                credential_name => 'OCI$RESOURCE_PRINCIPAL',
                file_uri_list   =>  lv_oci_file_path||lv_oci_file_name,--path contains the URL as well
                format => json_object('skipheaders'VALUE '1','type'VALUE 'csv','rejectlimit'value 'UNLIMITED','dateformat' VALUE 'yyyy/mm/dd','ignoremissingcolumns'value 'true','blankasnull'value 'true'), 
                column_list => 
						'status VARCHAR2(500),
						ledger_id VARCHAR2(500),
						effective_date_of_transaction VARCHAR2(500),
						user_je_source_name VARCHAR2(500),
						user_je_category_name VARCHAR2(500),
						currency_code VARCHAR2(500),
						date_created VARCHAR2(500),
						actual_flag VARCHAR2(500),
						segment1 VARCHAR2(500),
						segment2 VARCHAR2(500),
						segment3 VARCHAR2(500),
						segment4 VARCHAR2(500),
						segment5 VARCHAR2(500),
						segment6 VARCHAR2(500),
						segment7 VARCHAR2(500),
						segment8 VARCHAR2(500),
						segment9 VARCHAR2(500),
						segment10 VARCHAR2(500),
						segment11 VARCHAR2(500),
						segment12 VARCHAR2(500),
						segment13 VARCHAR2(500),
						segment14 VARCHAR2(500),
						segment15 VARCHAR2(500),
						segment16 VARCHAR2(500),
						segment17 VARCHAR2(500),
						segment18 VARCHAR2(500),
						segment19 VARCHAR2(500),
						segment20 VARCHAR2(500),
						segment21 VARCHAR2(500),
						segment22 VARCHAR2(500),
						segment23 VARCHAR2(500),
						segment24 VARCHAR2(500),
						segment25 VARCHAR2(500),
						segment26 VARCHAR2(500),
						segment27 VARCHAR2(500),
						segment28 VARCHAR2(500),
						segment29 VARCHAR2(500),
						segment30 VARCHAR2(500),
						entered_dr VARCHAR2(500),
						entered_cr VARCHAR2(500),
						accounted_dr VARCHAR2(500),
						accounted_cr VARCHAR2(500),
						reference1 VARCHAR2(500),
						reference2 VARCHAR2(500),
						reference3 VARCHAR2(500),
						reference4 VARCHAR2(500),
						reference5 VARCHAR2(500),
						reference6 VARCHAR2(500),
						reference7 VARCHAR2(500),
						reference8 VARCHAR2(500),
						reference9 VARCHAR2(500),
						reference10 VARCHAR2(500),
						reference21 VARCHAR2(500),
						reference22 VARCHAR2(500),
						reference23 VARCHAR2(500),
						reference24 VARCHAR2(500),
						reference25 VARCHAR2(500),
						reference26 VARCHAR2(500),
						reference27 VARCHAR2(500),
						reference28 VARCHAR2(500),
						reference29 VARCHAR2(500),
						reference30 VARCHAR2(500),
						stat_amount VARCHAR2(500),
						user_currency_conversion_type VARCHAR2(500),
						currency_conversion_date VARCHAR2(500),
						currency_conversion_rate VARCHAR2(500),
						group_id VARCHAR2(500),
						attribute_category VARCHAR2(500),
						attribute1 VARCHAR2(500),
						attribute2 VARCHAR2(500),
						attribute3 VARCHAR2(500),
						attribute4 VARCHAR2(500),
						attribute5 VARCHAR2(500),
						attribute6 VARCHAR2(500),
						attribute7 VARCHAR2(500),
						attribute8 VARCHAR2(500),
						attribute9 VARCHAR2(500),
						attribute10 VARCHAR2(500),
						attribute11 VARCHAR2(500),
						attribute12 VARCHAR2(500),
						attribute13 VARCHAR2(500),
						attribute14 VARCHAR2(500),
						attribute15 VARCHAR2(500),
						attribute16 VARCHAR2(500),
						attribute17 VARCHAR2(500),
						attribute18 VARCHAR2(500),
						attribute19 VARCHAR2(500),
						attribute20 VARCHAR2(500),
						attribute_category3 VARCHAR2(500),
						average_journal_flag VARCHAR2(500),
						originating_bal_seg_value VARCHAR2(500),
						ledger_name VARCHAR2(500),
						encumbrance_type_id VARCHAR2(500),				
						jgzz_recon_ref VARCHAR2(500),
						period_name VARCHAR2(500),
						reference18 VARCHAR2(500),
						reference19 VARCHAR2(500),
						reference20 VARCHAR2(500),
						attribute_date1 VARCHAR2(500),
						attribute_date2 VARCHAR2(500),
						attribute_date3 VARCHAR2(500),
						attribute_date4 VARCHAR2(500),
						attribute_date5 VARCHAR2(500),
						attribute_date6 VARCHAR2(500),
						attribute_date7 VARCHAR2(500),
						attribute_date8 VARCHAR2(500),
						attribute_date9 VARCHAR2(500),
						attribute_date10 VARCHAR2(500),
						attribute_number1 VARCHAR2(500),
						attribute_number2 VARCHAR2(500),
						attribute_number3 VARCHAR2(500),
						attribute_number4 VARCHAR2(500),
						attribute_number5 VARCHAR2(500),
						attribute_number6 VARCHAR2(500),
						attribute_number7 VARCHAR2(500),
						attribute_number8 VARCHAR2(500),
						attribute_number9 VARCHAR2(500),
						attribute_number10 VARCHAR2(500)'
                    );




   			EXECUTE IMMEDIATE 'INSERT INTO XXCNV_GL_C002_GL_SOURCE_INTERFACE_STG (
						status,                        
						ledger_id, 
						effective_date_of_transaction,
						user_je_source_name, 
						user_je_category_name,
						currency_code,
						date_created,
						actual_flag,
						segment_1,                  
						segment_2,
						segment_3,
						segment_4,
						segment_5,
						segment_6,
						segment_7,
						segment_8,
						segment_9,
						segment_10, 
						segment_11, 
						segment_12,
						segment_13,
						segment_14,
						segment_15,
						segment_16,
						segment_17,
						segment_18,
						segment_19,
						segment_20,
						segment_21,
						segment_22,
						segment_23,
						segment_24,
						segment_25,
						segment_26,
						segment_27,
						segment_28,
						segment_29,
						segment_30,
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
						attribute_number10

					) SELECT 
						status,                        
						ledger_id, 
						effective_date_of_transaction,
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
						attribute_number10

						FROM xxcnv_gl_c002_gl_journals_source_ext';
        COMMIT;
        dbms_output.put_line('Inserted records in the XXCNV_GL_C002_GL_SOURCE_INTERFACE_STG from OCI Source Folder: '||SQL%ROWCOUNT);
    p_status := gv_status_success;

    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error in load_staging_table: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
			p_status := gv_status_Failure;
            p_message := 'Error in load_staging_table: ->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace;
            RETURN;
    END;


END import_source_file_from_oci_to_stg_prc;




/*=================================================================================================================
-- PROCEDURE : import_transformed_file_from_oci_to_stg_prc
-- PARAMETERS: p_trans_file_path, p_trans_file_name
-- COMMENT   : This procedure is used to create an external table and transfer that data from external to stg table.
===================================================================================================================*/

PROCEDURE import_transformed_file_from_oci_to_stg_prc (
    p_trans_file_path IN VARCHAR2,
    p_trans_file_name IN VARCHAR2,
    p_status OUT VARCHAR2,
    p_message OUT VARCHAR2
) IS

    lv_table_count NUMBER := 0;
    lv_oci_file_path VARCHAR2(2000) := p_trans_file_path;
	lv_oci_file_name VARCHAR2(200) := p_trans_file_name;
	lv_transformed_external_table       CONSTANT    VARCHAR2(200)    := 'xxcnv_gl_c002_gl_journals_transformed_ext';


BEGIN

    BEGIN
        -- Check if the external table exists and drop it if it does
        SELECT COUNT(*)
        INTO lv_table_count
        FROM all_objects
        WHERE lower(object_name) = lv_transformed_external_table
        AND object_type = 'TABLE';

        IF lv_table_count > 0 THEN
            EXECUTE IMMEDIATE 'DROP TABLE xxcnv_gl_c002_gl_journals_transformed_ext';
            --EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_GL_C002_GL_TRANSFORMED_INTERFACE_STG'; 

            dbms_output.put_line('Table xxcnv_gl_c002_gl_journals_transformed_ext dropped');
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error dropping table xxcnv_gl_c002_gl_journals_transformed_ext: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
			RETURN;
    END;



    BEGIN
        dbms_output.put_line('Creating an external table:'|| lv_oci_file_path||lv_oci_file_name );

        -- Create the external table
            lv_oci_file_name:= p_trans_file_name;

            DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
                table_name => 'xxcnv_gl_c002_gl_journals_transformed_ext',
                credential_name => 'OCI$RESOURCE_PRINCIPAL',
                file_uri_list   =>  lv_oci_file_path||lv_oci_file_name,
                format => json_object('skipheaders'VALUE '0','type'VALUE 'csv','rejectlimit'value 'UNLIMITED','dateformat' VALUE 'yyyy/mm/dd','ignoremissingcolumns'value 'true','blankasnull'value 'true'), 
                column_list => 
						'status VARCHAR2(500),
						ledger_id VARCHAR2(500),
						accounting_date VARCHAR2(500),
						user_je_source_name VARCHAR2(500),
						user_je_category_name VARCHAR2(500),
						currency_code VARCHAR2(500),
						date_created VARCHAR2(500),
						actual_flag VARCHAR2(500),
						segment1 VARCHAR2(500),
						segment2 VARCHAR2(500),
						segment3 VARCHAR2(500),
						segment4 VARCHAR2(500),
						segment5 VARCHAR2(500),
						segment6 VARCHAR2(500),
						segment7 VARCHAR2(500),
						segment8 VARCHAR2(500),
						segment9 VARCHAR2(500),
						segment10 VARCHAR2(500),
						segment11 VARCHAR2(500),
						segment12 VARCHAR2(500),
						segment13 VARCHAR2(500),
						segment14 VARCHAR2(500),
						segment15 VARCHAR2(500),
						segment16 VARCHAR2(500),
						segment17 VARCHAR2(500),
						segment18 VARCHAR2(500),
						segment19 VARCHAR2(500),
						segment20 VARCHAR2(500),
						segment21 VARCHAR2(500),
						segment22 VARCHAR2(500),
						segment23 VARCHAR2(500),
						segment24 VARCHAR2(500),
						segment25 VARCHAR2(500),
						segment26 VARCHAR2(500),
						segment27 VARCHAR2(500),
						segment28 VARCHAR2(500),
						segment29 VARCHAR2(500),
						segment30 VARCHAR2(500),
						entered_dr VARCHAR2(500),
						entered_cr VARCHAR2(500),
						accounted_dr VARCHAR2(500),
						accounted_cr VARCHAR2(500),
						reference1 VARCHAR2(500),
						reference2 VARCHAR2(500),
						reference3 VARCHAR2(500),
						reference4 VARCHAR2(500),
						reference5 VARCHAR2(500),
						reference6 VARCHAR2(500),
						reference7 VARCHAR2(500),
						reference8 VARCHAR2(500),
						reference9 VARCHAR2(500),
						reference10 VARCHAR2(500),
						reference21 VARCHAR2(500),
						reference22 VARCHAR2(500),
						reference23 VARCHAR2(500),
						reference24 VARCHAR2(500),
						reference25 VARCHAR2(500),
						reference26 VARCHAR2(500),
						reference27 VARCHAR2(500),
						reference28 VARCHAR2(500),
						reference29 VARCHAR2(500),
						reference30 VARCHAR2(500),
						stat_amount VARCHAR2(500),
						user_currency_conversion_type VARCHAR2(500),
						currency_conversion_date VARCHAR2(500),
						currency_conversion_rate VARCHAR2(500),
						group_id VARCHAR2(500),
						attribute_category VARCHAR2(500),
						attribute1 VARCHAR2(500),
						attribute2 VARCHAR2(500),
						attribute3 VARCHAR2(500),
						attribute4 VARCHAR2(500),
						attribute5 VARCHAR2(500),
						attribute6 VARCHAR2(500),
						attribute7 VARCHAR2(500),
						attribute8 VARCHAR2(500),
						attribute9 VARCHAR2(500),
						attribute10 VARCHAR2(500),
						attribute11 VARCHAR2(500),
						attribute12 VARCHAR2(500),
						attribute13 VARCHAR2(500),
						attribute14 VARCHAR2(500),
						attribute15 VARCHAR2(500),
						attribute16 VARCHAR2(500),
						attribute17 VARCHAR2(500),
						attribute18 VARCHAR2(500),
						attribute19 VARCHAR2(500),
						attribute20 VARCHAR2(500),
						attribute_category3 VARCHAR2(500),
						average_journal_flag VARCHAR2(500),
						originating_bal_seg_value VARCHAR2(500),
						ledger_name VARCHAR2(500),
						encumbrance_type_id VARCHAR2(500),				
						jgzz_recon_ref VARCHAR2(500),
						period_name VARCHAR2(500),
						reference18 VARCHAR2(500),
						reference19 VARCHAR2(500),
						reference20 VARCHAR2(500),
						attribute_date1 VARCHAR2(500),
						attribute_date2 VARCHAR2(500),
						attribute_date3 VARCHAR2(500),
						attribute_date4 VARCHAR2(500),
						attribute_date5 VARCHAR2(500),
						attribute_date6 VARCHAR2(500),
						attribute_date7 VARCHAR2(500),
						attribute_date8 VARCHAR2(500),
						attribute_date9 VARCHAR2(500),
						attribute_date10 VARCHAR2(500),
						attribute_number1 VARCHAR2(500),
						attribute_number2 VARCHAR2(500),
						attribute_number3 VARCHAR2(500),
						attribute_number4 VARCHAR2(500),
						attribute_number5 VARCHAR2(500),
						attribute_number6 VARCHAR2(500),
						attribute_number7 VARCHAR2(500),
						attribute_number8 VARCHAR2(500),
						attribute_number9 VARCHAR2(500),
						attribute_number10 VARCHAR2(500),
						global_attribute_category VARCHAR2(500),
						global_attribute1 VARCHAR2(500),
						global_attribute2 VARCHAR2(500),
						global_attribute3 VARCHAR2(500),
						global_attribute4 VARCHAR2(500),
						global_attribute5 VARCHAR2(500),
						global_attribute6 VARCHAR2(500),
						global_attribute7 VARCHAR2(500),
						global_attribute8 VARCHAR2(500),
						global_attribute9 VARCHAR2(500),
						global_attribute10 VARCHAR2(500),
						global_attribute11 VARCHAR2(500),
						global_attribute12 VARCHAR2(500),
						global_attribute13 VARCHAR2(500),
						global_attribute14 VARCHAR2(500),
						global_attribute15 VARCHAR2(500),
						global_attribute16 VARCHAR2(500),
						global_attribute17 VARCHAR2(500),
						global_attribute18 VARCHAR2(500),
						global_attribute19 VARCHAR2(500),
						global_attribute20 VARCHAR2(500),
						global_attribute_date1 VARCHAR2(500),
						global_attribute_date2 VARCHAR2(500),
						global_attribute_date3 VARCHAR2(500),
						global_attribute_date4 VARCHAR2(500),
						global_attribute_date5 VARCHAR2(500),
						global_attribute_number1 VARCHAR2(500),
						global_attribute_number2 VARCHAR2(500),
						global_attribute_number3 VARCHAR2(500),
						global_attribute_number4 VARCHAR2(500),
						global_attribute_number5 VARCHAR2(500)'

		);

   			EXECUTE IMMEDIATE  'INSERT INTO XXCNV_GL_C002_GL_TRANSFORMED_INTERFACE_STG (
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
						global_attribute_number5
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
						global_attribute_number5
						FROM xxcnv_gl_c002_gl_journals_transformed_ext';

        dbms_output.put_line('Inserted records in the XXCNV_GL_C002_GL_TRANSFORMED_INTERFACE_STG from OCI Source Folder: '||SQL%ROWCOUNT);
    p_status := gv_status_success;

    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error in load_staging_table: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
			p_status := gv_status_Failure;
            p_message := 'Error in load_staging_table: ->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace;
            RETURN;
    END;


END import_transformed_file_from_oci_to_stg_prc;



/*=================================================================================================================
-- PROCEDURE : PROCEDURE import_reconciled_file_from_oci_to_stg_prc
-- PARAMETERS: p_recon_file_path, p_recon_file_name, p_oic_instance_id
-- COMMENT   : This procedure is used to create an external table and transfer that data from external to stg table.
===================================================================================================================*/

PROCEDURE import_reconciled_file_from_oci_to_stg_prc (
    p_recon_file_path IN varchar2,
    p_recon_file_name IN varchar2,
    p_status OUT VARCHAR2,
    p_message OUT VARCHAR2
) IS

    --lv_oic_instance_id VARCHAR2(200) := p_oic_instance_id;
    lv_table_count NUMBER := 0;
    lv_oci_file_path VARCHAR2(2000) := p_recon_file_path;
	lv_oci_file_name VARCHAR2(200) := p_recon_file_name;
	lv_reconciled_external_table       CONSTANT    VARCHAR2(200)    := 'xxcnv_gl_c002_gl_journals_reconciled_ext';


BEGIN

    BEGIN
        -- Check if the external table exists and drop it if it does
        SELECT COUNT(*)
        INTO lv_table_count
        FROM all_objects
        WHERE lower(object_name) = lv_reconciled_external_table
        AND object_type = 'TABLE';

        IF lv_table_count > 0 THEN
            EXECUTE IMMEDIATE 'DROP TABLE xxcnv_gl_c002_gl_journals_reconciled_ext';
            --EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_GL_C002_GL_RECONCILED_INTERFACE_STG'; 

            dbms_output.put_line('Table xxcnv_gl_c002_gl_journals_reconciled_ext dropped');
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error dropping table xxcnv_gl_c002_gl_journals_reconciled_ext: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
			RETURN;
    END;

    BEGIN
        dbms_output.put_line('Creating an external table:'|| lv_oci_file_path||lv_oci_file_name );

        -- Create the external table
        lv_oci_file_name:= p_recon_file_name;

        DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
            table_name => 'xxcnv_gl_c002_gl_journals_reconciled_ext',
            credential_name => 'OCI$RESOURCE_PRINCIPAL',
            file_uri_list   =>  lv_oci_file_path||lv_oci_file_name,
            format => json_object('skipheaders'VALUE '1','type'VALUE 'csv','rejectlimit'value 'UNLIMITED','dateformat' VALUE 'yyyy/mm/dd','ignoremissingcolumns'value 'true','blankasnull'value 'true'), 
            column_list => 
						'ledger VARCHAR2(500),
                        user_je_source_name VARCHAR2(500),
                        user_je_category_name VARCHAR2(500),
                        actual_flag VARCHAR2(500),
                        creation_date VARCHAR2(500),
                        effective_date VARCHAR2(500),
                        batch_name VARCHAR2(500),
                        batch_description VARCHAR2(500),
                        journal_name VARCHAR2(500),
                        header_description VARCHAR2(500),
                        segment1 VARCHAR2(500),
                        segment2 VARCHAR2(500),
                        segment3 VARCHAR2(500),
                        segment4 VARCHAR2(500),
                        segment5 VARCHAR2(500),
                        segment6 VARCHAR2(500),
                        segment7 VARCHAR2(500),
                        segment8 VARCHAR2(500),
                        segment9 VARCHAR2(500),
                        segment10 VARCHAR2(500),
                        currency_code VARCHAR2(500),
                        entered_dr VARCHAR2(500),
                        entered_cr VARCHAR2(500),
                        accounted_dr VARCHAR2(500),
                        accounted_cr VARCHAR2(500),
                        status VARCHAR2(500),
                        group_id VARCHAR2(500),
                        attribute1 VARCHAR2(500),
                        attribute2 VARCHAR2(500),
                        attribute3 VARCHAR2(500),
                        attribute4 VARCHAR2(500),
                        attribute5 VARCHAR2(500),
                        attribute6 VARCHAR2(500),
                        attribute7 VARCHAR2(500),
                        attribute8 VARCHAR2(500),
                        attribute9 VARCHAR2(500),
                        attribute10 VARCHAR2(500),
                        currency_conversion_rate VARCHAR2(500),
                        description VARCHAR2(500),
                        period_name VARCHAR2(500),
                        key VARCHAR2(500)'
		);

        EXECUTE IMMEDIATE 'INSERT INTO XXCNV_GL_C002_GL_RECONCILED_INTERFACE_STG (
                    ledger,
                    user_je_source_name,
                    user_je_category_name,
                    actual_flag,
                    creation_date,
                    effective_date,
                    batch_name,
                    batch_description,
                    journal_name,
                    header_description,
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
                    currency_code,
                    entered_dr,
                    entered_cr,
                    accounted_dr,
                    accounted_cr,
                    status,
                    group_id,
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
                    currency_conversion_rate,
                    description,
                    period_name,
                    key
                    ) SELECT 
						ledger,
                        user_je_source_name,
                        user_je_category_name,
                        actual_flag,
                        creation_date,
                        effective_date,
                        batch_name,
                        batch_description,
                        journal_name,
                        header_description,
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
                        currency_code,
                        nvl(entered_dr,null),
                        entered_cr,
                        nvl(accounted_dr,null),
                        accounted_cr,
                        status,
                        group_id,
                        nvl(attribute1,null),
                        attribute2,
                        attribute3,
                        attribute4,
                        attribute5,
                        attribute6,
                        attribute7,
                        attribute8,
                        attribute9,
                        attribute10,
                        currency_conversion_rate,
                        description,
                        period_name,
                        key
						FROM xxcnv_gl_c002_gl_journals_reconciled_ext';

        dbms_output.put_line('Inserted records in the XXCNV_GL_C002_GL_RECONCILED_INTERFACE_STG from OCI Source Folder: '||SQL%ROWCOUNT);
        p_status := gv_status_success;

    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error in load_staging_table: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
			p_status := gv_status_Failure;
            p_message := 'Error in load_staging_table: ->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace;
            RETURN;
    END;



END import_reconciled_file_from_oci_to_stg_prc;




/*=================================================================================================================
-- PROCEDURE : compare_source_transformed_recon_file_prc
-- PARAMETERS: p_oicInstanceId, p_transFileName, p_reconFileName
-- COMMENT   : This procedure is used to compare details of source file, transformed file and recon report.
===================================================================================================================*/

PROCEDURE compare_source_transformed_recon_file_prc(
    p_oicInstanceId IN VARCHAR2,
    p_conversionId IN VARCHAR2,
    p_iterationNumber IN VARCHAR2,
    p_status OUT VARCHAR2,
    p_message OUT VARCHAR2
)    
IS

	--Declaring local variables

    lv_oic_instance_id VARCHAR2(200) := p_oicInstanceId;
    lv_conversion_id VARCHAR2(200) := p_conversionId;
    lv_iteration_number VARCHAR2(200) := p_iterationNumber;
    lv_ledger_name VARCHAR2(200);
    lv_category_name VARCHAR2(200);
    lv_subsidiary_name VARCHAR2(200);
    lv_batch_name VARCHAR2(200);
    lv_status VARCHAR2(10) := '';
    lv_error_message VARCHAR2(4000);
    lv_s_line_count NUMBER :=0;
	lv_s_journal_count NUMBER :=0;
	lv_s_sum_entered_cr NUMBER :=0;
	lv_s_sum_entered_dr NUMBER :=0;
	lv_s_sum_acct_cr NUMBER :=0;
    lv_s_sum_acct_dr NUMBER :=0;
    lv_t_line_count NUMBER :=0;
	lv_t_journal_count NUMBER :=0;
	lv_t_sum_entered_cr number :=0;
	lv_t_sum_entered_dr number :=0;
	lv_t_sum_acct_cr NUMBER :=0;
	lv_t_sum_acct_dr NUMBER :=0;
	lv_r_line_count NUMBER :=0;
	lv_r_journal_count NUMBER :=0;
	lv_r_sum_entered_cr NUMBER :=0;
	lv_r_sum_entered_dr NUMBER :=0;
	lv_r_sum_acct_cr NUMBER :=0;
	lv_r_sum_acct_dr NUMBER :=0;



    cursor batch_cursor is
        --select distinct reference4,period_name from XXCNV_GL_C002_GL_SOURCE_INTERFACE_STG;
           select distinct reference2,period_name from XXCNV_GL_C002_GL_TRANSFORMED_INTERFACE_STG;
BEGIN

    BEGIN
      FOR i in batch_cursor LOOP
      --dbms_output.put_line(i.reference4||' '||i.period_name);
        -- for source file
        BEGIN
        select 
			NVL(count(1),0),--count of journal lines
			count(DISTINCT reference4), --count of journal names
			NVL(SUM(TO_NUMBER(entered_cr)),0),--sum of entered cr
			NVL(SUM(TO_NUMBER(entered_dr)),0),--sum of entered dr
			NVL(SUM(TO_NUMBER(accounted_cr)),0),--sum of accounted cr
			NVL(SUM(TO_NUMBER(accounted_dr)),0)--sum of accounted dr
		into
			lv_s_line_count,
			lv_s_journal_count,
			lv_s_sum_entered_cr,
			lv_s_sum_entered_dr,
			lv_s_sum_acct_cr,
			lv_s_sum_acct_dr
		from XXCNV_GL_C002_GL_SOURCE_INTERFACE_STG
        where upper(reference2)=upper(i.reference2) --subsidiary basis
        and upper(period_name)=upper(i.period_name);

		EXCEPTION
		WHEN NO_DATA_FOUND then
		DBMS_OUTPUT.PUT_LINE('Not able to fetch the source amount fields for the period name '||i.period_name||' and batch name: '||i.reference2);
		END;

             -- dbms_output.put_line('Fetched the source amount fields');

        --fetch value which are common for file wise comparison
		/*
		BEGIN 
        select
            user_je_category_name,
            reference2--subsidiary name
        into
            lv_category_name,
            lv_subsidiary_name
        from  XXCNV_GL_C002_GL_SOURCE_INTERFACE_STG
        where upper(reference2)=upper(i.reference2) --subsidiary basis
        and upper(period_name)=upper(i.period_name);
        and rownum=1;

            --  dbms_output.put_line('Fetched the ledger and category fields');

		EXCEPTION
		WHEN NO_DATA_FOUND then
		DBMS_OUTPUT.PUT_LINE('Not able to fetch the fields for the period name '||i.period_name||' and journal name: '||i.reference4);
		END;
		*/

		BEGIN
        Select
        r.ledger,
        r.batch_name
        into 
        lv_ledger_name,
        lv_batch_name
        from XXCNV_GL_C002_GL_RECONCILED_INTERFACE_STG r
        where upper(batch_description)=upper(i.reference2) --subsidiary basis
        and   upper(period_name)=upper(i.period_name)
		and rownum =1;
             -- dbms_output.put_line('Fetched the batch_name field');

		EXCEPTION
		WHEN NO_DATA_FOUND then
		DBMS_OUTPUT.PUT_LINE('The batch name is not able to fetch for the period name '||i.period_name||' and batch name: '||i.reference2);
		END;


        -- for transformed file
        select 
			NVL(count(1),0),
			count(DISTINCT reference4),
			NVL(SUM(TO_NUMBER(entered_cr)),0),
			NVL(SUM(TO_NUMBER(entered_dr)),0),
			NVL(SUM(TO_NUMBER(accounted_cr)),0),
			NVL(SUM(TO_NUMBER(accounted_dr)),0)
		into
			lv_t_line_count,
			lv_t_journal_count,
			lv_t_sum_entered_cr,
			lv_t_sum_entered_dr,
			lv_t_sum_acct_cr,
			lv_t_sum_acct_dr
		from XXCNV_GL_C002_GL_TRANSFORMED_INTERFACE_STG
        where upper(reference2)=upper(i.reference2) --subsidiary basis
        and upper(period_name)=upper(i.period_name);

             -- dbms_output.put_line('Fetched the transformed amount fields');

        -- for recon report file
        select 
			NVL(count(1),0),
			count(DISTINCT r.journal_name),
			NVL(SUM(TO_NUMBER(r.entered_cr)),0),
			NVL(SUM(TO_NUMBER(r.entered_dr)),0),
			NVL(SUM(TO_NUMBER(r.accounted_cr)),0),
			NVL(SUM(TO_NUMBER(r.accounted_dr)),0)
		into
			lv_r_line_count,
			lv_r_journal_count,
			lv_r_sum_entered_cr,
			lv_r_sum_entered_dr,
			lv_r_sum_acct_cr,
			lv_r_sum_acct_dr
		from XXCNV_GL_C002_GL_RECONCILED_INTERFACE_STG r
        where upper(batch_description)=upper(i.reference2) --subsidiary basis
        and upper(period_name)=upper(i.period_name);

              --dbms_output.put_line('Fetched the recon amount fields');

        --check status based on the comparison criteria
   /*     lv_status := CASE
					WHEN	(lv_s_line_count = lv_t_line_count and lv_t_line_count = lv_r_line_count) AND 
							(lv_s_journal_count = lv_t_journal_count and lv_t_journal_count = lv_r_journal_count) AND
							(lv_s_sum_entered_cr = lv_t_sum_entered_cr and lv_t_sum_entered_cr = lv_r_sum_entered_cr) AND
							(lv_s_sum_entered_dr = lv_t_sum_entered_dr and lv_t_sum_entered_dr = lv_r_sum_entered_dr)AND
							(lv_s_sum_acct_cr = lv_t_sum_acct_cr and lv_t_sum_acct_cr = lv_r_sum_acct_cr) AND
							(lv_t_sum_acct_dr = lv_t_sum_acct_dr and lv_t_sum_acct_dr = lv_r_sum_acct_dr)
						THEN gv_status_success
					ELSE gv_status_failure
             END;*/
       lv_error_message := 
    CASE WHEN lv_s_line_count != lv_t_line_count OR lv_t_line_count != lv_r_line_count
         THEN 'Line count mismatch; ' ELSE '' END ||
    CASE WHEN lv_s_journal_count != lv_t_journal_count OR lv_t_journal_count != lv_r_journal_count
         THEN 'Journal count mismatch; ' ELSE '' END ||
    CASE WHEN lv_s_sum_entered_cr != lv_t_sum_entered_cr OR lv_t_sum_entered_cr != lv_r_sum_entered_cr
         THEN 'Sum entered credit mismatch; ' ELSE '' END ||
    CASE WHEN lv_s_sum_entered_dr != lv_t_sum_entered_dr OR lv_t_sum_entered_dr != lv_r_sum_entered_dr
         THEN 'Sum entered debit mismatch; ' ELSE '' END ||
    CASE WHEN lv_s_sum_acct_cr != lv_t_sum_acct_cr OR lv_t_sum_acct_cr != lv_r_sum_acct_cr
         THEN 'Sum account credit mismatch; ' ELSE '' END ||
    CASE WHEN lv_s_sum_acct_dr != lv_t_sum_acct_dr OR lv_t_sum_acct_dr != lv_r_sum_acct_dr
         THEN 'Sum account debit mismatch; ' ELSE '' END;

lv_status := CASE 
                WHEN lv_error_message IS NULL OR lv_error_message = ''
                THEN gv_status_success
                ELSE gv_status_failure
             END;


        INSERT INTO XXCNV_GL_C002_GL_RECONCILE_STATUS_INTERFACE_STG VALUES(
            lv_oic_instance_id,
            lv_conversion_id,
            lv_iteration_number,
            NULL,
            i.period_name,
            lv_ledger_name,
            NULL,
            i.reference2,
            lv_batch_name,
            lv_s_line_count,
			lv_s_journal_count,
			lv_s_sum_entered_cr,
			lv_s_sum_entered_dr,
			lv_s_sum_acct_cr,
			lv_s_sum_acct_dr,
            lv_t_line_count,
			lv_t_journal_count,
			lv_t_sum_entered_cr,
			lv_t_sum_entered_dr,
			lv_t_sum_acct_cr,
			lv_t_sum_acct_dr,
            lv_r_line_count,
			lv_r_journal_count,
			lv_r_sum_entered_cr,
			lv_r_sum_entered_dr,
			lv_r_sum_acct_cr,
			lv_r_sum_acct_dr,
            lv_status,
            lv_error_message,
            gv_created_by,
            to_CHAR(sysdate,'DD-MM-YYYY HH24:MI:SS'),
            gv_last_updated_by,
            to_CHAR(sysdate,'DD-MM-YYYY HH24:MI:SS')
			);

            --  dbms_output.put_line('Inserted the data in matching table');

    END LOOP;  

        p_status := gv_status_success;

    EXCEPTION
    WHEN OTHERS THEN 
	ROLLBACK;
        dbms_output.put_line('An error occurred while reconciling journal status: '|| '->'|| SUBSTR(SQLERRM, 1, 3000) || '->'|| DBMS_UTILITY.format_error_backtrace);
        p_status := gv_status_Failure;
        p_message := 'An error occurred while reconciling journal status: ->'|| SUBSTR(SQLERRM, 1, 3000) || '->'|| DBMS_UTILITY.format_error_backtrace;
    END; 

END compare_source_transformed_recon_file_prc;



/*=================================================================================================================
-- PROCEDURE : compare_transformed_recon_file_prc
-- PARAMETERS: p_oicInstanceId, p_transFileName, p_reconFileName
-- COMMENT   : This procedure is used to compare details of transformed file and recon report.
===================================================================================================================*/

PROCEDURE compare_transformed_recon_file_prc(
    p_oicInstanceId IN VARCHAR2,
    p_conversionId IN VARCHAR2,
    p_iterationNumber IN VARCHAR2,
    p_status OUT VARCHAR2,
    p_message OUT VARCHAR2
)    
IS

	--Declaring local variables

    lv_oic_instance_id VARCHAR2(200) := p_oicInstanceId;
    lv_conversion_id VARCHAR2(200) := p_conversionId;
    lv_iteration_number VARCHAR2(200) := p_iterationNumber;
    lv_ledger_name VARCHAR2(200);
    lv_category_name VARCHAR2(200);
    lv_subsidiary_name VARCHAR2(200);
    lv_batch_name   VARCHAR2(200);
    lv_error_message VARCHAR2(600);
    lv_t_line_count NUMBER :=0;
	lv_t_journal_count NUMBER :=0;
	lv_t_sum_entered_cr number :=0;
	lv_t_sum_entered_dr number :=0;
	lv_t_sum_acct_cr NUMBER :=0;
	lv_t_sum_acct_dr NUMBER :=0;
	lv_r_line_count NUMBER :=0;
	lv_r_journal_count NUMBER :=0;
	lv_r_sum_entered_cr NUMBER :=0;
	lv_r_sum_entered_dr NUMBER :=0;
	lv_r_sum_acct_cr NUMBER :=0; 
	lv_r_sum_acct_dr NUMBER :=0;
	lv_status VARCHAR2(10) := '';

    cursor journal_cursor is
        select distinct reference4,period_name,segment1 from XXCNV_GL_C002_GL_TRANSFORMED_INTERFACE_STG;

BEGIN

    BEGIN
      FOR i in journal_cursor LOOP
      dbms_output.put_line(i.reference4||' '||i.period_name);

        --fetch value which are common for file wise comparison
        select
            ledger_name,
            user_je_category_name,
            reference2--subsidiary name
        into
            lv_ledger_name,
            lv_category_name,
            lv_subsidiary_name
        from XXCNV_GL_C002_GL_TRANSFORMED_INTERFACE_STG
        where upper(reference4)=upper(i.reference4) 
        and upper(period_name)=upper(i.period_name)
        and upper(segment1)=upper(i.segment1)
        and rownum=1;

        --Adding this logic as the period_name format is not same for transformed files and recon files
        --user_je_category_name='NETSUITE CONVERSION' for summary files
        --user_je_category_name='Period Activity' for perAccount files
        Select
             batch_name
        into 
             lv_batch_name
        from XXCNV_GL_C002_GL_RECONCILED_INTERFACE_STG
        WHERE
            1 = 1
            AND upper(segment1) = upper(i.segment1)
            AND rownum = 1
            AND (
                (upper(user_je_category_name) = upper('NETSUITE CONVERSION') AND upper(period_name) = upper(i.period_name))
                OR
                (upper(user_je_category_name) = upper('Period Activity') AND upper(period_name) = upper(TO_CHAR(TO_DATE(i.period_name,'YYYY-MM-DD'),'MON-YY'))
                )
            );               

        -- for transformed file
        select 
			NVL(count(1),0),
			count(DISTINCT reference4),
			NVL(SUM(TO_NUMBER(entered_cr)),0),
			NVL(SUM(TO_NUMBER(entered_dr)),0),
			NVL(SUM(TO_NUMBER(accounted_cr)),0),
			NVL(SUM(TO_NUMBER(accounted_dr)),0)
		into
			lv_t_line_count,
			lv_t_journal_count,
			lv_t_sum_entered_cr,
			lv_t_sum_entered_dr,
			lv_t_sum_acct_cr,
			lv_t_sum_acct_dr
		from XXCNV_GL_C002_GL_TRANSFORMED_INTERFACE_STG
        where upper(reference4)=upper(i.reference4) --journal name basis
        and upper(period_name)=upper(i.period_name)
        and upper(segment1)=upper(i.segment1);

        -- for recon report file
        if upper(lv_category_name) = upper('NETSUITE CONVERSION')
        then         
        select 
			NVL(count(1),0),
			count(DISTINCT journal_name),
			NVL(SUM(TO_NUMBER(entered_cr)),0),
			NVL(SUM(TO_NUMBER(entered_dr)),0),
			NVL(SUM(TO_NUMBER(accounted_cr)),0),
            NVL(SUM(TO_NUMBER(accounted_dr)),0)
		into
			lv_r_line_count,
			lv_r_journal_count,
			lv_r_sum_entered_cr,
			lv_r_sum_entered_dr,
			lv_r_sum_acct_cr,
			lv_r_sum_acct_dr
		from XXCNV_GL_C002_GL_RECONCILED_INTERFACE_STG
		where upper(SUBSTR(journal_name,1,INSTR(journal_name, 'Conversion') - 1))  = upper(SUBSTR(i.reference4,1,INSTR(i.reference4,'Conversion')-1))
        and upper(segment1)=upper(i.segment1)
        AND (
                (upper(user_je_category_name) = upper('NETSUITE CONVERSION') AND upper(period_name) = upper(i.period_name))
                OR
                (upper(user_je_category_name) = upper('Period Activity') AND upper(period_name) = upper(TO_CHAR(TO_DATE(i.period_name,'YYYY-MM-DD'),'MON-YY'))
                )
            );

        else

        select 
			NVL(count(1),0),
			count(DISTINCT journal_name),
			NVL(SUM(TO_NUMBER(entered_cr)),0),
			NVL(SUM(TO_NUMBER(entered_dr)),0),
			0,
            0
		into
			lv_r_line_count,
			lv_r_journal_count,
			lv_r_sum_entered_cr,
			lv_r_sum_entered_dr,
			lv_r_sum_acct_cr,
			lv_r_sum_acct_dr
		from XXCNV_GL_C002_GL_RECONCILED_INTERFACE_STG
		where upper(SUBSTR(journal_name,1,INSTR(journal_name, 'Conversion') - 1))  = upper(SUBSTR(i.reference4,1,INSTR(i.reference4,'Conversion')-1))
        and upper(segment1)=upper(i.segment1)
        AND (
                (upper(user_je_category_name) = upper('NETSUITE CONVERSION') AND upper(period_name) = upper(i.period_name))
                OR
                (upper(user_je_category_name) = upper('Period Activity') AND upper(period_name) = upper(TO_CHAR(TO_DATE(i.period_name,'YYYY-MM-DD'),'MON-YY'))
                )
            );
        end if;

        --check status based on the comparison criteria
      /*  lv_status := CASE
					WHEN	(lv_t_line_count = lv_r_line_count) AND 
							(lv_t_journal_count = lv_r_journal_count) AND
							(lv_t_sum_entered_cr = lv_r_sum_entered_cr) AND
							(lv_t_sum_entered_dr = lv_r_sum_entered_dr)AND
							(lv_t_sum_acct_cr = lv_r_sum_acct_cr) AND
							(lv_t_sum_acct_dr = lv_r_sum_acct_dr)
						THEN gv_status_success
					ELSE gv_status_failure 
             END;*/
             lv_error_message := 
    CASE WHEN lv_t_line_count != lv_r_line_count
         THEN 'Line count mismatch; ' ELSE '' END ||
    CASE WHEN lv_t_journal_count != lv_r_journal_count
         THEN 'Journal count mismatch; ' ELSE '' END ||
    CASE WHEN lv_t_sum_entered_cr != lv_r_sum_entered_cr
         THEN 'Sum entered credit mismatch; ' ELSE '' END ||
    CASE WHEN lv_t_sum_entered_dr != lv_r_sum_entered_dr
         THEN 'Sum entered debit mismatch; ' ELSE '' END ||
    CASE WHEN lv_t_sum_acct_cr != lv_r_sum_acct_cr
         THEN 'Sum account credit mismatch; ' ELSE '' END ||
    CASE WHEN lv_t_sum_acct_dr != lv_r_sum_acct_dr
         THEN 'Sum account debit mismatch; ' ELSE '' END;

       lv_status := CASE 
                WHEN lv_error_message IS NULL OR lv_error_message = ''
                THEN gv_status_success
                ELSE gv_status_failure
             END;




        INSERT INTO XXCNV_GL_C002_GL_RECONCILE_STATUS_INTERFACE_STG VALUES(
            lv_oic_instance_id,
            lv_conversion_id,
            lv_iteration_number,
            i.reference4,
            i.period_name,
            lv_ledger_name,
            lv_category_name,
            lv_subsidiary_name,
            lv_batch_name,
            null,
			null,
			null,
			null,
			null,
			null,
            lv_t_line_count,
			lv_t_journal_count,
			lv_t_sum_entered_cr,
			lv_t_sum_entered_dr,
			lv_t_sum_acct_cr,
			lv_t_sum_acct_dr,
            lv_r_line_count,
			lv_r_journal_count,
			lv_r_sum_entered_cr,
			lv_r_sum_entered_dr,
			lv_r_sum_acct_cr,
			lv_r_sum_acct_dr,
            lv_status,
            lv_error_message,
            gv_created_by,
                        to_CHAR(sysdate,'DD-MM-YYYY HH24:MI:SS'),
            gv_last_updated_by,
                        to_CHAR(sysdate,'DD-MM-YYYY HH24:MI:SS'));


    END LOOP;  

        p_status := gv_status_success;

    EXCEPTION
    WHEN OTHERS THEN 
	ROLLBACK;
        dbms_output.put_line('An error occurred while reconciling journal status: '|| '->'|| SUBSTR(SQLERRM, 1, 3000) || '->'|| DBMS_UTILITY.format_error_backtrace);
        p_status := gv_status_Failure;
        p_message := 'An error occurred while reconciling journal status: ->'|| SUBSTR(SQLERRM, 1, 3000) || '->'|| DBMS_UTILITY.format_error_backtrace;
    END; 

END compare_transformed_recon_file_prc;

END xxcnv_gl_c002_gl_journals_reconcile_pkg;