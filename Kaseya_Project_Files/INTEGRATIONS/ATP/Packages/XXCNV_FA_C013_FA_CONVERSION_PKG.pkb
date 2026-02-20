create or replace PACKAGE BODY   xxcnv.xxcnv_fa_c013_fa_conversion_pkg IS
	/*************************************************************************************
    NAME              :     FA_Conversion_Package BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -----------------------------------------
	-- Phanindra	   10-Mar-2024      1.0         Initial Development
	-- Phanindra       29-Jul-2025      1.1         Added changes for JIRA ID-6261 
	-- Satya Pavani    31-Jul-2025      1.2         Updated code based on JIRA ID-6493
	****************************************************************************************/

---Declaring global Variables

    gv_loading_status             VARCHAR2(256) := NULL;
    gv_error_message              VARCHAR2(500) := NULL;
    gv_file_name                  VARCHAR2(256) := NULL;
    gv_oci_file_name              VARCHAR2(4000) := NULL;
    gv_oci_file_path              VARCHAR2(200) := NULL;
    gv_oci_file_name_addition     VARCHAR2(100) := NULL;
    gv_oci_file_name_distribution VARCHAR2(100) := 'FaMassaddDistributions';
    gv_execution_id               VARCHAR2(100) := NULL;
    gv_book_type_code             VARCHAR2(50) := NULL;
    gv_interface_line_number      VARCHAR2(50) := NULL;
    gv_batch_id                   VARCHAR2(200) := NULL;
    gv_credential_name            CONSTANT VARCHAR2(30) := 'OCI$RESOURCE_PRINCIPAL';
    gv_status_success             CONSTANT VARCHAR2(100) := 'Success';
    gv_status_failure             CONSTANT VARCHAR2(100) := 'Failure';
    gv_conversion_id              VARCHAR2(100) := NULL;
    gv_boundary_system            VARCHAR2(100) := NULL;
    gv_status_picked              CONSTANT VARCHAR2(100) := 'FILE_PICKED_FROM_OCI AND LOADED TO STG';
    gv_status_picked_for_tr       CONSTANT VARCHAR2(100) := 'TRANSFORMED DATA FROM EXT TO STG';
    gv_status_validated           CONSTANT VARCHAR2(100) := 'VALIDATED';
    gv_status_failed              CONSTANT VARCHAR2(100) := 'FAILED_AT_VALIDATION';
    gv_coa_transformation         CONSTANT VARCHAR2(100) := 'COA_TRANSFORMATION';
    gv_coa_transformation_failed  CONSTANT VARCHAR2(50) := 'COA_TRANSFORMATION_FAILED';
    gv_fbdi_export_status         CONSTANT VARCHAR2(100) := 'EXPORTED_TO_FBDI';
    gv_fbdi_export_status_fail    CONSTANT VARCHAR2(100) := 'EXPORTED_TO_FBDI_FAILED';
    gv_status_staged              CONSTANT VARCHAR2(100) := 'STAGED_FOR_IMPORT';
    gv_transformed_folder         CONSTANT VARCHAR2(100) := 'Transformed_FBDI_Files';
    gv_source_folder              CONSTANT VARCHAR2(100) := 'Source_FBDI_Files';
    gv_properties                 CONSTANT VARCHAR2(100) := 'properties';
    gv_file_picked                VARCHAR2(100) := 'File_Picked_From_OCI_Server';
    gv_file_not_found             CONSTANT VARCHAR2(100) := 'File_not_found';
    gv_recon_folder               CONSTANT VARCHAR2(50) := 'ATP_Validation_Error_Files';
    gv_recon_report               CONSTANT VARCHAR2(50) := 'Recon_Report_Created';
    gv_addition                   CONSTANT VARCHAR2(50) := 'ADDITIONS';
    gv_distribution               CONSTANT VARCHAR2(50) := 'DISTRIBUTIONS';
    gv_status_validation2         CONSTANT VARCHAR2(100) := 'VALIDATION2';
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
                    WHEN lv_file_name LIKE '%FaMassAdditions%.csv' THEN
                        gv_oci_file_name_addition := lv_file_name;
                    /*WHEN lv_file_name LIKE '%FaMassaddDistributions%.csv' THEN
                        gv_oci_file_name_distribution := lv_file_name;*/
                    ELSE
                        dbms_output.put_line('No match found for file name: ' || lv_file_name); -- Debugging output
                END CASE;

                lv_start_pos := lv_end_pos + 1;
            END LOOP;

        -- Output the results for debugging
            dbms_output.put_line('lv_File Name: ' || lv_file_name);
            dbms_output.put_line('Addition File Name: ' || gv_oci_file_name_addition);
            --dbms_output.put_line('Distribution File Name: ' || gv_oci_file_name_distribution);
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error fetching execution details: ' || sqlerrm);
		--RETURN;
        END;	

    -- Call to import data from OCI to Stage table
        BEGIN
            import_data_from_oci_to_stg_prc(p_loading_status);
            IF p_loading_status = gv_status_failure THEN
                dbms_output.put_line('Error in IMPORT_DATA_FROM_OCI_TO_STG');
                RETURN;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling IMPORT_DATA_FROM_OCI_TO_STG: ' || sqlerrm);
            --RETURN;
        END;

    -- Call to perform data and business validations in staging table
   /* BEGIN
        data_validations2;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling data_validations2: ' || SQLERRM);
           -- RETURN;
    END;
*/

       -- Call to perform setup validations in staging table
        BEGIN
            data_validations_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling data_validations: ' || sqlerrm);
           -- RETURN;
        END;

		-- Call to perform COA transaction
        BEGIN
            coa_target_segments_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling coa_target_segments: ' || sqlerrm);
          --  RETURN;
        END; 


    -- Call to create a CSV file from xxcnv_fa_c013_fa_massadd_stg after all validations
        BEGIN
            create_fbdi_file_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_fbdi_file: ' || sqlerrm);
           -- RETURN;
        END;


        ---create a atp recon report
        BEGIN
            create_recon_report_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_recon_report: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);
          --  RETURN;
        END; 

		/*
	    -- Call to create a Properties file from xxcnv_fa_c013_fa_massadd_stg after all validations
        BEGIN
            create_properties_file;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling create_properties_file: ' || sqlerrm);
          --  RETURN;
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
            lv_table_count := 0;
       -- Check if the external table exists and drop it if it does
            SELECT
                COUNT(*)
            INTO lv_table_count
            FROM
                all_objects
            WHERE
                    upper(object_name) = 'XXCNV_FA_C013_FA_MASSADD_EXT'
                AND object_type = 'TABLE';

            IF lv_table_count > 0 THEN
                EXECUTE IMMEDIATE 'DROP TABLE XXCNV_FA_C013_FA_MASSADD_EXT';
                EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_FA_C013_FA_MASSADD_STG';
                EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_FA_C013_FA_MASSADD_DIST_STG';
                dbms_output.put_line('Table XXCNV_FA_C013_FA_MASSADD_EXT dropped');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error dropping table XXCNV_FA_C013_FA_MASSADD_EXT: '
                                     || '->'
                                     || substr(sqlerrm, 1, 3000)
                                     || '->'
                                     || dbms_utility.format_error_backtrace);

                p_loading_status := gv_status_failure;
        END;

    -- Create the external table
        BEGIN
            IF gv_oci_file_name_addition LIKE '%FaMassAdditions%' THEN
                dbms_output.put_line('Creating external table XXCNV_FA_C013_FA_MASSADD_EXT');
                dbms_output.put_line(' XXCNV_FA_C013_FA_MASSADD_EXT : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_addition);
                dbms_cloud.create_external_table(
                    table_name      => 'XXCNV_FA_C013_FA_MASSADD_EXT',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_addition,
                    format          =>
                            JSON_OBJECT(
                                'skipheaders' VALUE '1',
                                'type' VALUE 'csv',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'ignoremissingcolumns' VALUE 'true',
                                'blankasnull' VALUE 'true',
                                        'conversionerrors' VALUE 'store_null'
                            ),
                    column_list     => '  interface_line_number                   Number,
           Book_type_code                          Varchar2(30),
           Transaction_Name                         Varchar2(240),
           Asset_Number                            Varchar2(30),
           Description                             Varchar2(300),
           Tag_Number                              Varchar2(15),
           Manufacturer_Name                       Varchar2(360),
           Serial_Number                           Varchar2(35),
           Model_Number                            Varchar2(40),
           Asset_Type                              Varchar2(50),
           Fixed_Assets_Cost                       Number,
           Date_Placed_In_Service                  varchar2(50),
           Prorate_Convention_Code                 VarChar2(50),
           Fixed_Assets_Units                      Number,
           asset_category_id_segment1              VARCHAR2(100),
           asset_category_id_segment2              VARCHAR2(50),
           asset_category_id_segment3              VARCHAR2(50),
           asset_category_id_segment4              VARCHAR2(50),
           asset_category_id_segment5              VARCHAR2(50),
           asset_category_id_segment6              VARCHAR2(50),
           asset_category_id_segment7              VARCHAR2(50),
           posting_status                          VARCHAR2(15),
           queue_name                             VARCHAR2(30),
           feeder_system_name                     VARCHAR2(40),
           parent_asset_number                    VARCHAR2(30),
           add_to_asset_number                    VARCHAR2(30),
           asset_key_segment1                     VARCHAR2(50),
           asset_key_segment2                     VARCHAR2(50),
           asset_key_segment3                     VARCHAR2(50),
           asset_key_segment4                     VARCHAR2(50),
           asset_key_segment5                     VARCHAR2(50),
           asset_key_segment6                     VARCHAR2(50),
           asset_key_segment7                     VARCHAR2(50),
           asset_key_segment8                     VARCHAR2(50),
           asset_key_segment9                     VARCHAR2(50),
           asset_key_segment10                    VARCHAR2(50),
           inventorial                            VARCHAR2(3),
           property_type_code                     VARCHAR2(30),
           property_1245_1250_code                VARCHAR2(4),
           in_use_flag                            VARCHAR2(3),
           owned_leased                           VARCHAR2(15),
           new_used                               VARCHAR2(4),
           material_indicator_flag                VARCHAR2(1),
           commitment                             VARCHAR2(30),
           investment_law                         VARCHAR2(30),
           amortize_flag                          VARCHAR2(3),
           amortization_start_date                DATE,
           depreciate_flag                        VARCHAR2(3),
           salvage_type                           VARCHAR2(30),
           salvage_value                          NUMBER,
           percent_salvage_value                  NUMBER,
           ytd_deprn                              NUMBER,
           deprn_reserve                          NUMBER,
           bonus_ytd_deprn                        NUMBER,
           bonus_deprn_reserve                    NUMBER,
           ytd_impairment                         NUMBER,
            impairment_reserve                    NUMBER,
           method_code                            VARCHAR2(12),
           life_in_months                         NUMBER(4),
           basic_rate                             NUMBER,
           adjusted_rate                          NUMBER,
           unit_of_measure                        VARCHAR2(25),
           production_capacity                    NUMBER,
           ceiling_type                           VARCHAR2(50),
           bonus_rule                             VARCHAR2(30),
           cash_generating_unit                   VARCHAR2(30),
           deprn_limit_type                       VARCHAR2(30),
           allowed_deprn_limit                    NUMBER,
           allowed_deprn_limit_amount             NUMBER,
           payables_cost                          NUMBER,
           payables_code_combination_id_segment1  VARCHAR2(50),
           payables_code_combination_id_segment2  VARCHAR2(50),
           payables_code_combination_id_segment3  VARCHAR2(50),
           payables_code_combination_id_segment4  VARCHAR2(50),
           payables_code_combination_id_segment5  VARCHAR2(50),
           payables_code_combination_id_segment6  VARCHAR2(50),
           payables_code_combination_id_segment7  VARCHAR2(50),
           payables_code_combination_id_segment8  VARCHAR2(50),
           payables_code_combination_id_segment9  VARCHAR2(50),
           payables_code_combination_id_segment10 VARCHAR2(50),
           payables_code_combination_id_segment11 VARCHAR2(50),
           payables_code_combination_id_segment12 VARCHAR2(50),
           payables_code_combination_id_segment13 VARCHAR2(50),
           payables_code_combination_id_segment14 VARCHAR2(50),
           payables_code_combination_id_segment15 VARCHAR2(50),
           payables_code_combination_id_segment16 VARCHAR2(50),
           payables_code_combination_id_segment17 VARCHAR2(50),
           payables_code_combination_id_segment18 VARCHAR2(50),
           payables_code_combination_id_segment19 VARCHAR2(50),
           payables_code_combination_id_segment20 VARCHAR2(50),
           payables_code_combination_id_segment21 VARCHAR2(50),
           payables_code_combination_id_segment22 VARCHAR2(50),
           payables_code_combination_id_segment23 VARCHAR2(50),
           payables_code_combination_id_segment24 VARCHAR2(50),
           payables_code_combination_id_segment25 VARCHAR2(50),
           payables_code_combination_id_segment26 VARCHAR2(50),
           payables_code_combination_id_segment27 VARCHAR2(50),
           payables_code_combination_id_segment28 VARCHAR2(50),
           payables_code_combination_id_segment29 VARCHAR2(50),
           payables_code_combination_id_segment30 VARCHAR2(50),
           attribute1                             VARCHAR2(50),
           attribute2                             VARCHAR2(50),
           attribute3                             VARCHAR2(50),
           attribute4                             VARCHAR2(50),
           attribute5                             VARCHAR2(50),
           attribute6                             VARCHAR2(50),
           attribute7                             VARCHAR2(50),
           attribute8                             VARCHAR2(50),
           attribute9                             VARCHAR2(50),
           attribute10                            VARCHAR2(50),
           attribute11                            VARCHAR2(50),
           attribute12                            VARCHAR2(50),
           attribute13                            VARCHAR2(50),
           attribute14                            VARCHAR2(50),
           attribute15                            VARCHAR2(50),
           attribute16                            VARCHAR2(50),
           attribute17                            VARCHAR2(50),
           attribute18                            VARCHAR2(50),
           attribute19                            VARCHAR2(50),
           attribute20                            VARCHAR2(50),
           attribute21                            VARCHAR2(50),
           attribute22                            VARCHAR2(50),
           attribute23                            VARCHAR2(50),
           attribute24                            VARCHAR2(50),
           attribute25                            VARCHAR2(50),
           attribute26                            VARCHAR2(50),
           attribute27                            VARCHAR2(50),
           attribute28                            VARCHAR2(50),
           attribute29                            VARCHAR2(50),
           attribute30                            VARCHAR2(50),
           attribute_number1                      NUMBER,
           attribute_number2                      NUMBER,
           attribute_number3                      NUMBER,
           attribute_number4                      NUMBER,
           attribute_number5                      NUMBER,
           attribute_date1                        DATE,
           attribute_date2                        DATE,
           attribute_date3                        DATE,
           attribute_date4                        DATE,
           attribute_date5                        DATE,
           attribute_category_code                VARCHAR2(50),
           context                                VARCHAR2(1000),
           th_attribute1                          VARCHAR2(50),
           th_attribute2                          VARCHAR2(50),
           th_attribute3                          VARCHAR2(50),
           th_attribute4                          VARCHAR2(50),
           th_attribute5                          VARCHAR2(50),
           th_attribute6                          VARCHAR2(50),
           th_attribute7                          VARCHAR2(50),
           th_attribute8                          VARCHAR2(50),
           th_attribute9                          VARCHAR2(50),
           th_attribute10                         VARCHAR2(50),
           th_attribute11                         VARCHAR2(50),
           th_attribute12                         VARCHAR2(50),
           th_attribute13                         VARCHAR2(50),
           th_attribute14                         VARCHAR2(50),
           th_attribute15                         VARCHAR2(50),
           th_attribute_number1                   NUMBER,
           th_attribute_number2                   NUMBER,
           th_attribute_number3                   NUMBER,
           th_attribute_number4                   NUMBER,
           th_attribute_number5                   NUMBER,
           th_attribute_date1                     DATE,
           th_attribute_date2                     DATE,
           th_attribute_date3                     DATE,
           th_attribute_date4                     DATE,
           th_attribute_date5                     DATE,
           th_attribute_category_code             VARCHAR2(50),
           th2_attribute1                         VARCHAR2(50),
           th2_attribute2                         VARCHAR2(50),
           th2_attribute3                         VARCHAR2(50),
           th2_attribute4                         VARCHAR2(50),
           th2_attribute5                         VARCHAR2(50),
           th2_attribute6                         VARCHAR2(50),
           th2_attribute7                         VARCHAR2(50),
           th2_attribute8                         VARCHAR2(50),
           th2_attribute9                         VARCHAR2(50),
           th2_attribute10                        VARCHAR2(50),
           th2_attribute11                        VARCHAR2(50),
           th2_attribute12                        VARCHAR2(50),
           th2_attribute13                        VARCHAR2(50),
           th2_attribute14                        VARCHAR2(50),
           th2_attribute15                        VARCHAR2(50),
           th2_attribute_number1                  NUMBER,
           th2_attribute_number2                  NUMBER,
           th2_attribute_number3                  NUMBER,
           th2_attribute_number4                  NUMBER,
           th2_attribute_number5                  NUMBER,
           th2_attribute_date1                    DATE,
           th2_attribute_date2                    DATE,
           th2_attribute_date3                    DATE,
           th2_attribute_date4                    DATE,
           th2_attribute_date5                    DATE,
           th2_attribute_category_code            VARCHAR2(50),            
           ai_attribute1                          VARCHAR2(50),
           ai_attribute2                          VARCHAR2(50),
           ai_attribute3                          VARCHAR2(50),
           ai_attribute4                          VARCHAR2(50),
           ai_attribute5                          VARCHAR2(50),
           ai_attribute6                          VARCHAR2(50),
           ai_attribute7                          VARCHAR2(50),
           ai_attribute8                          VARCHAR2(50),
           ai_attribute9                          VARCHAR2(50),
           ai_attribute10                         VARCHAR2(50),
           ai_attribute11                         VARCHAR2(50),
           ai_attribute12                         VARCHAR2(50),
           ai_attribute13                         VARCHAR2(50),
           ai_attribute14                         VARCHAR2(50),
           ai_attribute15                         VARCHAR2(50),
           ai_attribute_number1                   NUMBER,
           ai_attribute_number2                   NUMBER,
           ai_attribute_number3                   NUMBER,
           ai_attribute_number4                   NUMBER,
           ai_attribute_number5                   NUMBER,
           ai_attribute_date1                     DATE,
           ai_attribute_date2                     DATE,
           ai_attribute_date3                     DATE,
           ai_attribute_date4                     DATE,
           ai_attribute_date5                     DATE,
           ai_attribute_category_code             VARCHAR2(50),
           mass_property_flag                     VARCHAR2(1),
           group_asset_number                     VARCHAR2(30),
           reduction_rate                         NUMBER,
           reduce_addition_flag                   VARCHAR2(1),
		   Apply_Reduction_Rate_to_Adjustments    VARCHAR2(1),
           reduce_retirement_flag                 VARCHAR2(1),
           recognize_gain_or_loss                 VARCHAR2(30),
           recapture_reserve_flag                 VARCHAR2(1),
           limit_proceeds_flag                    VARCHAR2(1),
           terminal_gain_or_loss                  VARCHAR2(30),
           tracking_method                        VARCHAR2(30),
           excess_allocation_option               VARCHAR2(30),
           depreciate_option                      VARCHAR2(30),
           member_rollup_flag                     VARCHAR2(1),
           allocate_to_fully_rsv_flag             VARCHAR2(1),
           over_depreciate_option                 VARCHAR2(30),
           preparer_email_address                 VARCHAR2(240),
           merged_code                            VARCHAR2(3),
           parent_interface_line_number           VARCHAR2(50),
           sum_units                              VARCHAR2(3),
           new_master_flag                        VARCHAR2(3),
           units_to_adjust                        NUMBER(15),
           short_fiscal_year_flag                 VARCHAR2(3),
           conversion_date                        DATE,
           original_deprn_start_date              DATE,
           global_attribute1                      VARCHAR2(50),
           global_attribute2                      VARCHAR2(50),
           global_attribute3                      VARCHAR2(50),
           global_attribute4                      VARCHAR2(50),
           global_attribute5                      VARCHAR2(50),
           global_attribute6                      VARCHAR2(50),
           global_attribute7                      VARCHAR2(50),
           global_attribute8                      VARCHAR2(50),
           global_attribute9                      VARCHAR2(50),
           global_attribute10                     VARCHAR2(50),
           global_attribute11                     VARCHAR2(50),
           global_attribute12                     VARCHAR2(50),
           global_attribute13                     VARCHAR2(50),
           global_attribute14                     VARCHAR2(50),
           global_attribute15                     VARCHAR2(50),
           global_attribute16                     VARCHAR2(50),
           global_attribute17                     VARCHAR2(50),
           global_attribute18                     VARCHAR2(50),
           global_attribute19                     VARCHAR2(50),
           global_attribute20                     VARCHAR2(50),
           global_attribute_number1               NUMBER,
           global_attribute_number2               NUMBER,
           global_attribute_number3               NUMBER,
           global_attribute_number4               NUMBER,
           global_attribute_number5               NUMBER,
           global_attribute_date1                 DATE,
           global_attribute_date2                 DATE,
           global_attribute_date3                 DATE,
           global_attribute_date4                 DATE,
           global_attribute_date5                 DATE,
           global_attribute_category              VARCHAR2(50),
           nbv_at_switch                          NUMBER,
           period_name_fully_reserved             VARCHAR2(15),
           period_name_extended                   VARCHAR2(15),
           prior_deprn_limit_type                 VARCHAR2(30),
           prior_deprn_limit                      NUMBER,
           prior_deprn_limit_amount               NUMBER,
           prior_method_code                      VARCHAR2(12),
           prior_life_in_months                   NUMBER(4),
           prior_basic_rate                       NUMBER,
           prior_adjusted_rate                    NUMBER,
           asset_schedule_number                  NUMBER,
           lease_number                           VARCHAR2(15),
           reval_reserve                          NUMBER,
           reval_loss_blanace                     NUMBER,
           reval_amortization_basis               NUMBER,
           impair_loss_balance                    NUMBER,
           reval_ceiling                          NUMBER,
           fair_market_value                      NUMBER,
           last_price_index_value                 NUMBER,
           global_attribute_number6               NUMBER,
           global_attribute_number7               NUMBER,
           global_attribute_number8               NUMBER,
           global_attribute_number9               NUMBER,
           global_attribute_number10              NUMBER,
           global_attribute_date6                 DATE,
           global_attribute_date7                 DATE,
           global_attribute_date8                 DATE,
           global_attribute_date9                 DATE,
           global_attribute_date10                DATE,
           bk_global_attribute1                   VARCHAR2(50),
           bk_global_attribute2                   VARCHAR2(50),
           bk_global_attribute3                   VARCHAR2(50),
           bk_global_attribute4                   VARCHAR2(50),
           bk_global_attribute5                   VARCHAR2(50),
           bk_global_attribute6                   VARCHAR2(50),
           bk_global_attribute7                   VARCHAR2(50),
           bk_global_attribute8                   VARCHAR2(50),
           bk_global_attribute9                   VARCHAR2(50),
		   bk_global_attribute10                  VARCHAR2(50),
		   bk_global_attribute11                  VARCHAR2(50),
		   bk_global_attribute12                  VARCHAR2(50),
		   bk_global_attribute13                  VARCHAR2(50),
		   bk_global_attribute14                  VARCHAR2(50),
		   bk_global_attribute15                  VARCHAR2(50),
		   bk_global_attribute16                  VARCHAR2(50),
		   bk_global_attribute17                  VARCHAR2(50),
		   bk_global_attribute18                  VARCHAR2(50),
		   bk_global_attribute19                  VARCHAR2(50),
		   bk_global_attribute20                  VARCHAR2(50),
           BK_GLOBAL_ATTRIBUTE_NUMBER1            NUMBER,
		   BK_GLOBAL_ATTRIBUTE_NUMBER2            NUMBER,
		   BK_GLOBAL_ATTRIBUTE_NUMBER3            NUMBER,
		   BK_GLOBAL_ATTRIBUTE_NUMBER4            NUMBER,
		   BK_GLOBAL_ATTRIBUTE_NUMBER5            NUMBER,
           BK_GLOBAL_ATTRIBUTE_DATE1              Date,
		   BK_GLOBAL_ATTRIBUTE_DATE2              Date,
		   BK_GLOBAL_ATTRIBUTE_DATE3              Date,
		   BK_GLOBAL_ATTRIBUTE_DATE4              Date,
		   BK_GLOBAL_ATTRIBUTE_DATE5              Date,
           BK_GLOBAL_ATTRIBUTE_CATEGORY           VARCHAR2(50),
		   th_global_attribute1 VARCHAR2(50),
		   th_global_attribute2 VARCHAR2(50),
		   th_global_attribute3 VARCHAR2(50),
		   th_global_attribute4 VARCHAR2(50),
		   th_global_attribute5 VARCHAR2(50),
		   th_global_attribute6 VARCHAR2(50),
		   th_global_attribute7 VARCHAR2(50),
		   th_global_attribute8 VARCHAR2(50),
		   th_global_attribute9 VARCHAR2(50),
		   th_global_attribute10 VARCHAR2(50),
		   th_global_attribute11 VARCHAR2(50),		  
           th_global_attribute12                  VARCHAR2(50),
           th_global_attribute13                  VARCHAR2(50),
           th_global_attribute14                  VARCHAR2(50),
           th_global_attribute15                  VARCHAR2(50),
           th_global_attribute16                  VARCHAR2(50),
           th_global_attribute17                  VARCHAR2(50),
           th_global_attribute18                  VARCHAR2(50),
           th_global_attribute19                  VARCHAR2(50),
           th_global_attribute20                  VARCHAR2(50),
           th_global_attribute_number1            NUMBER,
           th_global_attribute_number2            NUMBER,
           th_global_attribute_number3            NUMBER,
           th_global_attribute_number4            NUMBER,
           th_global_attribute_number5            NUMBER,
           th_global_attribute_date1              DATE,
           th_global_attribute_date2              DATE,
           th_global_attribute_date3              DATE,
           th_global_attribute_date4              DATE,
           th_global_attribute_date5              DATE,
           th_global_attribute_category           VARCHAR2(50),
           ai_global_attribute1                   VARCHAR2(50),
           ai_global_attribute2                   VARCHAR2(50),
           ai_global_attribute3                   VARCHAR2(50),
           ai_global_attribute4                   VARCHAR2(50),
           ai_global_attribute5                   VARCHAR2(50),
           ai_global_attribute6                   VARCHAR2(50),
           ai_global_attribute7                   VARCHAR2(50),
           ai_global_attribute8                   VARCHAR2(50),
           ai_global_attribute9                   VARCHAR2(50),
           ai_global_attribute10                  VARCHAR2(50),
           ai_global_attribute11                  VARCHAR2(50),
           ai_global_attribute12                  VARCHAR2(50),
           ai_global_attribute13                  VARCHAR2(50),
           ai_global_attribute14                  VARCHAR2(50),
           ai_global_attribute15                  VARCHAR2(50),
           ai_global_attribute16                  VARCHAR2(50),
           ai_global_attribute17                  VARCHAR2(50),
           ai_global_attribute18                  VARCHAR2(50),
           ai_global_attribute19                  VARCHAR2(50),
           ai_global_attribute20                  VARCHAR2(50),
           ai_global_attribute_number1            NUMBER,
           ai_global_attribute_number2            NUMBER,
           ai_global_attribute_number3            NUMBER,
           ai_global_attribute_number4            NUMBER,
           ai_global_attribute_number5            NUMBER,
           ai_global_attribute_date1              DATE,
           ai_global_attribute_date2              DATE,
           ai_global_attribute_date3              DATE,
           ai_global_attribute_date4              DATE,
           ai_global_attribute_date5              DATE,
           ai_global_attribute_category           VARCHAR2(50),
           vendor_name                            VARCHAR2(30),
           vendor_number                          VARCHAR2(30),
           po_number                              VARCHAR2(30),
           invoice_number                         VARCHAR2(30),
           invoice_voucher_number                 VARCHAR2(50),
           invoice_date                           DATE,
           payables_units                         NUMBER,
           invoice_line_number                    NUMBER,
           invoice_line_type                      VARCHAR2(30),
           invoice_line_description               VARCHAR2(240),
           invoice_payment_number                 NUMBER(18),
           project_number                         VARCHAR2(25),
           project_task_number                    VARCHAR2(100),
           fully_reserve_on_add_flag              VARCHAR2(1),
           deprn_adjustment_factor                NUMBER ,
		   revalued_cost           number,
		   backlog_deprn_reserve   number,
		   ytd_backlog_deprn       number,
		   reval_amort_balance     number,
		   ytd_reval_amortization  number,
		   ln_interface_line_number                  NUMBER,
                              Units_Number                        NUMBER,
                              Assigned_to                         VARCHAR2(255),
                              location_id_Segment1                VARCHAR2(255),
                              location_id_Segment2                VARCHAR2(255),
                              location_id_Segment3                VARCHAR2(255),
                              location_id_Segment4                VARCHAR2(255),
                              location_id_Segment5                VARCHAR2(255),
                              location_id_Segment6                VARCHAR2(255),
                              location_id_Segment7                VARCHAR2(255),
                              Deprn_Expense_CCID_Segment1         VARCHAR2(255),
                              Deprn_Expense_CCID_Segment2         VARCHAR2(255),
                              Deprn_Expense_CCID_Segment3         VARCHAR2(255),
                              Deprn_Expense_CCID_Segment4         VARCHAR2(255),
                              Deprn_Expense_CCID_Segment5         VARCHAR2(255),
                              Deprn_Expense_CCID_Segment6         VARCHAR2(255),
                              Deprn_Expense_CCID_Segment7         VARCHAR2(255),
                              Deprn_Expense_CCID_Segment8         VARCHAR2(255),
                              Deprn_Expense_CCID_Segment9         VARCHAR2(255),
                              Deprn_Expense_CCID_Segment10        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment11        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment12        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment13        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment14        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment15        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment16        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment17        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment18        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment19        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment20        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment21        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment22        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment23        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment24        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment25        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment26        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment27        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment28        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment29        VARCHAR2(255),
                              Deprn_Expense_CCID_Segment30        VARCHAR2(255)'
                );

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_fa_c013_fa_massadd_stg (
                    interface_line_number                 ,
                    Book_type_code                        ,
                    Transaction_Name                      ,
                    Asset_Number                          ,
                    Description                           ,
                    Tag_Number                            ,
                    Manufacturer_Name                     ,
                    Serial_Number                         ,
                    Model_Number                          ,
                    Asset_Type                            ,
                    Fixed_Assets_Cost                     ,
                    Date_Placed_In_Service				  ,
                    Prorate_Convention_Code               ,
                    Fixed_Assets_Units                    ,
                    asset_category_id_segment1            ,
                    asset_category_id_segment2            ,
                    asset_category_id_segment3            ,
                    asset_category_id_segment4            ,
                    asset_category_id_segment5            ,
                    asset_category_id_segment6            ,
                    asset_category_id_segment7            ,
                    posting_status                        ,
                    queue_name                            ,
                    feeder_system_name                    ,
                    parent_asset_number                   ,
                    add_to_asset_number                   ,
                    asset_key_segment1                    ,
                    asset_key_segment2                    ,
                    asset_key_segment3                    ,
                    asset_key_segment4                    ,
                    asset_key_segment5                    ,
                    asset_key_segment6                    ,
                    asset_key_segment7                    ,
                    asset_key_segment8                    ,
                    asset_key_segment9                    ,
                    asset_key_segment10                   ,
                    inventorial                           ,
                    property_type_code                    ,
                    property_1245_1250_code               ,
                    in_use_flag                           ,
                    owned_leased                          ,
                    new_used                              ,
                    material_indicator_flag               ,
                    commitment                            ,
                    investment_law                        ,
                    amortize_flag                         ,
                    amortization_start_date               ,
                    depreciate_flag                       ,
                    salvage_type                          ,
                    salvage_value                         ,
                    percent_salvage_value                 ,
                    ytd_deprn                             ,
                    deprn_reserve                         ,
                    bonus_ytd_deprn                       ,
                    bonus_deprn_reserve                   ,
                    ytd_impairment                        ,
                     impairment_reserve                   ,
                    method_code                           ,
                    life_in_months                        ,
                    basic_rate                            ,
                    adjusted_rate                         ,
                    unit_of_measure                       ,
                    production_capacity                   ,
                    ceiling_type                          ,
                    bonus_rule                            ,
                    cash_generating_unit                  ,
                    deprn_limit_type                      ,
                    allowed_deprn_limit                   ,
                    allowed_deprn_limit_amount            ,
                    payables_cost                         ,
                    attribute1                            ,
                    attribute2                            ,
                    attribute3                            ,
                    attribute4                            ,
                    attribute5                            ,
                    attribute6                            ,
                    attribute7                            ,
                    attribute8                            ,
                    attribute9                            ,
                    attribute10                           ,
                    attribute11                           ,
                    attribute12                           ,
                    attribute13                           ,
                    attribute14                           ,
                    attribute15                           ,
                    attribute16                           ,
                    attribute17                           ,
                    attribute18                           ,
                    attribute19                           ,
                    attribute20                           ,
                    attribute21                           ,
                    attribute22                           ,
                    attribute23                           ,
                    attribute24                           ,
                    attribute25                           ,
                    attribute26                           ,
                    attribute27                           ,
                    attribute28                           ,
                    attribute29                           ,
                    attribute30                           ,
                    attribute_number1                     ,
                    attribute_number2                     ,
                    attribute_number3                     ,
                    attribute_number4                     ,
                    attribute_number5                     ,
                    attribute_date1                       ,
                    attribute_date2                       ,
                    attribute_date3                       ,
                    attribute_date4                       ,
                    attribute_date5                       ,
                    attribute_category_code               ,
                    context                               ,
                    mass_property_flag                    ,
                    group_asset_number                    ,
                    reduction_rate                        ,
                    reduce_addition_flag                  ,
		            Apply_Reduction_Rate_to_Adjustments   ,
                    reduce_retirement_flag                ,
                    recognize_gain_or_loss                ,
                    recapture_reserve_flag                ,
                    limit_proceeds_flag                   ,
                    terminal_gain_or_loss                 ,
                    tracking_method                       ,
                    excess_allocation_option              ,
                    depreciate_option                     ,
                    member_rollup_flag                    ,
                    allocate_to_fully_rsv_flag            ,
                    over_depreciate_option                ,
                    preparer_email_address                ,
                    merged_code                           ,
                    parent_interface_line_number          ,
                    sum_units                             ,
                    new_master_flag                       ,
                    units_to_adjust                       ,
                    short_fiscal_year_flag                ,
                    conversion_date                       ,
                    original_deprn_start_date             ,
                    global_attribute1                     ,
                    global_attribute2                     ,
                    global_attribute3                     ,
                    global_attribute4                     ,
                    global_attribute5                     ,
                    global_attribute6                     ,
                    global_attribute7                     ,
                    global_attribute8                     ,
                    global_attribute9                     ,
                    global_attribute10                    ,
                    global_attribute11                    ,
                    global_attribute12                    ,
                    global_attribute13                    ,
                    global_attribute14                    ,
                    global_attribute15                    ,
                    global_attribute16                    ,
                    global_attribute17                    ,
                    global_attribute18                    ,
                    global_attribute19                    ,
                    global_attribute20                    ,
                    global_attribute_number1              ,
                    global_attribute_number2              ,
                    global_attribute_number3              ,
                    global_attribute_number4              ,
                    global_attribute_number5              ,
                    global_attribute_date1                ,
                    global_attribute_date2                ,
                    global_attribute_date3                ,
                    global_attribute_date4                ,
                    global_attribute_date5                ,
                    global_attribute_category             ,
                    nbv_at_switch                         ,
                    period_name_fully_reserved            ,
                    period_name_extended                  ,
                    prior_deprn_limit_type                ,
                    prior_deprn_limit                     ,
                    prior_deprn_limit_amount              ,
                    prior_method_code                     ,
                    prior_life_in_months                  ,
                    prior_basic_rate                      ,
                    prior_adjusted_rate                   ,
                    asset_schedule_number                 ,
                    lease_number                          ,
                    reval_reserve                         ,
                    reval_loss_blanace                    ,
                    reval_amortization_basis              ,
                    impair_loss_balance                   ,
                    reval_ceiling                         ,
                    fair_market_value                     ,
                    last_price_index_value                ,
                    global_attribute_number6              ,
                    global_attribute_number7              ,
                    global_attribute_number8              ,
                    global_attribute_number9              ,
                    global_attribute_number10             ,
                    global_attribute_date6                ,
                    global_attribute_date7                ,
                    global_attribute_date8                ,
                    global_attribute_date9                ,
                    global_attribute_date10               ,
                    vendor_name                           ,
                    vendor_number                         ,
                    po_number                             ,
                    invoice_number                        ,
                    invoice_voucher_number                ,
                    invoice_date                          ,
                    payables_units                        ,
                    invoice_line_number                   ,
                    invoice_line_type                     ,
                    invoice_line_description              ,
                    invoice_payment_number                ,
                    project_number                        ,
                    project_task_number                   ,
                    fully_reserve_on_add_flag             ,
					deprn_adjustment_factor                ,
					revalued_cost  ,         
					backlog_deprn_reserve ,
					ytd_backlog_deprn   ,    
					reval_amort_balance   ,  
					ytd_reval_amortization ,
					file_name,
					error_message,
					loading_status,
					file_reference_identifier,
                    source_system				  
                      )
					SELECT 
                    interface_line_number                 ,
                    Book_type_code                        ,
                    Transaction_Name                      ,
                    Asset_Number                          ,
                    Description                           ,
                    Tag_Number                            ,
                    Manufacturer_Name                     ,
                    Serial_Number                         ,
                    Model_Number                          ,
                    Asset_Type                            ,
                    Fixed_Assets_Cost                     ,
                    Date_Placed_In_Service                ,
                    Prorate_Convention_Code               ,
                    Fixed_Assets_Units                    ,
                    asset_category_id_segment1            ,
                    asset_category_id_segment2            ,
                    asset_category_id_segment3            ,
                    asset_category_id_segment4            ,
                    asset_category_id_segment5            ,
                    asset_category_id_segment6            ,
                    asset_category_id_segment7            ,
                    posting_status                        ,
                    queue_name                            ,
                    feeder_system_name                    ,
                    parent_asset_number                   ,
                    add_to_asset_number                   ,
                    asset_key_segment1                    ,
                    asset_key_segment2                    ,
                    asset_key_segment3                    ,
                    asset_key_segment4                    ,
                    asset_key_segment5                    ,
                    asset_key_segment6                    ,
                    asset_key_segment7                    ,
                    asset_key_segment8                    ,
                    asset_key_segment9                    ,
                    asset_key_segment10                   ,
                    inventorial                           ,
                    property_type_code                    ,
                    property_1245_1250_code               ,
                    in_use_flag                           ,
                    owned_leased                          ,
                    new_used                              ,
                    material_indicator_flag               ,
                    commitment                            ,
                    investment_law                        ,
                    amortize_flag                         ,
                    amortization_start_date               ,
                    depreciate_flag                       ,
                    salvage_type                          ,
                    salvage_value                         ,
                    percent_salvage_value                 ,
                    ytd_deprn                             ,
                    deprn_reserve                         ,
                    bonus_ytd_deprn                       ,
                    bonus_deprn_reserve                   ,
                    ytd_impairment                        ,
                     impairment_reserve                   ,
                    method_code                           ,
                    life_in_months                        ,
                    basic_rate                            ,
                    adjusted_rate                         ,
                    unit_of_measure                       ,
                    production_capacity                   ,
                    ceiling_type                          ,
                    bonus_rule                            ,
                    cash_generating_unit                  ,
                    deprn_limit_type                      ,
                    allowed_deprn_limit                   ,
                    allowed_deprn_limit_amount            ,
                    payables_cost                         ,
                    attribute1                            ,
                    attribute2                            ,
                    attribute3                            ,
                    attribute4                            ,
                    attribute5                            ,
                    attribute6                            ,
                    attribute7                            ,
                    attribute8                            ,
                    attribute9                            ,
                    attribute10                           ,
                    attribute11                           ,
                    attribute12                           ,
                    attribute13                           ,
                    attribute14                           ,
                    attribute15                           ,
                    attribute16                           ,
                    attribute17                           ,
                    attribute18                           ,
                    attribute19                           ,
                    attribute20                           ,
                    attribute21                           ,
                    attribute22                           ,
                    attribute23                           ,
                    attribute24                           ,
                    attribute25                           ,
                    attribute26                           ,
                    attribute27                           ,
                    attribute28                           ,
                    attribute29                           ,
                    attribute30                           ,
                    attribute_number1                     ,
                    attribute_number2                     ,
                    attribute_number3                     ,
                    attribute_number4                     ,
                    attribute_number5                     ,
                    attribute_date1                       ,
                    attribute_date2                       ,
                    attribute_date3                       ,
                    attribute_date4                       ,
                    attribute_date5                       ,
                    attribute_category_code               ,
                    context                               ,
                    mass_property_flag                    ,
                    group_asset_number                    ,
                    reduction_rate                        ,
                    reduce_addition_flag                  ,
		            Apply_Reduction_Rate_to_Adjustments   ,
                    reduce_retirement_flag                ,
                    recognize_gain_or_loss                ,
                    recapture_reserve_flag                ,
                    limit_proceeds_flag                   ,
                    terminal_gain_or_loss                 ,
                    tracking_method                       ,
                    excess_allocation_option              ,
                    depreciate_option                     ,
                    member_rollup_flag                    ,
                    allocate_to_fully_rsv_flag            ,
                    over_depreciate_option                ,
                    preparer_email_address                ,
                    merged_code                           ,
                    parent_interface_line_number          ,
                    sum_units                             ,
                    new_master_flag                       ,
                    units_to_adjust                       ,
                    short_fiscal_year_flag                ,
                    conversion_date                       ,
                    original_deprn_start_date             ,
                    global_attribute1                     ,
                    global_attribute2                     ,
                    global_attribute3                     ,
                    global_attribute4                     ,
                    global_attribute5                     ,
                    global_attribute6                     ,
                    global_attribute7                     ,
                    global_attribute8                     ,
                    global_attribute9                     ,
                    global_attribute10                    ,
                    global_attribute11                    ,
                    global_attribute12                    ,
                    global_attribute13                    ,
                    global_attribute14                    ,
                    global_attribute15                    ,
                    global_attribute16                    ,
                    global_attribute17                    ,
                    global_attribute18                    ,
                    global_attribute19                    ,
                    global_attribute20                    ,
                    global_attribute_number1              ,
                    global_attribute_number2              ,
                    global_attribute_number3              ,
                    global_attribute_number4              ,
                    global_attribute_number5              ,
                    global_attribute_date1                ,
                    global_attribute_date2                ,
                    global_attribute_date3                ,
                    global_attribute_date4                ,
                    global_attribute_date5                ,
                    global_attribute_category             ,
                    nbv_at_switch                         ,
                    period_name_fully_reserved            ,
                    period_name_extended                  ,
                    prior_deprn_limit_type                ,
                    prior_deprn_limit                     ,
                    prior_deprn_limit_amount              ,
                    prior_method_code                     ,
                    prior_life_in_months                  ,
                    prior_basic_rate                      ,
                    prior_adjusted_rate                   ,
                    asset_schedule_number                 ,
                    lease_number                          ,
                    reval_reserve                         ,
                    reval_loss_blanace                    ,
                    reval_amortization_basis              ,
                    impair_loss_balance                   ,
                    reval_ceiling                         ,
                    fair_market_value                     ,
                    last_price_index_value                ,
                    global_attribute_number6              ,
                    global_attribute_number7              ,
                    global_attribute_number8              ,
                    global_attribute_number9              ,
                    global_attribute_number10             ,
                    global_attribute_date6                ,
                    global_attribute_date7                ,
                    global_attribute_date8                ,
                    global_attribute_date9                ,
                    global_attribute_date10               ,
                    vendor_name                           ,
                    vendor_number                         ,
                    po_number                             ,
                    invoice_number                        ,
                    invoice_voucher_number                ,
                    invoice_date                          ,
                    payables_units                        ,
                    invoice_line_number                   ,
                    invoice_line_type                     ,
                    invoice_line_description              ,
                    invoice_payment_number                ,
                    project_number                        ,
                    project_task_number                   ,
                    fully_reserve_on_add_flag             ,
                    deprn_adjustment_factor               ,
					revalued_cost  ,         
					backlog_deprn_reserve ,
					ytd_backlog_deprn   ,    
					reval_amort_balance   ,  
					ytd_reval_amortization ,
                    null,
				    null,
					null,
					null,
                    null
                    from XXCNV_FA_C013_FA_MASSADD_EXT';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted Records in the xxcnv_fa_c013_fa_massadd_stg: ' || SQL%rowcount);
					--commit;

--file2

                EXECUTE IMMEDIATE 'INSERT INTO xxcnv_fa_c013_fa_massadd_dist_stg (
			                   interface_line_number          ,
							   Units_Number                        ,
							   Assigned_to                  ,
							   location_id_Segment1         ,
							   location_id_Segment2         ,
							   location_id_Segment3         ,
							   location_id_Segment4         ,
							   location_id_Segment5         ,
							   location_id_Segment6         ,
							   location_id_Segment7         ,
							   Deprn_Expense_CCID_Segment1  ,
							   Deprn_Expense_CCID_Segment2  ,
							   Deprn_Expense_CCID_Segment3  ,
							   Deprn_Expense_CCID_Segment4  ,
							   Deprn_Expense_CCID_Segment5  ,
							   Deprn_Expense_CCID_Segment6  ,
							   Deprn_Expense_CCID_Segment7  ,
							   Deprn_Expense_CCID_Segment8  ,
							   Deprn_Expense_CCID_Segment9  ,
							   Deprn_Expense_CCID_Segment10 ,
							   Deprn_Expense_CCID_Segment11 ,
							   Deprn_Expense_CCID_Segment12 ,
							   Deprn_Expense_CCID_Segment13 ,
							   Deprn_Expense_CCID_Segment14 ,
							   Deprn_Expense_CCID_Segment15 ,
							   Deprn_Expense_CCID_Segment16 ,
							   Deprn_Expense_CCID_Segment17 ,
							   Deprn_Expense_CCID_Segment18 ,
							   Deprn_Expense_CCID_Segment19 ,
							   Deprn_Expense_CCID_Segment20 ,
							   Deprn_Expense_CCID_Segment21 ,
							   Deprn_Expense_CCID_Segment22 ,
							   Deprn_Expense_CCID_Segment23 ,
							   Deprn_Expense_CCID_Segment24 ,
							   Deprn_Expense_CCID_Segment25 ,
							   Deprn_Expense_CCID_Segment26 ,
							   Deprn_Expense_CCID_Segment27 ,
							   Deprn_Expense_CCID_Segment28 ,
							   Deprn_Expense_CCID_Segment29 ,
							   Deprn_Expense_CCID_Segment30 ,
                               target_segment1                       ,
                               target_segment2                       ,
                               target_segment3                       ,
                               target_segment4                       ,
                               target_segment5                       ,
                               target_segment6                       ,
                               target_segment7                       ,
                               target_segment8                       ,
                               target_segment9                       ,
                               target_segment10,
							   file_name,
						       loading_status,
						       error_message,
						       file_reference_identifier,
							   source_system)
				       SELECT
							   ln_interface_line_number          ,
							   Units_Number                 ,
							   Assigned_to                  ,
							   location_id_Segment1         ,
							   location_id_Segment2         ,
							   location_id_Segment3         ,
							   location_id_Segment4         ,
							   location_id_Segment5         ,
							   location_id_Segment6         ,
							   location_id_Segment7         ,
							   Deprn_Expense_CCID_Segment1  ,
							   Deprn_Expense_CCID_Segment2  ,
							   Deprn_Expense_CCID_Segment3  ,
							   Deprn_Expense_CCID_Segment4  ,
							   Deprn_Expense_CCID_Segment5  ,
							   Deprn_Expense_CCID_Segment6  ,
							   Deprn_Expense_CCID_Segment7  ,
							   Deprn_Expense_CCID_Segment8  ,
							   Deprn_Expense_CCID_Segment9  ,
							   Deprn_Expense_CCID_Segment10 ,
							   Deprn_Expense_CCID_Segment11 ,
							   Deprn_Expense_CCID_Segment12 ,
							   Deprn_Expense_CCID_Segment13 ,
							   Deprn_Expense_CCID_Segment14 ,
							   Deprn_Expense_CCID_Segment15 ,
							   Deprn_Expense_CCID_Segment16 ,
							   Deprn_Expense_CCID_Segment17 ,
							   Deprn_Expense_CCID_Segment18 ,
							   Deprn_Expense_CCID_Segment19 ,
							   Deprn_Expense_CCID_Segment20 ,
							   Deprn_Expense_CCID_Segment21 ,
							   Deprn_Expense_CCID_Segment22 ,
							   Deprn_Expense_CCID_Segment23 ,
							   Deprn_Expense_CCID_Segment24 ,
							   Deprn_Expense_CCID_Segment25 ,
							   Deprn_Expense_CCID_Segment26 ,
							   Deprn_Expense_CCID_Segment27 ,
							   Deprn_Expense_CCID_Segment28 ,
							   Deprn_Expense_CCID_Segment29 ,
							   Deprn_Expense_CCID_Segment30 ,
							   null                        ,
                               null                         ,
                               null                          ,
                               null                          ,
                               null                          ,
                               null                          ,
                               null                          ,
                               null                          ,
                               null                          ,
                               null                          ,
                               null                          ,
                               null                          ,
                               null                          ,
                               null                          ,
							    null	
                              FROM XXCNV_FA_C013_FA_MASSADD_EXT';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in xxcnv_fa_c013_fa_massadd_dist_stg: ' || SQL%rowcount);
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error creating external table: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

	   -- Count the number of rows in the external table
        BEGIN
            IF gv_oci_file_name_addition LIKE '%FaMassAdditions%' THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_fa_c013_fa_massadd_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_fa_c013_fa_massadd_stg from OCI Source Folder: ' || lv_row_count)
                ;
            END IF;

            IF gv_oci_file_name_distribution LIKE '%FaMassAdditions%' THEN
                SELECT
                    COUNT(*)
                INTO lv_row_count
                FROM
                    xxcnv_fa_c013_fa_massadd_dist_stg;

                dbms_output.put_line('Inserted Records in the xxcnv_fa_c013_fa_massadd_dist_stg from OCI Source Folder: ' || lv_row_count
                );
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in the external table: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

        BEGIN
				-- Count the number of rows in the external table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_fa_c013_fa_massadd_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_fa_c013_fa_massadd_stg from OCI Source Folder: ' || lv_row_count)
            ;

				-- Use an implicit cursor in the FOR LOOP to iterate over distinct book_type_code
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_picked,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_addition,
                p_attribute1        => NULL,
                p_attribute2        => lv_row_count,
                p_process_reference => NULL
            );

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_fa_c013_fa_massadd_stg: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

        BEGIN
			-- Count the number of rows in the external table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_fa_c013_fa_massadd_dist_stg;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_fa_c013_fa_massadd_dist_stg from OCI Source Folder: ' || lv_row_count
            );

			-- Use an implicit cursor in the FOR LOOP to iterate over distinct book_type_code

            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_picked,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_distribution,
                p_attribute1        => NULL,
                p_attribute2        => lv_row_count,
                p_process_reference => NULL
            );

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_fa_c013_fa_massadd_dist_stg: ' || sqlerrm);
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
        lv_row_count       NUMBER;
        lv_error_count     NUMBER;
        lv_conversion_year VARCHAR2(4);
        lv_error_message   VARCHAR2(4000);
    BEGIN
        SELECT
            to_char(sysdate, 'yyyymmddhhmmss')
        INTO gv_batch_id
        FROM
            dual;

        BEGIN
            BEGIN
                UPDATE xxcnv_fa_c013_fa_massadd_stg
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
                xxcnv_fa_c013_fa_massadd_stg
            WHERE
                execution_id = gv_execution_id;

            IF lv_row_count <> 0 THEN 

       -- Initialize error_message to an empty string if it is NULL
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = ''
                    WHERE
                        error_message IS NULL
                        AND execution_id = gv_execution_id;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('An error occurred while initializing error_message: '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);
                END;

		    -- Validate Interface Line Number
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Interface Line Number cannot be blank or non-unique'
                    WHERE
                        interface_line_number IS NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Interface Line Number should be unique'
                    WHERE
                        interface_line_number IN (
                            SELECT
                                interface_line_number
                            FROM
                                xxcnv_fa_c013_fa_massadd_stg b
                            WHERE
                                execution_id = gv_execution_id
                            GROUP BY
                                interface_line_number
                            HAVING
                                COUNT(interface_line_number) > 1
                        )
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Interface Line Number is validated');
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg st
                    SET
                        book_type_code = (
                            SELECT
                                vt.fa_sub_ledger
                            FROM
                                xxcnv_gl_le_bu_mapping            vt,
                                xxcnv_fa_c013_fa_massadd_dist_stg dist
                            WHERE
                                    st.interface_line_number = dist.interface_line_number
                                AND vt.ns_legal_entity_id = dist.deprn_expense_ccid_segment1
                        )
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        transaction_name = 'NetSuite Conversion Asset Addition',
                        asset_type = 'CAPITALIZED',
                        posting_status = 'POST',
                        queue_name = 'POST',
                        feeder_system_name = 'NETSUITE CONVERSION',
                        asset_key_segment1 = 'NONE',
                        method_code = 'STL',
                        inventorial = 'NO',
                        owned_leased = 'OWNED'
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Default Values are updated');
                END;

       -- Validate Asset Description    
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Asset Description cannot be blank'
                    WHERE
                        description IS NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Description is validated');
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        description = replace(description, '"', '')
                    WHERE
                            1 = 1
                        AND description IS NOT NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Description is trimmed');
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        description = substr(description, 1, 76)
                    WHERE
                            1 = 1
                        AND description IS NOT NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Description is trimmed');
                END;

     --Validate Asset Description with comma's

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        description = '"'
                                      || description
                                      || '"'
                    WHERE
                        description LIKE '%,%'
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Description With Comma is validated');
                END;

	    -- Validate Fixed_Assets_Cost    
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Cost should not be null'
                    WHERE
                        fixed_assets_cost IS NULL
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Cost is validated');
                END;

	    -- Validate Date Placed in Service
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Date Placed in Service should not be null'
                    WHERE
                        date_placed_in_service IS NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Date Placed in Service is validated');
                END;


    -- Validate Prorate Convention	
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Prorate Convention should not be null'
                    WHERE
                        prorate_convention_code IS NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Prorate Convention is validated');
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Prorate Convention value is not valid'
                    WHERE
                        prorate_convention_code IS NOT NULL
                        AND prorate_convention_code NOT IN ( 'DAILY', 'CURRENT_MONTH', 'CUR_MONTH', 'FOL_MONTH' )
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Prorate Convention is validated');
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        prorate_convention_code = 'CUR_MONTH'
                    WHERE
                            prorate_convention_code = 'CURRENT_MONTH'
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Prorate Convention is validated');
                END;

	   -- Validate Asset Units
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Asset Units must be a whole number'
                    WHERE
                        fixed_assets_units IS NULL
                        OR NOT ( round(fixed_assets_units, 0) = fixed_assets_units )
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Asset Units is validated');
                END;

	   -- Update Asset Category Segment1, asset category segment2
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg stg
                    SET
                        stg.asset_category_id_segment1 = (
                            SELECT
                                amt.asset_category_id_segment1
                            FROM
                                xxcnv_fa_asset_category_mapping amt
                            WHERE
                                substr(amt.ns_asset_category, 1, 5) = substr(stg.asset_category_id_segment1, 1, 5)
                        ),
                        stg.asset_category_id_segment2 = (
                            SELECT
                                amt.asset_category_id_segment2
                            FROM
                                xxcnv_fa_asset_category_mapping amt
                            WHERE
                                substr(amt.ns_asset_category, 1, 5) = substr(stg.asset_category_id_segment1, 1, 5)
                        )
                    WHERE
                        stg.asset_category_id_segment1 IS NOT NULL
                        AND stg.execution_id = gv_execution_id
                        AND stg.file_reference_identifier IS NULL;

                    dbms_output.put_line('Asset Category Segment1 is updated');
                END;

	    -- Validate Asset Category Segment1
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Asset Category Segment1 cannot be blank'
                    WHERE
                        asset_category_id_segment1 IS NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Asset Category Segment1 is validated');
                END;


	    -- Validate IN_USE 
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|In_Use flag value should be Y or N'
                    WHERE
                        nvl(in_use_flag, 'Z') NOT IN ( 'Y', 'N' )
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('In Use flag is validated');
                END;

	    -- UPDATE IN_USE 
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        in_use_flag = 'YES'
                    WHERE
                            in_use_flag = 'Y'
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('In Use flag is updated');
                END;

	    -- UPDATE IN_USE 
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        in_use_flag = 'NO'
                    WHERE
                            in_use_flag = 'N'
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('In Use flag is updated');
                END;


		-- Validate MAPPING - Depreciate is not YES (Not Land)
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        depreciate_flag = 'YES'
                    WHERE
                            1 = 1
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Depreciate flag is updated');
                END;


		-- Validate Depreciation Reserve
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|YTD Depreciation must not be null'
                    WHERE
                        ytd_deprn IS NULL;

                    dbms_output.put_line('YTD Depreciation is validated');
                END;


		-- Validate Depreciation Reserve
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Depreciation Reserve must not be null'
                    WHERE
                        deprn_reserve IS NULL;

                    dbms_output.put_line('Depreciation Reserve is validated');
                END;

	   -- Validate Life in Months     
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        life_in_months = 1
                    WHERE
                            life_in_months = 0
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Life in Months is validated.');
                END;

	   -- Validate Life in Months     
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Life in Months must be provided and greater than 0'
                    WHERE
                        life_in_months IS NULL
                        OR life_in_months <= 0
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Life in Months is validated.');
                END;

		 --Invoice line Description with commas have been validated
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        invoice_line_description = '"'
                                                   || invoice_line_description
                                                   || '"'
                    WHERE
                        invoice_line_description LIKE '%,%'
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Invoice line Description With Comma is validated');
                END;

		 		  --Erroring out the record in parent table as it doesn't have corresponding child record
                BEGIN
              -- Update the import_status in xxcnv_fa_c013_fa_massadd_dist_stg to 'ERROR' where the interface_line_number IN xxcnv_fa_c013_fa_massadd_stg  has import_status 'ERROR'
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Child record not found in xxcnv_fa_c013_fa_massadd_dist_stg table'
                    WHERE
                        interface_line_number NOT IN (
                            SELECT
                                interface_line_number
                            FROM
                                xxcnv_fa_c013_fa_massadd_dist_stg
                            WHERE
                                file_reference_identifier IS NULL
                        )
                        AND execution_id = gv_execution_id;
			 -- and file_reference_identifier is null
                END;




              -- Update import_status based on error_message
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        loading_status =
                            CASE
                                WHEN error_message IS NOT NULL THEN
                                    'ERROR'
                                ELSE
                                    'PROCESSED'
                            END
                    WHERE
                            execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('import_status is validated');
                END;

                dbms_output.put_line('file_reference_identifier CHECK1');

     -- Final update to set error_message and loading_status
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = ltrim(error_message, ','),
                        loading_status =
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
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        source_system = gv_conversion_id
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('source_system is updated');
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        file_name = gv_oci_file_name_addition
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('file_name column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                        error_message IS NOT NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('file_reference_identifier column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
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

    --Check if there are any error messages
                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_fa_c013_fa_massadd_stg
                WHERE
                    error_message IS NOT NULL
                    AND file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                IF lv_error_count > 0 THEN

	    -- Logging the message If data is not validated
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_failed,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_addition,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                ELSE
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_validated,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_addition,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

                IF gv_oci_file_name_addition IS NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_file_not_found,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_addition,
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
            lv_row_count := 0;
            UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
            SET
                execution_id = gv_execution_id,
                batch_id = gv_batch_id
            WHERE
                file_reference_identifier IS NULL;
          --END;

            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_fa_c013_fa_massadd_dist_stg
            WHERE
                execution_id = gv_execution_id;

            IF lv_row_count <> 0 THEN
                BEGIN
        -- Initialize error_message to an empty string if it is NULL
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        error_message = ''
                    WHERE
                        error_message IS NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('An error occurred while initializing error_message: '
                                             || '->'
                                             || substr(sqlerrm, 1, 3000)
                                             || '->'
                                             || dbms_utility.format_error_backtrace);
                END;


		  --Erroring out the record in child table as it errored out in parent table
                BEGIN
              -- Update the import_status in xxcnv_fa_c013_fa_massadd_dist_stg to 'ERROR' where the interface_line_number IN xxcnv_fa_c013_fa_massadd_stg  has import_status 'ERROR'
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        error_message = error_message || '|Parent Record failed at validation'
                    WHERE
                        interface_line_number IN (
                            SELECT
                                interface_line_number
                            FROM
                                xxcnv_fa_c013_fa_massadd_stg
                            WHERE
                                    loading_status = 'ERROR'
                                AND execution_id = gv_execution_id
                        )
                        AND execution_id = gv_execution_id;
			 -- and file_reference_identifier is null
                END;

		  --Erroring out the record in child table as it doesn't have corresponding parent record
                BEGIN
              -- Update the import_status in xxcnv_fa_c013_fa_massadd_dist_stg to 'ERROR' where the interface_line_number IN xxcnv_fa_c013_fa_massadd_stg  has import_status 'ERROR'
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        error_message = error_message || '|Parent Record not found in xxcnv_fa_c013_fa_massadd_stg table'
                    WHERE
                        interface_line_number NOT IN (
                            SELECT
                                interface_line_number
                            FROM
                                xxcnv_fa_c013_fa_massadd_stg
                            WHERE
                                execution_id = gv_execution_id
                        )
                        AND execution_id = gv_execution_id;
			 -- and file_reference_identifier is null
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        error_message = error_message || '|Duplicate interface_line_number in xxcnv_fa_c013_fa_massadd_dist_stg table'
                    WHERE
                        interface_line_number IS NOT NULL
                        AND interface_line_number IN (
                            SELECT
                                interface_line_number
                            FROM
                                xxcnv_fa_c013_fa_massadd_dist_stg
                            WHERE
                                interface_line_number IS NOT NULL
                                AND execution_id = gv_execution_id
                            GROUP BY
                                interface_line_number
                            HAVING
                                COUNT(1) > 1
                        );

                END;




	       -- Validate Units NUMBER
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        error_message = error_message || '|Units_Assigned should not be null'
                    WHERE
                        units_number IS NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('UNITS_NUMBER is validated');
                END;

		   -- Validate Asset Location Segment1
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        error_message = error_message || '|Asset Location Segment1 should not be null'
                    WHERE
                        location_id_segment1 IS NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('location_id_segment1 is validated');
                END;

		--Update Asset Location Segments 1 to 6
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg stg
                    SET
                        location_id_segment2 = (
                            SELECT
                                lmt.country
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
                            WHERE
                                nvl(lmt.netsuite_id, 'ZZ') = stg.location_id_segment1
                        ),
                        location_id_segment3 = (
                            SELECT
                                lmt.state
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
                            WHERE
                                nvl(lmt.netsuite_id, 'ZZ') = stg.location_id_segment1
                        ),
                        location_id_segment4 = (
                            SELECT
                                lmt.county
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
                            WHERE
                                nvl(lmt.netsuite_id, 'ZZ') = stg.location_id_segment1
                        ),
                        location_id_segment5 = (
                            SELECT
                                lmt.city
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
                            WHERE
                                nvl(lmt.netsuite_id, 'ZZ') = stg.location_id_segment1
                        ),
                        location_id_segment6 = (
                            SELECT
                                lmt.zip
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
                            WHERE
                                nvl(lmt.netsuite_id, 'ZZ') = stg.location_id_segment1
                        ),
                        location_id_segment1 = (
                            SELECT
                                lmt.ee_site
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
                            WHERE
                                nvl(lmt.netsuite_id, 'ZZ') = stg.location_id_segment1
                        )
                    WHERE
                            execution_id = gv_execution_id
                        AND location_id_segment1 IS NOT NULL
                        AND location_id_segment1 != '999'
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Asset Location Segment1 is updated');
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for location id');
                    WHEN OTHERS THEN
                        dbms_output.put_line('An error occurred while fetching location segments');
                END;

		--Update Asset Location Segments 1 to 6
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg stg
                    SET
                        location_id_segment2 = (
                            SELECT
                                lmt.country
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
                            WHERE
                                nvl(lmt.deprn_seg1, 'ZZ') = stg.deprn_expense_ccid_segment1
                        ),
                        location_id_segment3 = (
                            SELECT
                                lmt.state
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
                            WHERE
                                nvl(lmt.deprn_seg1, 'ZZ') = stg.deprn_expense_ccid_segment1
                        ),
                        location_id_segment4 = (
                            SELECT
                                lmt.county
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
                            WHERE
                                nvl(lmt.deprn_seg1, 'ZZ') = stg.deprn_expense_ccid_segment1
                        ),
                        location_id_segment5 = (
                            SELECT
                                lmt.city
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
                            WHERE
                                nvl(lmt.deprn_seg1, 'ZZ') = stg.deprn_expense_ccid_segment1
                        ),
                        location_id_segment6 = (
                            SELECT
                                lmt.zip
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
                            WHERE
                                nvl(lmt.deprn_seg1, 'ZZ') = stg.deprn_expense_ccid_segment1
                        ),
                        location_id_segment1 = (
                            SELECT
                                lmt.ee_site
                            FROM
                                xxcnv_fa_location_segment_mapping lmt
			                                 -- WHERE NVL(lmt.netsuite_id, 'ZZ') = stg.deprn_expense_ccid_segment1)	 --commented for v1.2
                            WHERE
                                nvl(lmt.deprn_seg1, 'ZZ') = stg.deprn_expense_ccid_segment1
                        )     -- added for v1.2
                    WHERE
                            execution_id = gv_execution_id
                        AND location_id_segment1 = '999'
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Asset Location Segment1 is updated for values 999');
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for location id');
                    WHEN OTHERS THEN
                        dbms_output.put_line('An error occurred while fetching location segments');
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        location_id_segment3 = '"'
                                               || location_id_segment3
                                               || '"'
                    WHERE
                        location_id_segment3 LIKE '%,%'
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('location_id_segment3 With Comma is validated');
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        error_message = error_message || '|Not able to fetch the location segments'
                    WHERE
                        location_id_segment1 IS NULL
                        AND location_id_segment2 IS NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Location segments are validated');
                END;



		   -- Validate Depreciation Expense Account Segment1
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        error_message = error_message || '|Depreciation Expense Account Segment1 should not be null'
                    WHERE
                        deprn_expense_ccid_segment1 IS NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Depreciation Account Expense Segment1 is validated');
                END;

		   -- Validate Depreciation Account Expense Segment4
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        error_message = error_message || '|Depreciation Account Expense Segment4 should not be null'
                    WHERE
                        deprn_expense_ccid_segment4 IS NULL
                        AND execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Depreciation Account Expense Segment4 is validated');
                END;

        -- Update import_status based on error_message
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        loading_status =
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

     -- Final update to set error_message and loading_status
                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        error_message = ltrim(error_message, ','),
                        loading_status =
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

		  --Erroring out the record in child table as it errored out in parent table
                BEGIN
              -- Update the import_status in xxcnv_fa_c013_fa_massadd_dist_stg to 'ERROR' where the interface_line_number IN xxcnv_fa_c013_fa_massadd_stg  has import_status 'ERROR'
                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || '|Child record failed at validation',
                        loading_status = 'ERROR',
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                        interface_line_number IN (
                            SELECT
                                interface_line_number
                            FROM
                                xxcnv_fa_c013_fa_massadd_dist_stg
                            WHERE
                                    loading_status = 'ERROR'
                                AND execution_id = gv_execution_id
                        )
                        AND execution_id = gv_execution_id;
			 -- and file_reference_identifier is null
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        file_name = gv_oci_file_name_distribution
                    WHERE
                            execution_id = gv_execution_id
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('file_name column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        source_system = gv_conversion_id
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('source_system is updated');
                END;

                UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_failure
                WHERE
                    file_reference_identifier IS NULL
                    AND error_message IS NOT NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_reference_identifier column is updated');
                UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                SET
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success
                WHERE
                    error_message IS NULL
                    AND file_reference_identifier IS NULL
                    AND execution_id = gv_execution_id;

                dbms_output.put_line('file_reference_identifier column is updated');

		-- Check if there are any error messages
                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_fa_c013_fa_massadd_dist_stg
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
                        p_file_name         => gv_oci_file_name_distribution,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                ELSE
	       -- Logging the message If data is validated
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_validated,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_distribution,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

                IF gv_oci_file_name_distribution IS NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_file_not_found,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_distribution,
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
-- PROCEDURE : COA_TARGET_SEGMENTS_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used .
================================================================================================================================= */
    PROCEDURE coa_target_segments_prc IS

        lv_status           VARCHAR2(100);
        lv_message          VARCHAR2(2000);
        lv_target_segment   VARCHAR2(200);
        lv_error_message    VARCHAR2(500);
        lv_target_segment1  VARCHAR2(100);
        lv_target_segment2  VARCHAR2(25);
        lv_target_segment3  VARCHAR2(25);
        lv_target_segment4  VARCHAR2(25);
        lv_target_segment5  VARCHAR2(25);
        lv_target_segment6  VARCHAR2(25);
        lv_target_segment7  VARCHAR2(25);
        lv_target_segment8  VARCHAR2(25);
        lv_target_segment9  VARCHAR2(25);
        lv_target_segment10 VARCHAR2(25);
        lv_source_coa       VARCHAR2(100);
        lv_target_coa       VARCHAR2(100);
        lv_not_transformed  VARCHAR2(200) := 'COA_NOT_TRANSFORMED';
        lv_pkg_name         VARCHAR2(10) := 'FA';
    BEGIN
        FOR rec IN (
            SELECT
                x.rowid AS identifier,
                x.*,
                y.date_placed_in_service,
                y.rowid AS y_identifier
            FROM
                xxcnv_fa_c013_fa_massadd_dist_stg x,
                xxcnv_fa_c013_fa_massadd_stg      y
            WHERE
                    x.file_reference_identifier = gv_execution_id
                                                  || '_'
                                                  || gv_status_success
                AND y.file_reference_identifier = gv_execution_id
                || '_'
                || gv_status_success
                   AND x.interface_line_number = y.interface_line_number
        ) LOOP
            BEGIN
                dbms_output.put_line('Entering For loop' || rec.identifier);

                -- Call the COA_TRANSFORMATION_PKG for each row
                xxcnv_gl_coa_transformation_pkg.coa_segment_mapping_prc(
                    p_in_segment1       => rec.deprn_expense_ccid_segment1,
                    p_in_segment2       => rec.deprn_expense_ccid_segment2,
                    p_in_segment3       => rec.deprn_expense_ccid_segment3,
                    p_in_segment4       => rec.deprn_expense_ccid_segment4,
                    p_in_segment5       => rec.deprn_expense_ccid_segment5,
                    p_in_segment6       => rec.deprn_expense_ccid_segment6,
                    p_in_segment7       => rec.deprn_expense_ccid_segment7,
                    p_in_segment8       => rec.deprn_expense_ccid_segment8,
                    p_in_segment9       => rec.deprn_expense_ccid_segment9,
                    p_in_segment10      => rec.deprn_expense_ccid_segment10,
                    p_out_target_system => lv_target_segment,
                    p_out_status        => lv_status,
                    p_out_message       => lv_message,
                    p_in_pkg_name       => lv_pkg_name
                );

                dbms_output.put_line('Completed Successfully');
                dbms_output.put_line('Target Segment: ' || lv_target_segment);
                dbms_output.put_line('Status: ' || lv_status);
                dbms_output.put_line('Message: ' || lv_message);
                IF lv_status = 'SUCCESS' THEN
                    dbms_output.put_line('Mapping Target Segments: ' || lv_target_segment);
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

                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
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
                        ROWID = rec.identifier;

                    dbms_output.put_line('Successfully transformed segments for record interface_line_number: ' || rec.interface_line_number
                    );
                ELSE
                    dbms_output.put_line('Source segments are not transformed successfully, so we cannot map the target segments');
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_coa_transformation_failed,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_file_name,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => lv_message,
                        p_process_reference => NULL
                    );

                    UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                    SET
                        error_message = error_message || lv_message,
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                        ROWID = rec.identifier;

                    BEGIN
                        UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
                        SET
                            loading_status =
                                CASE
                                    WHEN error_message IS NOT NULL THEN
                                        'ERROR'
                                    ELSE
                                        'PROCESSED'
                                END
                        WHERE
                            ROWID = rec.identifier;

                        dbms_output.put_line('import_status is validated');
                    END;

                    UPDATE xxcnv_fa_c013_fa_massadd_stg
                    SET
                        error_message = error_message || lv_message,
                        file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_failure
                    WHERE
                        ROWID = rec.y_identifier;

                    BEGIN
                        UPDATE xxcnv_fa_c013_fa_massadd_stg
                        SET
                            loading_status =
                                CASE
                                    WHEN error_message IS NOT NULL THEN
                                        'ERROR'
                                    ELSE
                                        'PROCESSED'
                                END
                        WHERE
                            ROWID = rec.y_identifier;

                        dbms_output.put_line('import_status is validated');
                    END;

                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    lv_error_message := '->'
                                        || substr(sqlerrm, 1, 3000)
                                        || '->'
                                        || dbms_utility.format_error_backtrace;

                    dbms_output.put_line('Completed With Error: ' || lv_error_message);
                    dbms_output.put_line('Error transforming segments for record interface_line_number: '
                                         || rec.interface_line_number
                                         || ' - '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);
                    --RETURN;
            END;
        END LOOP;

        BEGIN
            UPDATE xxcnv_fa_c013_fa_massadd_dist_stg
            SET
                error_message = error_message || lv_not_transformed,
                file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure
            WHERE
                    file_reference_identifier = gv_execution_id
                                                || '_'
                                                || gv_status_success
                AND target_segment1 IS NULL;

        END;

        dbms_output.put_line('Completed mapping target segments.');
        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
            p_conversion_id     => gv_conversion_id,
            p_execution_id      => gv_execution_id,
            p_execution_step    => gv_coa_transformation,
            p_boundary_system   => gv_boundary_system,
            p_file_path         => gv_oci_file_path,
            p_file_name         => gv_file_name,
            p_attribute1        => gv_book_type_code,
            p_attribute2        => NULL,
            p_process_reference => NULL
        );

    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('An unexpected error occurred: '
                                 || '->'
                                 || substr(sqlerrm, 1, 3000)
                                 || '->'
                                 || dbms_utility.format_error_backtrace);

            RETURN;
    END coa_target_segments_prc;	

/*==============================================================================================================================
-- PROCEDURE : CREATE_FBDI_FILE_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the FBDI CSV file by using the data in the FA_interface Table after all validations.
================================================================================================================================= */
    PROCEDURE create_fbdi_file_prc IS

        CURSOR batch_id_cursor IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_fa_c013_fa_massadd_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_success;

        CURSOR batch_id_cursor_dist IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_fa_c013_fa_massadd_dist_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_success;

        lv_success_count NUMBER;
        lv_batch_id      VARCHAR(200);
    BEGIN
        BEGIN
            FOR g_id IN batch_id_cursor LOOP
                lv_batch_id := g_id.batch_id;
                lv_success_count := 0;
                dbms_output.put_line('In create FBDI Processing Batch_ID: ' || lv_batch_id);
                BEGIN
                    dbms_output.put_line('FBDI CHECK1 ' || lv_batch_id);
            -- Count the number of success records count
                    SELECT
                        COUNT(*)
                    INTO lv_success_count
                    FROM
                        xxcnv_fa_c013_fa_massadd_stg
                    WHERE
                            batch_id = lv_batch_id
                        AND file_reference_identifier = gv_execution_id
                                                        || '_'
                                                        || gv_status_success;

                    dbms_output.put_line('Success record count for batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || lv_batch_id);
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found for batch_id: ' || lv_batch_id);
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
                                             || gv_oci_file_name_addition,
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv',
                                        'trimspaces' VALUE 'rtrim',
                                        'maxfilesize' VALUE '629145600',
                                        'header' VALUE FALSE
                                    ),
                            query           => 'SELECT 
                    interface_line_number                 ,
                    Book_type_code                        ,
                    Transaction_Name                      ,
                    Asset_Number                          ,
                    Description                           ,                 
                    Tag_Number                            ,
                    Manufacturer_Name                     ,
                    Serial_Number                         ,
                    Model_Number                          ,
                    Asset_Type                            ,
                    Fixed_Assets_Cost                     ,
           -- TO_CHAR(TO_DATE(Date_Placed_In_Service,''DD/MM/YYYY''), ''YYYY/MM/DD'') AS     Date_Placed_In_Service , --commented for v1.2
           TO_CHAR(TO_DATE(Date_Placed_In_Service,''MM/DD/YYYY''), ''YYYY/MM/DD'') AS     Date_Placed_In_Service , --added for v1.2
                    Prorate_Convention_Code               ,
                    Fixed_Assets_Units                    ,
                    asset_category_id_segment1            ,
                    asset_category_id_segment2            ,
                    asset_category_id_segment3            ,
                    asset_category_id_segment4            ,
                    asset_category_id_segment5            ,
                    asset_category_id_segment6            ,
                    asset_category_id_segment7            ,
                    posting_status                        ,
                    queue_name                            ,
                    feeder_system_name                    ,
                    parent_asset_number                   ,
                    add_to_asset_number                   ,
                    asset_key_segment1                    ,
                    asset_key_segment2                    ,
                    asset_key_segment3                    ,
                    asset_key_segment4                    ,
                    asset_key_segment5                    ,
                    asset_key_segment6                    ,
                    asset_key_segment7                    ,
                    asset_key_segment8                    ,
                    asset_key_segment9                    ,
                    asset_key_segment10                   ,
                    inventorial                           ,
                    property_type_code                    ,
                    property_1245_1250_code               ,
                    in_use_flag                           ,
                    owned_leased                          ,
                    new_used                              ,
                    material_indicator_flag               ,
                    commitment                            ,
                    investment_law                        ,
                    amortize_flag                         ,
                    amortization_start_date               ,
                    depreciate_flag                       ,
                    salvage_type                          ,
                    salvage_value                         ,
                    percent_salvage_value                 ,
                    ytd_deprn                             ,
                    deprn_reserve                         ,
                    bonus_ytd_deprn                       ,
                    bonus_deprn_reserve                   ,
                    ytd_impairment                        ,
                     impairment_reserve                   ,
                    method_code                           ,
                    life_in_months                        ,
                    basic_rate                            ,
                    adjusted_rate                         ,
                    unit_of_measure                       ,
                    production_capacity                   ,
                    ceiling_type                          ,
                    bonus_rule                            ,
                    cash_generating_unit                  ,
                    deprn_limit_type                      ,
                    allowed_deprn_limit                   ,
                    allowed_deprn_limit_amount            ,
                    payables_cost                         ,
                    payables_code_combination_id_segment1 ,
                    payables_code_combination_id_segment2 ,
                    payables_code_combination_id_segment3 ,
                    payables_code_combination_id_segment4 ,
                    payables_code_combination_id_segment5 ,
                    payables_code_combination_id_segment6 ,
                    payables_code_combination_id_segment7 ,
                    payables_code_combination_id_segment8 ,
                    payables_code_combination_id_segment9 ,
                    payables_code_combination_id_segment10,
                    payables_code_combination_id_segment11,
                    payables_code_combination_id_segment12,
                    payables_code_combination_id_segment13,
                    payables_code_combination_id_segment14,
                    payables_code_combination_id_segment15,
                    payables_code_combination_id_segment16,
                    payables_code_combination_id_segment17,
                    payables_code_combination_id_segment18,
                    payables_code_combination_id_segment19,
                    payables_code_combination_id_segment20,
                    payables_code_combination_id_segment21,
                    payables_code_combination_id_segment22,
                    payables_code_combination_id_segment23,
                    payables_code_combination_id_segment24,
                    payables_code_combination_id_segment25,
                    payables_code_combination_id_segment26,
                    payables_code_combination_id_segment27,
                    payables_code_combination_id_segment28,
                    payables_code_combination_id_segment29,
                    payables_code_combination_id_segment30,
                    attribute1                            ,
                    attribute2                            ,
                    attribute3                            ,
                    attribute4                            ,
                    attribute5                            ,
                    attribute6                            ,
                    attribute7                            ,
                    attribute8                            ,
                    attribute9                            ,
                    attribute10                           ,
                    attribute11                           ,
                    attribute12                           ,
                    attribute13                           ,
                    attribute14                           ,
                    attribute15                           ,
                    attribute16                           ,
                    attribute17                           ,
                    attribute18                           ,
                    attribute19                           ,
                    attribute20                           ,
                    attribute21                           ,
                    attribute22                           ,
                    attribute23                           ,
                    attribute24                           ,
                    attribute25                           ,
                    attribute26                           ,
                    attribute27                           ,
                    attribute28                           ,
                    attribute29                           ,
                    attribute30                           ,
                    attribute_number1                     ,
                    attribute_number2                     ,
                    attribute_number3                     ,
                    attribute_number4                     ,
                    attribute_number5                     ,
                    attribute_date1					      ,
                    attribute_date2                       ,
                    attribute_date3                       ,
                    attribute_date4                       ,
                    attribute_date5                       ,
                    attribute_category_code               ,
                    context                               ,
                    th_attribute1                         ,
                    th_attribute2                         ,
                    th_attribute3                         ,
                    th_attribute4                         ,
                    th_attribute5                         ,
                    th_attribute6                         ,
                    th_attribute7                         ,
                    th_attribute8                         ,
                    th_attribute9                         ,
                    th_attribute10                        ,
                    th_attribute11                        ,
                    th_attribute12                        ,
                    th_attribute13                        ,
                    th_attribute14                        ,
                    th_attribute15                        ,
                    th_attribute_number1                  ,
                    th_attribute_number2                  ,
                    th_attribute_number3                  ,
                    th_attribute_number4                  ,
                    th_attribute_number5                  ,
                    th_attribute_date1                    ,
                    th_attribute_date2                    ,
                    th_attribute_date3                    ,
                    th_attribute_date4                    ,
                    th_attribute_date5                    ,
                    th_attribute_category_code            ,
                    th2_attribute1                        ,
                    th2_attribute2                        ,
                    th2_attribute3                        ,
                    th2_attribute4                        ,
                    th2_attribute5                        ,
                    th2_attribute6                        ,
                    th2_attribute7                        ,
                    th2_attribute8                        ,
                    th2_attribute9                        ,
                    th2_attribute10                       ,
                    th2_attribute11                       ,
                    th2_attribute12                       ,
                    th2_attribute13                       ,
                    th2_attribute14                       ,
                    th2_attribute15                       ,
                    th2_attribute_number1                 ,
                    th2_attribute_number2                 ,
                    th2_attribute_number3                 ,
                    th2_attribute_number4                 ,
                    th2_attribute_number5                 ,
                    th2_attribute_date1                   ,
                    th2_attribute_date2                   ,
                    th2_attribute_date3                   ,
                    th2_attribute_date4                   ,
                    th2_attribute_date5                   ,
                    th2_attribute_category_code           , 
                    ai_attribute1                         ,
                    ai_attribute2                         ,
                    ai_attribute3                         ,
                    ai_attribute4                         ,
                    ai_attribute5                         ,
                    ai_attribute6                         ,
                    ai_attribute7                         ,
                    ai_attribute8                         ,
                    ai_attribute9                         ,
                    ai_attribute10                        ,
                    ai_attribute11                        ,
                    ai_attribute12                        ,
                    ai_attribute13                        ,
                    ai_attribute14                        ,
                    ai_attribute15                        ,
                    ai_attribute_number1                  ,
                    ai_attribute_number2                  ,
                    ai_attribute_number3                  ,
                    ai_attribute_number4                  ,
                    ai_attribute_number5                  ,
                    ai_attribute_date1                    ,
                    ai_attribute_date2                    ,
                    ai_attribute_date3                    ,
                    ai_attribute_date4                    ,
                    ai_attribute_date5                    ,
                    ai_attribute_category_code            ,
                    mass_property_flag                    ,
                    group_asset_number                    ,
                    reduction_rate                        ,
                    reduce_addition_flag                  ,
		            Apply_Reduction_Rate_to_Adjustments   ,
                    reduce_retirement_flag                ,
                    recognize_gain_or_loss                ,
                    recapture_reserve_flag                ,
                    limit_proceeds_flag                   ,
                    terminal_gain_or_loss                 ,
                    tracking_method                       ,
                    excess_allocation_option              ,
                    depreciate_option                     ,
                    member_rollup_flag                    ,
                    allocate_to_fully_rsv_flag            ,
                    over_depreciate_option                ,
                    preparer_email_address                ,
                    merged_code                           ,
                    parent_interface_line_number          ,
                    sum_units                             ,
                    new_master_flag                       ,
                    units_to_adjust                       ,
                    short_fiscal_year_flag                ,
                    conversion_date                       ,
                    original_deprn_start_date             ,
                    global_attribute1                     ,
                    global_attribute2                     ,
                    global_attribute3                     ,
                    global_attribute4                     ,
                    global_attribute5                     ,
                    global_attribute6                     ,
                    global_attribute7                     ,
                    global_attribute8                     ,
                    global_attribute9                     ,
                    global_attribute10                    ,
                    global_attribute11                    ,
                    global_attribute12                    ,
                    global_attribute13                    ,
                    global_attribute14                    ,
                    global_attribute15                    ,
                    global_attribute16                    ,
                    global_attribute17                    ,
                    global_attribute18                    ,
                    global_attribute19                    ,
                    global_attribute20                    ,
                    global_attribute_number1              ,
                    global_attribute_number2              ,
                    global_attribute_number3              ,
                    global_attribute_number4              ,
                    global_attribute_number5              ,
                    global_attribute_date1                ,
                    global_attribute_date2                ,
                    global_attribute_date3                ,
                    global_attribute_date4                ,
                    global_attribute_date5                ,
                    global_attribute_category             ,
                    nbv_at_switch                         ,
                    period_name_fully_reserved            ,
                    period_name_extended                  ,
                    prior_deprn_limit_type                ,
                    prior_deprn_limit                     ,
                    prior_deprn_limit_amount              ,
                    prior_method_code                     ,
                    prior_life_in_months                  ,
                    prior_basic_rate                      ,
                    prior_adjusted_rate                   ,
                    asset_schedule_number                 ,
                    lease_number                          ,
                    reval_reserve                         ,
                    reval_loss_blanace                    ,
                    reval_amortization_basis              ,
                    impair_loss_balance                   ,
                    reval_ceiling                         ,
                    fair_market_value                     ,
                    last_price_index_value                ,
                    global_attribute_number6              ,
                    global_attribute_number7              ,
                    global_attribute_number8              ,
                    global_attribute_number9              ,
                    global_attribute_number10             ,
                    global_attribute_date6                ,
                    global_attribute_date7                ,
                    global_attribute_date8                ,
                    global_attribute_date9                ,
                    global_attribute_date10               ,
                    bk_global_attribute1                  ,
                    bk_global_attribute2                  ,
                    bk_global_attribute3                  ,
                    bk_global_attribute4                  ,
                    bk_global_attribute5                  ,
                    bk_global_attribute6                  ,
                    bk_global_attribute7                  ,
                    bk_global_attribute8                  ,
                    bk_global_attribute9                  ,
		            bk_global_attribute10                 ,
		            bk_global_attribute11                 ,
		            bk_global_attribute12                 ,
		            bk_global_attribute13                 ,
		            bk_global_attribute14                 ,
		            bk_global_attribute15                 ,
		            bk_global_attribute16                 ,
		            bk_global_attribute17                 ,
		            bk_global_attribute18                 ,
		            bk_global_attribute19                 ,
		            bk_global_attribute20                 ,
                    BK_GLOBAL_ATTRIBUTE_NUMBER1           ,
		            BK_GLOBAL_ATTRIBUTE_NUMBER2           ,
		            BK_GLOBAL_ATTRIBUTE_NUMBER3           ,
		            BK_GLOBAL_ATTRIBUTE_NUMBER4           ,
		            BK_GLOBAL_ATTRIBUTE_NUMBER5           ,
                    BK_GLOBAL_ATTRIBUTE_DATE1             ,
		            BK_GLOBAL_ATTRIBUTE_DATE2             ,
		            BK_GLOBAL_ATTRIBUTE_DATE3             ,
		            BK_GLOBAL_ATTRIBUTE_DATE4             ,
		            BK_GLOBAL_ATTRIBUTE_DATE5             ,
                    BK_GLOBAL_ATTRIBUTE_CATEGORY          ,
		            th_global_attribute1                  ,
		            th_global_attribute2                  ,
		            th_global_attribute3                  ,
		            th_global_attribute4                  ,
		            th_global_attribute5                  ,
		            th_global_attribute6                  ,
		            th_global_attribute7                  ,
		            th_global_attribute8                  ,
		            th_global_attribute9                  ,
		            th_global_attribute10                 ,
		            th_global_attribute11               	,
                    th_global_attribute12                 ,
                    th_global_attribute13                 ,
                    th_global_attribute14                 ,
                    th_global_attribute15                 ,
                    th_global_attribute16                 ,
                    th_global_attribute17                 ,
                    th_global_attribute18                 ,
                    th_global_attribute19                 ,
                    th_global_attribute20                 ,
                    th_global_attribute_number1           ,
                    th_global_attribute_number2           ,
                    th_global_attribute_number3           ,
                    th_global_attribute_number4           ,
                    th_global_attribute_number5           ,
                    th_global_attribute_date1             ,
                    th_global_attribute_date2             ,
                    th_global_attribute_date3             ,
                    th_global_attribute_date4             ,
                    th_global_attribute_date5             ,
                    th_global_attribute_category          ,
                    ai_global_attribute1                  ,
                    ai_global_attribute2                  ,
                    ai_global_attribute3                  ,
                    ai_global_attribute4                  ,
                    ai_global_attribute5                  ,
                    ai_global_attribute6                  ,
                    ai_global_attribute7                  ,
                    ai_global_attribute8                  ,
                    ai_global_attribute9                  ,
                    ai_global_attribute10                 ,
                    ai_global_attribute11                 ,
                    ai_global_attribute12                 ,
                    ai_global_attribute13                 ,
                    ai_global_attribute14                 ,
                    ai_global_attribute15                 ,
                    ai_global_attribute16                 ,
                    ai_global_attribute17                 ,
                    ai_global_attribute18                 ,
                    ai_global_attribute19                 ,
                    ai_global_attribute20                 ,
                    ai_global_attribute_number1           ,
                    ai_global_attribute_number2           ,
                    ai_global_attribute_number3           ,
                    ai_global_attribute_number4           ,
                    ai_global_attribute_number5           ,
                    ai_global_attribute_date1             ,
                    ai_global_attribute_date2             ,
                    ai_global_attribute_date3             ,
                    ai_global_attribute_date4             ,
                    ai_global_attribute_date5             ,
                    ai_global_attribute_category          ,
                    vendor_name                           ,
                    vendor_number                         ,
                    po_number                             ,
                    invoice_number                        ,
                    invoice_voucher_number                ,
                    invoice_date                          ,
                    payables_units                        ,
                    invoice_line_number                   ,
                    invoice_line_type                     ,
                    invoice_line_description              ,
                    invoice_payment_number                ,
                    project_number                        ,
                    project_task_number                   ,
                    fully_reserve_on_add_flag             ,
                    deprn_adjustment_factor               
                                    FROM xxcnv_fa_c013_fa_massadd_stg
                                     WHERE loading_status = '''
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

                        dbms_output.put_line(' CSV file for batch_id'
                                             || lv_batch_id
                                             || ' exported for xxcnv_fa_c013_fa_massadd_stg successfully to OCI Object Storage.');
                        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                            p_conversion_id     => gv_conversion_id,
                            p_execution_id      => gv_execution_id,
                            p_execution_step    => gv_fbdi_export_status,
                            p_boundary_system   => gv_boundary_system,
                            p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                            p_file_name         => lv_batch_id
                                           || '_'
                                           || gv_oci_file_name_addition,
                            p_attribute1        => lv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );

                    EXCEPTION
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error exporting data to CSV for xxcnv_fa_c013_fa_massadd_stg batch_id '
                                                 || lv_batch_id
                                                 || ': '
                                                 || sqlerrm);
                            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                                p_conversion_id     => gv_conversion_id,
                                p_execution_id      => gv_execution_id,
                                p_execution_step    => gv_fbdi_export_status_fail,
                                p_boundary_system   => gv_boundary_system,
                                p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                                p_file_name         => lv_batch_id
                                               || '_'
                                               || gv_oci_file_name_addition,
                                p_attribute1        => lv_batch_id,
                                p_attribute2        => NULL,
                                p_process_reference => NULL
                            );

                            RETURN;
                    END;
                ELSE
                    dbms_output.put_line('Process Stopped for xxcnv_fa_c013_fa_massadd_stg batch_id '
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
                FOR g_id IN batch_id_cursor_dist LOOP
                    lv_batch_id := g_id.batch_id;
                    lv_success_count := 0;
                    dbms_output.put_line('Processing Batch_ID: ' || lv_batch_id);
                    BEGIN
                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                        SELECT
                            COUNT(*)
                        INTO lv_success_count
                        FROM
                            xxcnv_fa_c013_fa_massadd_dist_stg
                        WHERE
                                batch_id = lv_batch_id
                            AND file_reference_identifier = gv_execution_id
                                                            || '_'
                                                            || gv_status_success;

                        dbms_output.put_line('Success record count for xxcnv_fa_c013_fa_massadd_dist_stg for batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || lv_success_count);
                    EXCEPTION
                        WHEN no_data_found THEN
                            dbms_output.put_line('No data found for xxcnv_fa_c013_fa_massadd_dist_stg batch_id: ' || lv_batch_id);
                            RETURN;
                        WHEN OTHERS THEN
                            dbms_output.put_line('Error checking Success record count for batch_id '
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
                                                 || gv_oci_file_name_distribution,
                                format          =>
                                        JSON_OBJECT(
                                            'type' VALUE 'csv',
                                            'trimspaces' VALUE 'rtrim',
                                            'maxfilesize' VALUE '629145600',
                                            'header' VALUE FALSE
                                        ),
                                query           => 'SELECT 
                                    interface_line_number        , 
									 UNITS_NUMBER                 ,     	
									 Assigned_to                  ,
									 location_id_Segment1         ,
									 location_id_Segment2         ,
									 location_id_Segment3         ,
									 location_id_Segment4         ,
									 location_id_Segment5         ,
									 location_id_Segment6         ,
									 location_id_Segment7         ,
									 target_segment1 AS Deprn_Expense_CCID_Segment1  ,
									 target_segment2 AS Deprn_Expense_CCID_Segment2  ,
									 target_segment3 AS Deprn_Expense_CCID_Segment3  ,
									 target_segment4 AS Deprn_Expense_CCID_Segment4  ,
									 target_segment5 AS Deprn_Expense_CCID_Segment5  ,
									 target_segment6 AS Deprn_Expense_CCID_Segment6  ,
									 target_segment7 AS Deprn_Expense_CCID_Segment7  ,
									 target_segment8 AS Deprn_Expense_CCID_Segment8  ,
									 target_segment9 AS Deprn_Expense_CCID_Segment9  ,
									 target_segment10 ASDeprn_Expense_CCID_Segment10 ,
									 Deprn_Expense_CCID_Segment11 ,
									 Deprn_Expense_CCID_Segment12 ,
									 Deprn_Expense_CCID_Segment13 ,
									 Deprn_Expense_CCID_Segment14 ,
									 Deprn_Expense_CCID_Segment15 ,
									 Deprn_Expense_CCID_Segment16 ,
									 Deprn_Expense_CCID_Segment17 ,
									 Deprn_Expense_CCID_Segment18 ,
									 Deprn_Expense_CCID_Segment19 ,
									 Deprn_Expense_CCID_Segment20 ,
									 Deprn_Expense_CCID_Segment21 ,
									 Deprn_Expense_CCID_Segment22 ,
									 Deprn_Expense_CCID_Segment23 ,
									 Deprn_Expense_CCID_Segment24 ,
									 Deprn_Expense_CCID_Segment25 ,
									 Deprn_Expense_CCID_Segment26 ,
									 Deprn_Expense_CCID_Segment27 ,
									 Deprn_Expense_CCID_Segment28 ,
									 Deprn_Expense_CCID_Segment29 ,
									 Deprn_Expense_CCID_Segment30 
                                            FROM xxcnv_fa_c013_fa_massadd_dist_stg
                                            WHERE loading_status = '''
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
                                                 || ' exported successfully to MassDistributions OCI Object Storage.');
                            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                                p_conversion_id     => gv_conversion_id,
                                p_execution_id      => gv_execution_id,
                                p_execution_step    => gv_fbdi_export_status,
                                p_boundary_system   => gv_boundary_system,
                                p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                                p_file_name         => lv_batch_id
                                               || '_'
                                               || gv_oci_file_name_distribution,
                                p_attribute1        => lv_batch_id,
                                p_attribute2        => NULL,
                                p_process_reference => NULL
                            );

                        EXCEPTION
                            WHEN OTHERS THEN
                                dbms_output.put_line('Error exporting data to CSV for xxcnv_fa_c013_fa_massadd_dist_stg batch_id '
                                                     || lv_batch_id
                                                     || ': '
                                                     || sqlerrm);
                                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                                    p_conversion_id     => gv_conversion_id,
                                    p_execution_id      => gv_execution_id,
                                    p_execution_step    => gv_fbdi_export_status_fail,
                                    p_boundary_system   => gv_boundary_system,
                                    p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                                    p_file_name         => lv_batch_id
                                                   || '_'
                                                   || gv_oci_file_name_distribution,
                                    p_attribute1        => lv_batch_id,
                                    p_attribute2        => NULL,
                                    p_process_reference => NULL
                                );

                                RETURN;
                        END;
                    ELSE
                        dbms_output.put_line('Process Stopped for xxcnv_fa_c013_fa_massadd_dist_stg batch_id '
                                             || lv_batch_id
                                             || ': Error message columns contain data.');
                        RETURN;
                    END IF;

                END LOOP;

                dbms_output.put_line('FBDI created ' || lv_batch_id);
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
-- PROCEDURE : CREATE_PROPERTIES_FILE_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating properties file.
================================================================================================================================= */
    PROCEDURE create_properties_file_prc IS

        CURSOR book_type_code_cursor IS
        SELECT DISTINCT
            book_type_code
        FROM
            xxcnv_fa_c013_fa_massadd_stg
        WHERE
            execution_id = gv_execution_id;
		--file_reference_identifier = gv_execution_id||'_'||gv_status_success;

        lv_error_count    NUMBER;
        lv_book_type_code VARCHAR(250);
    BEGIN
        FOR g_id IN book_type_code_cursor LOOP
            lv_book_type_code := g_id.book_type_code;
            dbms_output.put_line('Processing book_type_code: ' || lv_book_type_code);
            BEGIN
                dbms_cloud.export_data(
                    credential_name => gv_credential_name,
                    file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                     || '/'
                                     || gv_batch_id
                                     || lv_book_type_code
                                     || 'fixedassetsimport.properties',
                    format          =>
                            JSON_OBJECT(
                                'trimspaces' VALUE 'rtrim'
                            ),
                    query           => 'SELECT ''/oracle/apps/ess/financials/assets/additions/,PostMassAdditions,fixedassetsimport,'
                             || lv_book_type_code
                             || ',null,NORMAL,null,null,null,null,null''as column1 from dual'
                );

                dbms_output.put_line('Properties file for book_type_code '
                                     || lv_book_type_code
                                     || ' exported successfully to OCI Object Storage.');
                xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                    p_conversion_id     => gv_conversion_id,
                    p_execution_id      => gv_execution_id,
                    p_execution_step    => gv_status_staged,
                    p_boundary_system   => gv_boundary_system,
                    p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                    p_file_name         => gv_batch_id
                                   || lv_book_type_code
                                   || '_'
                                   || 'fixedassetsimport.properties',
                    p_attribute1        => gv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error exporting data to properties for book_type_code '
                                         || lv_book_type_code
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
            xxcnv_fa_c013_fa_massadd_stg
        WHERE
                execution_id = gv_execution_id
            AND file_reference_identifier = gv_execution_id
                                            || '_'
                                            || gv_status_failure;

        CURSOR batch_id_cursor_dist IS
        SELECT DISTINCT
            batch_id
        FROM
            xxcnv_fa_c013_fa_massadd_dist_stg
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
            dbms_output.put_line('Processing recon report FOR BATCH_ID: '
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
                                     || 'ATP_Recon_MassAdditions'
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

                    interface_line_number                 ,
                    Book_type_code                        ,
                    Transaction_Name                      ,
                    Asset_Number                          ,
                    Description                           ,
                    Tag_Number                            ,
                    Manufacturer_Name                     ,
                    Serial_Number                         ,
                    Model_Number                          ,
                    Asset_Type                            ,
                    Fixed_Assets_Cost                     ,
            -- TO_CHAR(TO_DATE(Date_Placed_In_Service,''DD/MM/YYYY''), ''YYYY/MM/DD'') AS     Date_Placed_In_Service , --commented for v1.2
            TO_CHAR(TO_DATE(Date_Placed_In_Service,''MM/DD/YYYY''), ''YYYY/MM/DD'') AS     Date_Placed_In_Service ,    -- Added for v1.2
                    Prorate_Convention_Code               ,
                    Fixed_Assets_Units                    ,
                    asset_category_id_segment1            ,
                    asset_category_id_segment2            ,
                    asset_category_id_segment3            ,
                    asset_category_id_segment4            ,
                    asset_category_id_segment5            ,
                    asset_category_id_segment6            ,
                    asset_category_id_segment7            ,
                    posting_status                        ,
                    queue_name                            ,
                    feeder_system_name                    ,
                    parent_asset_number                   ,
                    add_to_asset_number                   ,
                    asset_key_segment1                    ,
                    asset_key_segment2                    ,
                    asset_key_segment3                    ,
                    asset_key_segment4                    ,
                    asset_key_segment5                    ,
                    asset_key_segment6                    ,
                    asset_key_segment7                    ,
                    asset_key_segment8                    ,
                    asset_key_segment9                    ,
                    asset_key_segment10                   ,
                    inventorial                           ,
                    property_type_code                    ,
                    property_1245_1250_code               ,
                    in_use_flag                           ,
                    owned_leased                          ,
                    new_used                              ,
                    material_indicator_flag               ,
                    commitment                            ,
                    investment_law                        ,
                    amortize_flag                         ,
                    amortization_start_date               ,
                    depreciate_flag                       ,
                    salvage_type                          ,
                    salvage_value                         ,
                    percent_salvage_value                 ,
                    ytd_deprn                             ,
                    deprn_reserve                         ,
                    bonus_ytd_deprn                       ,
                    bonus_deprn_reserve                   ,
                    ytd_impairment                        ,
                     impairment_reserve                   ,
                    method_code                           ,
                    life_in_months                        ,
                    basic_rate                            ,
                    adjusted_rate                         ,
                    unit_of_measure                       ,
                    production_capacity                   ,
                    ceiling_type                          ,
                    bonus_rule                            ,
                    cash_generating_unit                  ,
                    deprn_limit_type                      ,
                    allowed_deprn_limit                   ,
                    allowed_deprn_limit_amount            ,
                    payables_cost                         ,
                    payables_code_combination_id_segment1 ,
                    payables_code_combination_id_segment2 ,
                    payables_code_combination_id_segment3 ,
                    payables_code_combination_id_segment4 ,
                    payables_code_combination_id_segment5 ,
                    payables_code_combination_id_segment6 ,
                    payables_code_combination_id_segment7 ,
                    payables_code_combination_id_segment8 ,
                    payables_code_combination_id_segment9 ,
                    payables_code_combination_id_segment10,
                    payables_code_combination_id_segment11,
                    payables_code_combination_id_segment12,
                    payables_code_combination_id_segment13,
                    payables_code_combination_id_segment14,
                    payables_code_combination_id_segment15,
                    payables_code_combination_id_segment16,
                    payables_code_combination_id_segment17,
                    payables_code_combination_id_segment18,
                    payables_code_combination_id_segment19,
                    payables_code_combination_id_segment20,
                    payables_code_combination_id_segment21,
                    payables_code_combination_id_segment22,
                    payables_code_combination_id_segment23,
                    payables_code_combination_id_segment24,
                    payables_code_combination_id_segment25,
                    payables_code_combination_id_segment26,
                    payables_code_combination_id_segment27,
                    payables_code_combination_id_segment28,
                    payables_code_combination_id_segment29,
                    payables_code_combination_id_segment30,
                    attribute1                            ,
                    attribute2                            ,
                    attribute3                            ,
                    attribute4                            ,
                    attribute5                            ,
                    attribute6                            ,
                    attribute7                            ,
                    attribute8                            ,
                    attribute9                            ,
                    attribute10                           ,
                    attribute11                           ,
                    attribute12                           ,
                    attribute13                           ,
                    attribute14                           ,
                    attribute15                           ,
                    attribute16                           ,
                    attribute17                           ,
                    attribute18                           ,
                    attribute19                           ,
                    attribute20                           ,
                    attribute21                           ,
                    attribute22                           ,
                    attribute23                           ,
                    attribute24                           ,
                    attribute25                           ,
                    attribute26                           ,
                    attribute27                           ,
                    attribute28                           ,
                    attribute29                           ,
                    attribute30                           ,
                    attribute_number1                     ,
                    attribute_number2                     ,
                    attribute_number3                     ,
                    attribute_number4                     ,
                    attribute_number5                     ,
                    attribute_date1                       ,
                    attribute_date2                       ,
                    attribute_date3                       ,
                    attribute_date4                       ,
                    attribute_date5                       ,
                    attribute_category_code               ,
                    context                               ,
                    th_attribute1                         ,
                    th_attribute2                         ,
                    th_attribute3                         ,
                    th_attribute4                         ,
                    th_attribute5                         ,
                    th_attribute6                         ,
                    th_attribute7                         ,
                    th_attribute8                         ,
                    th_attribute9                         ,
                    th_attribute10                        ,
                    th_attribute11                        ,
                    th_attribute12                        ,
                    th_attribute13                        ,
                    th_attribute14                        ,
                    th_attribute15                        ,
                    th_attribute_number1                  ,
                    th_attribute_number2                  ,
                    th_attribute_number3                  ,
                    th_attribute_number4                  ,
                    th_attribute_number5                  ,
                    th_attribute_date1                    ,
                    th_attribute_date2                    ,
                    th_attribute_date3                    ,
                    th_attribute_date4                    ,
                    th_attribute_date5                    ,
                    th_attribute_category_code            ,
                    th2_attribute1                        ,
                    th2_attribute2                        ,
                    th2_attribute3                        ,
                    th2_attribute4                        ,
                    th2_attribute5                        ,
                    th2_attribute6                        ,
                    th2_attribute7                        ,
                    th2_attribute8                        ,
                    th2_attribute9                        ,
                    th2_attribute10                       ,
                    th2_attribute11                       ,
                    th2_attribute12                       ,
                    th2_attribute13                       ,
                    th2_attribute14                       ,
                    th2_attribute15                       ,
                    th2_attribute_number1                 ,
                    th2_attribute_number2                 ,
                    th2_attribute_number3                 ,
                    th2_attribute_number4                 ,
                    th2_attribute_number5                 ,
                    th2_attribute_date1                   ,
                    th2_attribute_date2                   ,
                    th2_attribute_date3                   ,
                    th2_attribute_date4                   ,
                    th2_attribute_date5                   ,
                    th2_attribute_category_code           , 
                    ai_attribute1                         ,
                    ai_attribute2                         ,
                    ai_attribute3                         ,
                    ai_attribute4                         ,
                    ai_attribute5                         ,
                    ai_attribute6                         ,
                    ai_attribute7                         ,
                    ai_attribute8                         ,
                    ai_attribute9                         ,
                    ai_attribute10                        ,
                    ai_attribute11                        ,
                    ai_attribute12                        ,
                    ai_attribute13                        ,
                    ai_attribute14                        ,
                    ai_attribute15                        ,
                    ai_attribute_number1                  ,
                    ai_attribute_number2                  ,
                    ai_attribute_number3                  ,
                    ai_attribute_number4                  ,
                    ai_attribute_number5                  ,
                    ai_attribute_date1                    ,
                    ai_attribute_date2                    ,
                    ai_attribute_date3                    ,
                    ai_attribute_date4                    ,
                    ai_attribute_date5                    ,
                    ai_attribute_category_code            ,
                    mass_property_flag                    ,
                    group_asset_number                    ,
                    reduction_rate                        ,
                    reduce_addition_flag                  ,
                    Apply_Reduction_Rate_to_Adjustments   ,
                    reduce_retirement_flag                ,
                    recognize_gain_or_loss                ,
                    recapture_reserve_flag                ,
                    limit_proceeds_flag                   ,
                    terminal_gain_or_loss                 ,
                    tracking_method                       ,
                    excess_allocation_option              ,
                    depreciate_option                     ,
                    member_rollup_flag                    ,
                    allocate_to_fully_rsv_flag            ,
                    over_depreciate_option                ,
                    preparer_email_address                ,
                    merged_code                           ,
                    parent_interface_line_number          ,
                    sum_units                             ,
                    new_master_flag                       ,
                    units_to_adjust                       ,
                    short_fiscal_year_flag                ,
                    conversion_date                       ,
                    original_deprn_start_date             ,
                    global_attribute1                     ,
                    global_attribute2                     ,
                    global_attribute3                     ,
                    global_attribute4                     ,
                    global_attribute5                     ,
                    global_attribute6                     ,
                    global_attribute7                     ,
                    global_attribute8                     ,
                    global_attribute9                     ,
                    global_attribute10                    ,
                    global_attribute11                    ,
                    global_attribute12                    ,
                    global_attribute13                    ,
                    global_attribute14                    ,
                    global_attribute15                    ,
                    global_attribute16                    ,
                    global_attribute17                    ,
                    global_attribute18                    ,
                    global_attribute19                    ,
                    global_attribute20                    ,
                    global_attribute_number1              ,
                    global_attribute_number2              ,
                    global_attribute_number3              ,
                    global_attribute_number4              ,
                    global_attribute_number5              ,
                    global_attribute_date1                ,
                    global_attribute_date2                ,
                    global_attribute_date3                ,
                    global_attribute_date4                ,
                    global_attribute_date5                ,
                    global_attribute_category             ,
                    nbv_at_switch                         ,
                    period_name_fully_reserved            ,
                    period_name_extended                  ,
                    prior_deprn_limit_type                ,
                    prior_deprn_limit                     ,
                    prior_deprn_limit_amount              ,
                    prior_method_code                     ,
                    prior_life_in_months                  ,
                    prior_basic_rate                      ,
                    prior_adjusted_rate                   ,
                    asset_schedule_number                 ,
                    lease_number                          ,
                    reval_reserve                         ,
                    reval_loss_blanace                    ,
                    reval_amortization_basis              ,
                    impair_loss_balance                   ,
                    reval_ceiling                         ,
                    fair_market_value                     ,
                    last_price_index_value                ,
                    global_attribute_number6              ,
                    global_attribute_number7              ,
                    global_attribute_number8              ,
                    global_attribute_number9              ,
                    global_attribute_number10             ,
                    global_attribute_date6                ,
                    global_attribute_date7                ,
                    global_attribute_date8                ,
                    global_attribute_date9                ,
                    global_attribute_date10               ,
                    bk_global_attribute1                  ,
                    bk_global_attribute2                  ,
                    bk_global_attribute3                  ,
                    bk_global_attribute4                  ,
                    bk_global_attribute5                  ,
                    bk_global_attribute6                  ,
                    bk_global_attribute7                  ,
                    bk_global_attribute8                  ,
                    bk_global_attribute9                  ,
                    bk_global_attribute10                 ,
                    bk_global_attribute11                 ,
                    bk_global_attribute12                 ,
                    bk_global_attribute13                 ,
                    bk_global_attribute14                 ,
                    bk_global_attribute15                 ,
                    bk_global_attribute16                 ,
                    bk_global_attribute17                 ,
                    bk_global_attribute18                 ,
                    bk_global_attribute19                 ,
                    bk_global_attribute20                 ,
                    BK_GLOBAL_ATTRIBUTE_NUMBER1           ,
                    BK_GLOBAL_ATTRIBUTE_NUMBER2           ,
                    BK_GLOBAL_ATTRIBUTE_NUMBER3           ,
                    BK_GLOBAL_ATTRIBUTE_NUMBER4           ,
                    BK_GLOBAL_ATTRIBUTE_NUMBER5           ,
                    BK_GLOBAL_ATTRIBUTE_DATE1             ,
                    BK_GLOBAL_ATTRIBUTE_DATE2             ,
                    BK_GLOBAL_ATTRIBUTE_DATE3             ,
                    BK_GLOBAL_ATTRIBUTE_DATE4             ,
                    BK_GLOBAL_ATTRIBUTE_DATE5             ,
                    BK_GLOBAL_ATTRIBUTE_CATEGORY          ,
                    th_global_attribute1                  ,
                    th_global_attribute2                  ,
                    th_global_attribute3                  ,
                    th_global_attribute4                  ,
                    th_global_attribute5                  ,
                    th_global_attribute6                  ,
                    th_global_attribute7                  ,
                    th_global_attribute8                  ,
                    th_global_attribute9                  ,
                    th_global_attribute10                 ,
                    th_global_attribute11                   ,
                    th_global_attribute12                 ,
                    th_global_attribute13                 ,
                    th_global_attribute14                 ,
                    th_global_attribute15                 ,
                    th_global_attribute16                 ,
                    th_global_attribute17                 ,
                    th_global_attribute18                 ,
                    th_global_attribute19                 ,
                    th_global_attribute20                 ,
                    th_global_attribute_number1           ,
                    th_global_attribute_number2           ,
                    th_global_attribute_number3           ,
                    th_global_attribute_number4           ,
                    th_global_attribute_number5           ,
                    th_global_attribute_date1             ,
                    th_global_attribute_date2             ,
                    th_global_attribute_date3             ,
                    th_global_attribute_date4             ,
                    th_global_attribute_date5             ,
                    th_global_attribute_category          ,
                    ai_global_attribute1                  ,
                    ai_global_attribute2                  ,
                    ai_global_attribute3                  ,
                    ai_global_attribute4                  ,
                    ai_global_attribute5                  ,
                    ai_global_attribute6                  ,
                    ai_global_attribute7                  ,
                    ai_global_attribute8                  ,
                    ai_global_attribute9                  ,
                    ai_global_attribute10                 ,
                    ai_global_attribute11                 ,
                    ai_global_attribute12                 ,
                    ai_global_attribute13                 ,
                    ai_global_attribute14                 ,
                    ai_global_attribute15                 ,
                    ai_global_attribute16                 ,
                    ai_global_attribute17                 ,
                    ai_global_attribute18                 ,
                    ai_global_attribute19                 ,
                    ai_global_attribute20                 ,
                    ai_global_attribute_number1           ,
                    ai_global_attribute_number2           ,
                    ai_global_attribute_number3           ,
                    ai_global_attribute_number4           ,
                    ai_global_attribute_number5           ,
                    ai_global_attribute_date1             ,
                    ai_global_attribute_date2             ,
                    ai_global_attribute_date3             ,
                    ai_global_attribute_date4             ,
                    ai_global_attribute_date5             ,
                    ai_global_attribute_category          ,
                    vendor_name                           ,
                    vendor_number                         ,
                    po_number                             ,
                    invoice_number                        ,
                    invoice_voucher_number                ,
                    invoice_date                          ,
                    payables_units                        ,
                    invoice_line_number                   ,
                    invoice_line_type                     ,
                    invoice_line_description              ,
                    invoice_payment_number                ,
                    project_number                        ,
                    project_task_number                   ,
                    fully_reserve_on_add_flag             ,
                    deprn_adjustment_factor               ,
                     file_name,
                               loading_status,
                               error_message,
                               file_reference_identifier,
                               batch_id,
                               source_system							   
                                    FROM xxcnv_fa_c013_fa_massadd_stg 
    where  loading_status = '''
                             || 'ERROR'
                             || '''
    AND execution_id  =  '''
                             || gv_execution_id
                             || '''
    AND file_reference_identifier= '''
                             || gv_execution_id
                             || '_'
                             || gv_status_failure
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
                                   || gv_oci_file_name_addition,
                    p_attribute1        => lv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error exporting data to CSV for BATCH_ID '
                                         || lv_batch_id
                                         || ': '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);
                --RETURN;
            END;

        END LOOP;

----Table 2

        BEGIN
            FOR g_id IN batch_id_cursor_dist LOOP
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
                                         || 'ATP_Recon_MassAddDistributions'
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
				                     interface_line_number        , 
									 UNITS_NUMBER                 ,     	
									 Assigned_to                  ,
									 location_id_Segment1         ,
									 location_id_Segment2         ,
									 location_id_Segment3         ,
									 location_id_Segment4         ,
									 location_id_Segment5         ,
									 location_id_Segment6         ,
									 location_id_Segment7         ,
									 CAST( Deprn_Expense_CCID_Segment1 AS VARCHAR2(100))  AS SOURCE_SEGMENT1,
									 CAST(Deprn_Expense_CCID_Segment2  AS VARCHAR2(100)) AS  SOURCE_SEGMENT2,
									 CAST(Deprn_Expense_CCID_Segment3  AS VARCHAR2(100)) AS  SOURCE_SEGMENT3,
									 CAST(Deprn_Expense_CCID_Segment4  AS VARCHAR2(100)) AS  SOURCE_SEGMENT4,
									 CAST(Deprn_Expense_CCID_Segment5  AS VARCHAR2(100)) AS  SOURCE_SEGMENT5,
									 CAST(Deprn_Expense_CCID_Segment6  AS VARCHAR2(100)) AS  SOURCE_SEGMENT6,
									 CAST(Deprn_Expense_CCID_Segment7  AS VARCHAR2(100)) AS  SOURCE_SEGMENT7,
									 CAST(Deprn_Expense_CCID_Segment8  AS VARCHAR2(100)) AS  SOURCE_SEGMENT8,
									 CAST(Deprn_Expense_CCID_Segment9  AS VARCHAR2(100)) AS  SOURCE_SEGMENT9,
									 CAST(Deprn_Expense_CCID_Segment10 AS VARCHAR2(100)) AS  SOURCE_SEGMENT10,
									 Deprn_Expense_CCID_Segment11 ,
									 Deprn_Expense_CCID_Segment12 ,
									 Deprn_Expense_CCID_Segment13 ,
									 Deprn_Expense_CCID_Segment14 ,
									 Deprn_Expense_CCID_Segment15 ,
									 Deprn_Expense_CCID_Segment16 ,
									 Deprn_Expense_CCID_Segment17 ,
									 Deprn_Expense_CCID_Segment18 ,
									 Deprn_Expense_CCID_Segment19 ,
									 Deprn_Expense_CCID_Segment20 ,
									 Deprn_Expense_CCID_Segment21 ,
									 Deprn_Expense_CCID_Segment22 ,
									 Deprn_Expense_CCID_Segment23 ,
									 Deprn_Expense_CCID_Segment24 ,
									 Deprn_Expense_CCID_Segment25 ,
									 Deprn_Expense_CCID_Segment26 ,
									 Deprn_Expense_CCID_Segment27 ,
									 Deprn_Expense_CCID_Segment28 ,
									 Deprn_Expense_CCID_Segment29 ,
									 Deprn_Expense_CCID_Segment30 ,
									 CAST(target_segment1 AS VARCHAR2(100)) AS Deprn_Expense_CCID_Segment1,
									 CAST(target_segment2 AS VARCHAR2(100)) AS Deprn_Expense_CCID_Segment2 ,
									 CAST(target_segment3 AS VARCHAR2(100)) AS Deprn_Expense_CCID_Segment3 ,
									 CAST(target_segment4 AS VARCHAR2(100)) AS Deprn_Expense_CCID_Segment4 ,
									 CAST(target_segment5 AS VARCHAR2(100)) AS Deprn_Expense_CCID_Segment5 ,
									 CAST(target_segment6 AS VARCHAR2(100)) AS Deprn_Expense_CCID_Segment6 ,
									 CAST(target_segment7 AS VARCHAR2(100)) AS Deprn_Expense_CCID_Segment7 ,
									 CAST(target_segment8 AS VARCHAR2(100)) AS Deprn_Expense_CCID_Segment8 ,
									 CAST(target_segment9 AS VARCHAR2(100)) AS Deprn_Expense_CCID_Segment9 ,
									 CAST(target_segment10 AS VARCHAR2(100))AS Deprn_Expense_CCID_Segment10,						 
                                      file_name,
						       loading_status,
						       error_message,
						       file_reference_identifier,
                               batch_id,
							   source_system

                                    FROM xxcnv_fa_c013_fa_massadd_dist_stg   
                                    where  loading_status = '''
                                 || 'ERROR'
                                 || '''
                                    AND execution_id  =  '''
                                 || gv_execution_id
                                 || '''
                                    AND file_reference_identifier= '''
                                 || gv_execution_id
                                 || '_'
                                 || gv_status_failure
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
                                       || gv_oci_file_name_distribution,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

--------- Summary File

                    BEGIN
                        dbms_cloud.export_data(
                            credential_name => gv_credential_name,
                            file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                             || '/'
                                             || lv_batch_id
                                             || 'ATP_Recon_Summary'
                                             || sysdate,
                            format          =>
                                    JSON_OBJECT(
                                        'type' VALUE 'csv',
                                        'trimspaces' VALUE 'rtrim',
                                        'maxfilesize' VALUE '629145600',
                                        'header' VALUE TRUE
                                    ),
                            query           => 'SELECT 
										book_type_code,
										execution_id,
										'''
                                     || gv_addition
                                     || ''' as source,
										COUNT(*) AS total_records,
										SUM(CASE WHEN loading_status = ''PROCESSED'' THEN 1 ELSE 0 END) AS success_count,
										SUM(CASE WHEN loading_status = ''ERROR'' THEN 1 ELSE 0 END) AS error_count,
										(SUM(CASE WHEN loading_status = ''ERROR'' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS error_percentage,
										SUM(CASE WHEN loading_status = ''ERROR'' AND error_message IS NOT NULL THEN 1 ELSE 0 END) AS error_message_count,
										(SUM(CASE WHEN loading_status = ''ERROR'' AND error_message IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS error_message_percentage
									FROM 
										xxcnv_fa_c013_fa_massadd_stg
									where 
										execution_id  = '''
                                     || gv_execution_id
                                     || '''
									GROUP BY 
										book_type_code,
										execution_id
									UNION ALL
									SELECT  							
										execution_id,
										COUNT(*) AS total_records,
										'''
                                     || gv_distribution
                                     || ''' as source,
										SUM(CASE WHEN loading_status = ''PROCESSED'' THEN 1 ELSE 0 END) AS success_count,
										SUM(CASE WHEN loading_status = ''ERROR'' THEN 1 ELSE 0 END) AS error_count,
										(SUM(CASE WHEN loading_status = ''ERROR'' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS error_percentage,
										SUM(CASE WHEN loading_status = ''ERROR'' AND error_message IS NOT NULL THEN 1 ELSE 0 END) AS error_message_count,
										(SUM(CASE WHEN loading_status = ''ERROR'' AND error_message IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS error_message_percentage
									FROM 
										xxcnv_fa_c013_fa_massadd_dist_stg
									where 
										execution_id  = '''
                                     || gv_execution_id
                                     || '''
									GROUP BY   
										execution_id
									ORDER BY    
										execution_id'
                        );
                    END;

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for BATCH_ID '
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

END xxcnv_fa_c013_fa_conversion_pkg;