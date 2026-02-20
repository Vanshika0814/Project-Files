create or replace PACKAGE BODY       xxcnv.xxcnv_pdh_c027_item_conversion_pkg IS
    /*************************************************************************************
    NAME              :     Item_Conversion_Package BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
    -- Modification History
    -- Developer          Date         Version     Comments and changes made
    -- -------------   ------       ----------  -----------------------------------------
    --  Satya Pavani     28-Aug-2024       1.0         Initial Development
	-- Satya Pavani      31-Jul-2025       1.1         Updated code based on JIRA ID-6492
   |**************************************************************************************/

    -- Declaring global Variables
    gv_import_status                 VARCHAR2(256) := NULL;
    gv_error_message                 VARCHAR2(500) := NULL;
    gv_oci_file_path                 VARCHAR2(256) := NULL;
    gv_oci_file_name                 VARCHAR2(4000) := NULL;
    gv_oci_file_name_item            VARCHAR2(100) := NULL;
    gv_oci_file_name_item_categories VARCHAR2(500) := NULL;
    gv_oci_file_name_item_effs       VARCHAR2(500) := NULL;
    gv_execution_id                  VARCHAR2(100) := NULL;
    gv_batch_id                      NUMBER(38) := NULL;
    gv_credential_name               CONSTANT VARCHAR2(100) := 'OCI$RESOURCE_PRINCIPAL';
    gv_status_success                CONSTANT VARCHAR2(100) := 'Success';
    gv_status_failure                CONSTANT VARCHAR2(100) := 'Failure';
    gv_conversion_id                 VARCHAR2(100) := NULL;
    gv_boundary_system               VARCHAR2(100) := NULL;
    gv_status_picked                 CONSTANT VARCHAR2(100) := 'File_Picked_From_OCI_And_Loaded_To_Stg';
    gv_status_picked_for_tr          CONSTANT VARCHAR2(100) := 'Transformed_Data_From_Ext_To_Stg';
    gv_status_validated              CONSTANT VARCHAR2(100) := 'VALIDATED';
    gv_status_failed_validation      CONSTANT VARCHAR2(100) := 'NOT_VALIATED';
    gv_fbdi_export_status            CONSTANT VARCHAR2(100) := 'EXPORTED_TO_FBDI';
    gv_status_staged                 CONSTANT VARCHAR2(100) := 'STAGED_FOR_IMPORT';
    gv_transformed_folder            CONSTANT VARCHAR2(100) := 'Transformed_FBDI_Files';
    gv_source_folder                 CONSTANT VARCHAR2(100) := 'Source_FBDI_Files';
    gv_properties                    CONSTANT VARCHAR2(100) := 'properties';
    gv_file_picked                   VARCHAR2(100) := 'File_Picked_From_OCI_Server';
    gv_status_failed                 CONSTANT VARCHAR2(100) := 'FAILED_AT_VALIDATION';
    gv_recon_folder                  CONSTANT VARCHAR2(50) := 'ATP_Validation_Error_Files';
    gv_recon_report                  CONSTANT VARCHAR2(100) := 'Recon_Report_Created';
    gv_file_not_found                CONSTANT VARCHAR2(100) := 'File_not_found';

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
                    WHEN lv_file_name LIKE '%EgpSystemItems%.csv' THEN
                        gv_oci_file_name_item := lv_file_name;
                    WHEN lv_file_name LIKE '%EgpItemCategories%.csv' THEN
                        gv_oci_file_name_item_categories := lv_file_name;
                    WHEN lv_file_name LIKE '%EgoItemIntfEff%.csv' THEN
                        gv_oci_file_name_item_effs := lv_file_name;
                    ELSE
                        dbms_output.put_line('No match found for file name: ' || lv_file_name); -- Debugging output
                END CASE;

                lv_start_pos := lv_end_pos + 1;
            END LOOP;

				-- Output the results for debugging
            dbms_output.put_line('lv_File Name: ' || lv_file_name);
            dbms_output.put_line('Item File Name: ' || gv_oci_file_name_item);
            dbms_output.put_line('Categories File Name: ' || gv_oci_file_name_item_categories);
            dbms_output.put_line('Effs File Name: ' || gv_oci_file_name_item_effs);
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error fetching execution details: ' || sqlerrm);
        END;

			-- Call to import data from OCI to external table
        BEGIN
            import_data_from_oci_to_stg_prc(p_loading_status);
            IF p_loading_status = gv_status_failure THEN
                dbms_output.put_line('Error in IMPORT_DATA_FROM_OCI_TO_STG_PRC');
                RETURN;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling IMPORT_DATA_FROM_OCI_TO_STG_PRC: ' || sqlerrm);
        END;

    -- Call to perform data and business validations in interface table
        BEGIN
            data_validations_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling DATA_VALIDATIONS_PRC: ' || sqlerrm);
        END;

    -- Call to create a CSV file from XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG after all validations
        BEGIN
            create_fbdi_file_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling CREATE_FBDI_FILE_PRC: ' || sqlerrm);
        END;

	--CREATE RECON REPORT 

        BEGIN
            create_recon_report_prc;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error calling CREATE_RECON_REPORT_PRC: ' || sqlerrm);
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
    -- Check if the external table exists and drop it if it does

        EXECUTE IMMEDIATE ( 'TRUNCATE TABLE XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG' );
        EXECUTE IMMEDIATE ( 'TRUNCATE TABLE XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG' );
        EXECUTE IMMEDIATE ( 'TRUNCATE TABLE XXCNV_PDH_C027_EGO_ITEM_EFF_STG' );
        BEGIN
            BEGIN
                lv_table_count := 0;
                SELECT
                    COUNT(*)
                INTO lv_table_count
                FROM
                    all_objects
                WHERE
                        upper(object_name) = 'XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_EXT';
                    dbms_output.put_line('Table XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_EXT dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_EXT: '
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
                        upper(object_name) = 'XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_EXT';
                    dbms_output.put_line('Table XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_EXT dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_EXT: '
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
                        upper(object_name) = 'XXCNV_PDH_C027_EGO_ITEM_EFF_EXT'
                    AND object_type = 'TABLE';

                IF lv_table_count > 0 THEN
                    EXECUTE IMMEDIATE 'DROP TABLE XXCNV_PDH_C027_EGO_ITEM_EFF_EXT';
                    dbms_output.put_line('Table XXCNV_PDH_C027_EGO_ITEM_EFF_EXT dropped');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error dropping table XXCNV_PDH_C027_EGO_ITEM_EFF_EXT: '
                                         || '->'
                                         || substr(sqlerrm, 1, 3000)
                                         || '->'
                                         || dbms_utility.format_error_backtrace);

                    p_loading_status := gv_status_failure;
            END;

        END;

    -- Create the external table
        BEGIN
            IF gv_oci_file_name_item LIKE '%EgpSystemItems%' THEN
                dbms_output.put_line('Creating external table XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_EXT');
                dbms_output.put_line(' XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_EXT : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_item);
                dbms_cloud.create_external_table(
                    table_name      => 'XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_EXT',
                    credential_name => 'OCI$RESOURCE_PRINCIPAL',
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_item,
		  -- file_uri_list   =>'https://objectstorage.us-phoenix-1.oraclecloud.com/n/axcepiuovkix/b/Non_Prod_Conversion/o/mock1/ITEM/3/SourceFBDI/XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG_V1_20241017.csv',
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
                    column_list     => 'transaction_type	varchar2(100),
						batch_id	number(18),
						batch_number	varchar2(40),
						item_number	varchar2(300),
						outside_process_service_flag	VARCHAR2(10),
						organization_code	VARCHAR2(50),
						description	VARCHAR2(240),
						template_name	varchar2(960),
						source_system_code	varchar2(30),
						source_system_reference	varchar2(255),
						source_system_reference_desc	VARCHAR2(500),
						item_class_name varchar2(820),
						primary_uom_name	varchar2(25),
						current_phase_code	varchar2(120),
						inventory_item_status_code	varchar2(10),
						new_item_class_name	varchar2(820),
						asset_tracked_flag	varchar2(5),
						allow_MAIN_PRCtenance_asset_flag	VARCHAR2(10),
						enable_genealogy_tracking_flag	VARCHAR2(10),
						asset_class	varchar2(30),
						eam_item_type	number,
						eam_activity_type_code	varchar2(30),
						eam_activity_cause_code	varchar2(30),
						eam_act_notification_flag	VARCHAR2(10),
						eam_act_shutdown_status	varchar2(30),
						eam_activity_source_code	varchar2(30),
						costing_enabled_flag	VARCHAR2(10),
						std_lot_size	number,
						inventory_asset_flag	VARCHAR2(10),
						default_include_in_rollup_flag	VARCHAR2(10),
						order_cost	number,
						vmi_minimum_days	number,
						vmi_fixed_order_quantity	number,
						vmi_minimum_units	number,
						asn_autoexpire_flag	number,
						carrying_cost	number,
						consigned_flag	number,
						fixed_days_supply	number,
						fixed_lot_multiplier	number,
						fixed_order_quantity	number,
						forecast_horizon	number,
						inventory_planning_code	number,
						safety_stock_planning_method	number,
						demand_period	number,
						days_of_cover	number,
						min_minmax_quantity	number,
						max_minmax_quantity	number,
						minimum_order_quantity	number,
						maximum_order_quantity	number,
						planner_code	varchar2(10),
						planning_make_buy_code	varchar2(10), ---will change on mock2
						source_subinventory	varchar2(10),
						source_type	number,
						so_authorization_flag	number,
						subcontracting_component	number,
						vmi_forecast_type	number,
						vmi_maximum_units	number,
						vmi_maximum_days	number,
						source_organization_code	VARCHAR2(50),
						restrict_subinventories_code	number,
						restrict_locators_code	number,
						child_lot_flag	VARCHAR2(10),
						child_lot_prefix	varchar2(30),
						child_lot_starting_number	number,
						child_lot_validation_flag	VARCHAR2(10),
						copy_lot_attribute_flag	VARCHAR2(10),
						expiration_action_code	varchar2(32),
						expiration_action_interval	number,
						stock_enabled_flag	VARCHAR2(10),
						start_auto_lot_number	varchar2(80),
						shelf_life_code	number,
						shelf_life_days	number,
						serial_number_control_code	number,
						serial_status_enabled	VARCHAR2(10),
						revision_qty_control_code	number,
						retest_interval	number,
						auto_lot_alpha_prefix	varchar2(80),
						auto_serial_alpha_prefix	varchar2(80),
						bulk_picked_flag	VARCHAR2(10),
						check_shortages_flag	VARCHAR2(10),
						cycle_count_enabled_flag	VARCHAR2(10),
						default_grade	varchar2(150),
						grade_control_flag	VARCHAR2(10),
						hold_days	number,
						lot_divisible_flag	VARCHAR2(10),
						maturity_days	number,
						default_lot_status_id	number(18),
						default_serial_status_id	number(18),
						lot_split_enabled	VARCHAR2(10),
						lot_merge_enabled	VARCHAR2(10),
						inventory_item_flag	VARCHAR2(10),
						location_control_code	number,
						lot_control_code	number,
						lot_status_enabled	VARCHAR2(10),
						lot_substitution_enabled	VARCHAR2(10),
						lot_translate_enabled	VARCHAR2(10),
						mtl_transactions_enabled_flag	VARCHAR2(10),
						positive_measurement_error	number,
						negative_measurement_error	number,
						parent_child_generation_flag	VARCHAR2(10),
						reservable_type	number, --will change in mock2
						start_auto_serial_number	varchar2(80),
						invoicing_rule_name	number(18),
						tax_code	varchar2(50),
						sales_account	number(18),
						payment_terms_name	varchar2(15),
						invoice_enabled_flag	VARCHAR2(10),
						invoiceable_item_flag	VARCHAR2(10),
						accounting_rule_name	number(18),
						auto_created_config_flag	VARCHAR2(10),
						replenish_to_order_flag	VARCHAR2(10),
						pick_components_flag	VARCHAR2(10),
						base_item_number	varchar2(300),
						effectivity_control	number,
						config_orgs	varchar2(30),
						config_match	varchar2(30),
						config_model_type	varchar2(30),
						bom_item_type	number,
						cum_manufacturing_lead_time	number,
						preprocessing_lead_time	number,
						cumulative_total_lead_time	number,
						fixed_lead_time	number,
						variable_lead_time	number,
						full_lead_time	number,
						lead_time_lot_size	number,
						postprocessing_lead_time	number,
						ato_forecast_control	number,
						critical_component_flag	number,
						acceptable_early_days	number,
						create_supply_flag	VARCHAR2(10),
						days_tgt_inv_supply	number,
						days_tgt_inv_window	number,
						days_max_inv_supply	number,
						days_max_inv_window	number,
						demand_time_fence_code	number,
						demand_time_fence_days	number,
						drp_planned_flag	number,
						end_assembly_pegging_flag	VARCHAR2(10),
						exclude_from_budget_flag	number,
						mrp_calculate_atp_flag	VARCHAR2(10),
						mrp_planning_code	number,
						planned_inv_point_flag	VARCHAR2(10),
						planning_time_fence_code	number,
						planning_time_fence_days	number,
						preposition_point	VARCHAR2(10),
						release_time_fence_code	number,
						release_time_fence_days	number,
						repair_leadtime	number,
						repair_yield	number,
						repair_program	number,
						rounding_control_type	number,
						shrinkage_rate	number,
						substitution_window_code	number,
						substitution_window_days	number,
						trade_item_descriptor	varchar2(35),
						allowed_units_lookup_code	number,
						dual_uom_deviation_high	number,
						dual_uom_deviation_low	number,
						item_type	varchar2(30),
						long_description	varchar2(4000),
						html_long_description	clob,
						ont_pricing_qty_source	varchar2(30),
						secondary_default_ind	varchar2(30),
						secondary_uom_name	varchar2(25),
						tracking_quantity_ind	varchar2(30),
						engineered_item_flag	VARCHAR2(10),
						atp_components_flag	VARCHAR2(10),
						atp_flag	VARCHAR2(10),
						over_shipment_tolerance	number,
						under_shipment_tolerance	number,
						over_return_tolerance	number,
						under_return_tolerance	number,
						downloadable_flag	VARCHAR2(10),
						electronic_flag	VARCHAR2(10),
						indivisible_flag	VARCHAR2(10),
						internal_order_enabled_flag	VARCHAR2(10),
						atp_rule_id	number(18),
						charge_periodicity_name	varchar2(25),
						customer_order_enabled_flag	VARCHAR2(10),
						default_shipping_org_code	VARCHAR2(50),
						default_so_source_type	varchar2(30),
						eligibility_compatibility_rule	VARCHAR2(10),
						financing_allowed_flag	VARCHAR2(10),
						internal_order_flag	VARCHAR2(10),
						picking_rule_id	number,
						returnable_flag	VARCHAR2(10),
						return_inspection_requirement	number,
						sales_product_type	varchar2(30),
						back_to_back_enabled	varchar2(5),
						shippable_item_flag	VARCHAR2(10),
						ship_model_complete_flag	VARCHAR2(10),
						so_transactions_flag	VARCHAR2(10),
						customer_order_flag	VARCHAR2(10),
						unit_weight	number,
						weight_uom_name	varchar2(25),
						unit_volume	number,
						volume_uom_name	varchar2(25),
						dimension_uom_name	varchar2(25),
						unit_length	number,
						unit_width	number,
						unit_height	number,
						collateral_flag	VARCHAR2(10),
						container_item_flag	VARCHAR2(10),
						container_type_code	varchar2(30),
						equipment_type	number,
						event_flag	VARCHAR2(10),
						internal_volume	number,
						maximum_load_weight	number,
						minimum_fill_percent	number,
						vehicle_item_flag	VARCHAR2(10),
						cas_number	varchar2(30),
						hazardous_material_flag	VARCHAR2(10),
						process_costing_enabled_flag	VARCHAR2(10),
						process_execution_enabled_flag	VARCHAR2(10),
						process_quality_enabled_flag	VARCHAR2(10),
						process_supply_locator_id	number(18),
						process_supply_subinventory	varchar2(10),
						process_yield_locator_id	number(18),
						process_yield_subinventory	varchar2(10),
						recipe_enabled_flag	VARCHAR2(10),
						expense_account	number(18),
						un_number_code	varchar2(30),
						unit_of_issue	varchar2(25),
						rounding_factor	number,
						receive_close_tolerance	number,
						purchasing_tax_code	varchar2(50),
						purchasing_item_flag	VARCHAR2(10),
						price_tolerance_percent	number,
						outsourced_assembly	number,
						outside_operation_uom_type	varchar2(25),
						negotiation_required_flag	VARCHAR2(10),
						must_use_approved_vendor_flag	VARCHAR2(10),
						match_approval_level	number(1),
						invoice_match_option	number(1),
						list_price_per_unit	number,
						invoice_close_tolerance	number,
						hazard_class_code	varchar2(30),
						buyer_name	varchar2(960),
						taxable_flag	VARCHAR2(10),
						purchasing_enabled_flag	VARCHAR2(10),
						outside_operation_flag	VARCHAR2(10),
						market_price	number,
						asset_category_id	number(18),
						allow_item_desc_update_flag	VARCHAR2(10),
						allow_express_delivery_flag	VARCHAR2(10),
						allow_substitute_receipts_flag	VARCHAR2(10),
						allow_unordered_receipts_flag	VARCHAR2(10),
						days_early_receipt_allowed	number,
						days_late_receipt_allowed	number,
						receiving_routing_id	number(18),
						enforce_ship_to_location_code	varchar2(25),
						qty_rcv_exception_code	varchar2(25),
						qty_rcv_tolerance	number,
						receipt_days_exception_code	varchar2(25),
						asset_creation_code	varchar2(30),
						service_start_type_code	varchar2(30),
						comms_nl_trackable_flag	VARCHAR2(10),
						css_enabled_flag	VARCHAR2(10),
						contract_item_type_code	varchar2(30),
						standard_coverage	varchar2(150),
						defect_tracking_on_flag	VARCHAR2(10),
						ib_item_instance_class	varchar2(30),
						material_billable_flag	varchar2(30),
						recovered_part_disp_code	varchar2(30),
						serviceable_product_flag	VARCHAR2(10),
						service_starting_delay	number,
						service_duration	number,
						service_duration_period_name	varchar2(25),
						serv_req_enabled_code	varchar2(30),
						allow_suspend_flag	VARCHAR2(10),
						allow_terminate_flag	VARCHAR2(10),
						requires_fulfillment_loc_flag	VARCHAR2(10),
						requires_itm_association_flag	VARCHAR2(10),
						service_start_delay	number,
						service_duration_type_code	varchar2(30),
						comms_activation_reqd_flag	VARCHAR2(10),
						serv_billing_enabled_flag	VARCHAR2(10),
						orderable_on_web_flag	VARCHAR2(10),
						back_orderable_flag	VARCHAR2(10),
						web_status	varchar2(30),
						minimum_license_quantity	number,
						build_in_wip_flag	VARCHAR2(10),
						contract_manufacturing	VARCHAR2(10),
						wip_supply_locator_id	number(18),
						wip_supply_type	number(18),
						wip_supply_subinventory	varchar2(10),
						overcompletion_tolerance_type	number,
						overcompletion_tolerance_value	number,
						inventory_carry_penalty	number,
						operation_slack_penalty	number,
						revision	VARCHAR2(50),
						style_item_flag	VARCHAR2(10),
						style_item_number	varchar(700),
						version_start_date	date,
						version_revision_code	VARCHAR2(50),
						version_label	varchar2(80),
						start_upon_milestone_code	varchar2(30),
						sales_product_sub_type	varchar2(30),
						global_attribute_category	varchar2(150),
						global_attribute1	varchar2(150),
						global_attribute2	varchar2(150),
						global_attribute3	varchar2(150),
						global_attribute4	varchar2(150),
						global_attribute5	varchar2(150),
						global_attribute6	varchar2(150),
						global_attribute7	varchar2(150),
						global_attribute8	varchar2(150),
						global_attribute9	varchar2(150),
						global_attribute10	varchar2(150),
						attribute_category	varchar2(30),
						attribute1	VARCHAR2(500),
						attribute2	VARCHAR2(500),
						attribute3	VARCHAR2(500),
						attribute4	VARCHAR2(500),
						attribute5	VARCHAR2(500),
						attribute6	VARCHAR2(500),
						attribute7	VARCHAR2(500),
						attribute8	VARCHAR2(500),
						attribute9	VARCHAR2(500),
						attribute10	VARCHAR2(500),
						attribute11	VARCHAR2(500),
						attribute12	VARCHAR2(500),
						attribute13	VARCHAR2(500),
						attribute14	VARCHAR2(500),
						attribute15	VARCHAR2(500),
						attribute16	VARCHAR2(500),
						attribute17	VARCHAR2(500),
						attribute18	VARCHAR2(500),
						attribute19	VARCHAR2(500),
						attribute20	VARCHAR2(500),
						attribute21	VARCHAR2(500),
						attribute22	VARCHAR2(500),
						attribute23	VARCHAR2(500),
						attribute24	VARCHAR2(500),
						attribute25	VARCHAR2(500),
						attribute26	VARCHAR2(500),
						attribute27	VARCHAR2(500),
						attribute28	VARCHAR2(500),
						attribute29	VARCHAR2(500),
						attribute30	VARCHAR2(500),
						attribute_number1	number,
						attribute_number2	number,
						attribute_number3	number,
						attribute_number4	number,
						attribute_number5	number,
						attribute_number6	number,
						attribute_number7	number,
						attribute_number8	number,
						attribute_number9	number,
						attribute_number10	number,
						attribute_date1	date,
						attribute_date2	date,
						attribute_date3	date,
						attribute_date4	date,
						attribute_date5	date,
						attribute_timestamp1	timestamp(6),
						attribute_timestamp2	timestamp(6),
						attribute_timestamp3	timestamp(6),
						attribute_timestamp4	timestamp(6),
						attribute_timestamp5	timestamp(6),
						global_attribute11	varchar2(150),
						global_attribute12	varchar2(150),
						global_attribute13	varchar2(150),
						global_attribute14	varchar2(150),
						global_attribute15	varchar2(150),
						global_attribute16	varchar2(150),
						global_attribute17	varchar2(150),
						global_attribute18	varchar2(150),
						global_attribute19	varchar2(150),
						global_attribute20	varchar2(150),
						global_attribute_number1	number,
						global_attribute_number2	number,
						global_attribute_number3	number,
						global_attribute_number4	number,
						global_attribute_number5	number,
						global_attribute_date1	date,
						global_attribute_date2	date,
						global_attribute_date3	date,
						global_attribute_date4	date,
						global_attribute_date5	date,
						prc_bu_name	VARCHAR2(500),
						force_purchase_lead_time_flag	VARCHAR2(10),
						replacement_type	varchar2(30),
						buyer_email_address	VARCHAR2(500),
						default_expenditure_type	VARCHAR2(500),
						hard_pegging_level	varchar2(25),
						comn_supply_prj_demand_flag	VARCHAR2(10),
						enable_iot_flag	VARCHAR2(10),
						packaging_string	varchar2(100),
						create_supply_after_date	date,
						create_fixed_asset	varchar2(30),
						under_compl_tolerance_type	varchar2(30),
						under_compl_tolerance_value	number,
						repair_transaction_name	varchar(250),
						new_primary_uom_name	varchar2(25),
						new_secondary_uom_name	varchar2(25)'
                );

                dbms_output.put_line(' External table XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_EXT is created');
                EXECUTE IMMEDIATE 'INSERT INTO XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG (

					    transaction_type,
						batch_id,
						batch_number,
						item_number,
						outside_process_service_flag,
						organization_code,
						description,
						template_name,
						source_system_code,
						source_system_reference,
						source_system_reference_desc,
						item_class_name,
						primary_uom_name,
						current_phase_code,
						inventory_item_status_code,
						new_item_class_name,
						asset_tracked_flag,
						allow_MAIN_PRCtenance_asset_flag,
						enable_genealogy_tracking_flag,
						asset_class,
						eam_item_type,
						eam_activity_type_code,
						eam_activity_cause_code,
						eam_act_notification_flag,
						eam_act_shutdown_status,
						eam_activity_source_code,
						costing_enabled_flag,
						std_lot_size,
						inventory_asset_flag,
						default_include_in_rollup_flag,
						order_cost,
						vmi_minimum_days,
						vmi_fixed_order_quantity,
						vmi_minimum_units,
						asn_autoexpire_flag,
						carrying_cost,
						consigned_flag,
						fixed_days_supply,
						fixed_lot_multiplier,
						fixed_order_quantity,
						forecast_horizon,
						inventory_planning_code,
						safety_stock_planning_method,
						demand_period,
						days_of_cover,
						min_minmax_quantity,
						max_minmax_quantity,
						minimum_order_quantity,
						maximum_order_quantity,
						planner_code,
						planning_make_buy_code,
						source_subinventory,
						source_type,
						so_authorization_flag,
						subcontracting_component,
						vmi_forecast_type,
						vmi_maximum_units,
						vmi_maximum_days,
						source_organization_code,
						restrict_subinventories_code,
						restrict_locators_code,
						child_lot_flag,
						child_lot_prefix,
						child_lot_starting_number,
						child_lot_validation_flag,
						copy_lot_attribute_flag,
						expiration_action_code,
						expiration_action_interval,
						stock_enabled_flag,
						start_auto_lot_number,
						shelf_life_code,
						shelf_life_days,
						serial_number_control_code,
						serial_status_enabled,
						revision_qty_control_code,
						retest_interval,
						auto_lot_alpha_prefix,
						auto_serial_alpha_prefix,
						bulk_picked_flag,
						check_shortages_flag,
						cycle_count_enabled_flag,
						default_grade,
						grade_control_flag,
						hold_days,
						lot_divisible_flag,
						maturity_days,
						default_lot_status_id,
						default_serial_status_id,
						lot_split_enabled,
						lot_merge_enabled,
						inventory_item_flag,
						location_control_code,
						lot_control_code,
						lot_status_enabled,
						lot_substitution_enabled,
						lot_translate_enabled,
						mtl_transactions_enabled_flag,
						positive_measurement_error,
						negative_measurement_error,
						parent_child_generation_flag,
						reservable_type,
						start_auto_serial_number,
						invoicing_rule_name,
						tax_code,
						sales_account,
						payment_terms_name,
						invoice_enabled_flag,
						invoiceable_item_flag,
						accounting_rule_name,
						auto_created_config_flag,
						replenish_to_order_flag,
						pick_components_flag,
						base_item_number,
						effectivity_control,
						config_orgs,
						config_match,
						config_model_type,
						bom_item_type,
						cum_manufacturing_lead_time,
						preprocessing_lead_time,
						cumulative_total_lead_time,
						fixed_lead_time,
						variable_lead_time,
						full_lead_time,
						lead_time_lot_size,
						postprocessing_lead_time,
						ato_forecast_control,
						critical_component_flag,
						acceptable_early_days,
						create_supply_flag,
						days_tgt_inv_supply,
						days_tgt_inv_window,
						days_max_inv_supply,
						days_max_inv_window,
						demand_time_fence_code,
						demand_time_fence_days,
						drp_planned_flag,
						end_assembly_pegging_flag,
						exclude_from_budget_flag,
						mrp_calculate_atp_flag,
						mrp_planning_code,
						planned_inv_point_flag,
						planning_time_fence_code,
						planning_time_fence_days,
						preposition_point,
						release_time_fence_code,
						release_time_fence_days,
						repair_leadtime,
						repair_yield,
						repair_program,
						rounding_control_type,
						shrinkage_rate,
						substitution_window_code,
						substitution_window_days,
						trade_item_descriptor,
						allowed_units_lookup_code,
						dual_uom_deviation_high,
						dual_uom_deviation_low,
						item_type,
						long_description,
						html_long_description,
						ont_pricing_qty_source,
						secondary_default_ind,
						secondary_uom_name,
						tracking_quantity_ind,
						engineered_item_flag,
						atp_components_flag,
						atp_flag,
						over_shipment_tolerance,
						under_shipment_tolerance,
						over_return_tolerance,
						under_return_tolerance,
						downloadable_flag,
						electronic_flag,
						indivisible_flag,
						internal_order_enabled_flag,
						atp_rule_id,
						charge_periodicity_name,
						customer_order_enabled_flag,
						default_shipping_org_code,
						default_so_source_type,
						eligibility_compatibility_rule,
						financing_allowed_flag,
						internal_order_flag,
						picking_rule_id,
						returnable_flag,
						return_inspection_requirement,
						sales_product_type,
						back_to_back_enabled,
						shippable_item_flag,
						ship_model_complete_flag,
						so_transactions_flag,
						customer_order_flag,
						unit_weight,
						weight_uom_name,
						unit_volume,
						volume_uom_name,
						dimension_uom_name,
						unit_length,
						unit_width,
						unit_height,
						collateral_flag,
						container_item_flag,
						container_type_code,
						equipment_type,
						event_flag,
						internal_volume,
						maximum_load_weight,
						minimum_fill_percent,
						vehicle_item_flag,
						cas_number,
						hazardous_material_flag,
						process_costing_enabled_flag,
						process_execution_enabled_flag,
						process_quality_enabled_flag,
						process_supply_locator_id,
						process_supply_subinventory,
						process_yield_locator_id,
						process_yield_subinventory,
						recipe_enabled_flag,
						expense_account,
						un_number_code,
						unit_of_issue,
						rounding_factor,
						receive_close_tolerance,
						purchasing_tax_code,
						purchasing_item_flag,
						price_tolerance_percent,
						outsourced_assembly,
						outside_operation_uom_type,
						negotiation_required_flag,
						must_use_approved_vendor_flag,
						match_approval_level,
						invoice_match_option,
						list_price_per_unit,
						invoice_close_tolerance,
						hazard_class_code,
						buyer_name,
						taxable_flag,
						purchasing_enabled_flag,
						outside_operation_flag,
						market_price,
						asset_category_id,
						allow_item_desc_update_flag,
						allow_express_delivery_flag,
						allow_substitute_receipts_flag,
						allow_unordered_receipts_flag,
						days_early_receipt_allowed,
						days_late_receipt_allowed,
						receiving_routing_id,
						enforce_ship_to_location_code,
						qty_rcv_exception_code,
						qty_rcv_tolerance,
						receipt_days_exception_code,
						asset_creation_code,
						service_start_type_code,
						comms_nl_trackable_flag,
						css_enabled_flag,
						contract_item_type_code,
						standard_coverage,
						defect_tracking_on_flag,
						ib_item_instance_class,
						material_billable_flag,
						recovered_part_disp_code,
						serviceable_product_flag,
						service_starting_delay,
						service_duration,
						service_duration_period_name,
						serv_req_enabled_code,
						allow_suspend_flag,
						allow_terminate_flag,
						requires_fulfillment_loc_flag,
						requires_itm_association_flag,
						service_start_delay,
						service_duration_type_code,
						comms_activation_reqd_flag,
						serv_billing_enabled_flag,
						orderable_on_web_flag,
						back_orderable_flag,
						web_status,
						minimum_license_quantity,
						build_in_wip_flag,
						contract_manufacturing,
						wip_supply_locator_id,
						wip_supply_type,
						wip_supply_subinventory,
						overcompletion_tolerance_type,
						overcompletion_tolerance_value,
						inventory_carry_penalty,
						operation_slack_penalty,
						revision,
						style_item_flag,
						style_item_number,
						version_start_date,
						version_revision_code,
						version_label,
						start_upon_milestone_code,
						sales_product_sub_type,
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
						attribute21,
						attribute22,
						attribute23,
						attribute24,
						attribute25,
						attribute26,
						attribute27,
						attribute28,
						attribute29,
						attribute30,
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
						attribute_date1,
						attribute_date2,
						attribute_date3,
						attribute_date4,
						attribute_date5,
						attribute_timestamp1,
						attribute_timestamp2,
						attribute_timestamp3,
						attribute_timestamp4,
						attribute_timestamp5,
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
						global_attribute_number1,
						global_attribute_number2,
						global_attribute_number3,
						global_attribute_number4,
						global_attribute_number5,
						global_attribute_date1,
						global_attribute_date2,
						global_attribute_date3,
						global_attribute_date4,
						global_attribute_date5,
						prc_bu_name,
						force_purchase_lead_time_flag,
						replacement_type,
						buyer_email_address,
						default_expenditure_type,
						hard_pegging_level,
						comn_supply_prj_demand_flag,
						enable_iot_flag,
						packaging_string,
						create_supply_after_date,
						create_fixed_asset,
						under_compl_tolerance_type,
						under_compl_tolerance_value,
						repair_transaction_name,
						new_primary_uom_name,
						new_secondary_uom_name,
						file_name,
						error_message,
						import_status,
						file_reference_identifier,
						source_system)

						SELECT 
						transaction_type,
						batch_id,
						batch_number,
						item_number,
						outside_process_service_flag,
						organization_code,
						description,
						template_name,
						source_system_code,
						source_system_reference,
						source_system_reference_desc,
						item_class_name,
						primary_uom_name,
						current_phase_code,
						inventory_item_status_code,
						new_item_class_name,
						asset_tracked_flag,
						allow_MAIN_PRCtenance_asset_flag,
						enable_genealogy_tracking_flag,
						asset_class,
						eam_item_type,
						eam_activity_type_code,
						eam_activity_cause_code,
						eam_act_notification_flag,
						eam_act_shutdown_status,
						eam_activity_source_code,
						costing_enabled_flag,
						std_lot_size,
						inventory_asset_flag,
						default_include_in_rollup_flag,
						order_cost,
						vmi_minimum_days,
						vmi_fixed_order_quantity,
						vmi_minimum_units,
						asn_autoexpire_flag,
						carrying_cost,
						consigned_flag,
						fixed_days_supply,
						fixed_lot_multiplier,
						fixed_order_quantity,
						forecast_horizon,
						inventory_planning_code,
						safety_stock_planning_method,
						demand_period,
						days_of_cover,
						min_minmax_quantity,
						max_minmax_quantity,
						minimum_order_quantity,
						maximum_order_quantity,
						planner_code,
						planning_make_buy_code,
						source_subinventory,
						source_type,
						so_authorization_flag,
						subcontracting_component,
						vmi_forecast_type,
						vmi_maximum_units,
						vmi_maximum_days,
						source_organization_code,
						restrict_subinventories_code,
						restrict_locators_code,
						child_lot_flag,
						child_lot_prefix,
						child_lot_starting_number,
						child_lot_validation_flag,
						copy_lot_attribute_flag,
						expiration_action_code,
						expiration_action_interval,
						stock_enabled_flag,
						start_auto_lot_number,
						shelf_life_code,
						shelf_life_days,
						serial_number_control_code,
						serial_status_enabled,
						revision_qty_control_code,
						retest_interval,
						auto_lot_alpha_prefix,
						auto_serial_alpha_prefix,
						bulk_picked_flag,
						check_shortages_flag,
						cycle_count_enabled_flag,
						default_grade,
						grade_control_flag,
						hold_days,
						lot_divisible_flag,
						maturity_days,
						default_lot_status_id,
						default_serial_status_id,
						lot_split_enabled,
						lot_merge_enabled,
						inventory_item_flag,
						location_control_code,
						lot_control_code,
						lot_status_enabled,
						lot_substitution_enabled,
						lot_translate_enabled,
						mtl_transactions_enabled_flag,
						positive_measurement_error,
						negative_measurement_error,
						parent_child_generation_flag,
						reservable_type,
						start_auto_serial_number,
						invoicing_rule_name,
						tax_code,
						sales_account,
						payment_terms_name,
						invoice_enabled_flag,
						invoiceable_item_flag,
						accounting_rule_name,
						auto_created_config_flag,
						replenish_to_order_flag,
						pick_components_flag,
						base_item_number,
						effectivity_control,
						config_orgs,
						config_match,
						config_model_type,
						bom_item_type,
						cum_manufacturing_lead_time,
						preprocessing_lead_time,
						cumulative_total_lead_time,
						fixed_lead_time,
						variable_lead_time,
						full_lead_time,
						lead_time_lot_size,
						postprocessing_lead_time,
						ato_forecast_control,
						critical_component_flag,
						acceptable_early_days,
						create_supply_flag,
						days_tgt_inv_supply,
						days_tgt_inv_window,
						days_max_inv_supply,
						days_max_inv_window,
						demand_time_fence_code,
						demand_time_fence_days,
						drp_planned_flag,
						end_assembly_pegging_flag,
						exclude_from_budget_flag,
						mrp_calculate_atp_flag,
						mrp_planning_code,
						planned_inv_point_flag,
						planning_time_fence_code,
						planning_time_fence_days,
						preposition_point,
						release_time_fence_code,
						release_time_fence_days,
						repair_leadtime,
						repair_yield,
						repair_program,
						rounding_control_type,
						shrinkage_rate,
						substitution_window_code,
						substitution_window_days,
						trade_item_descriptor,
						allowed_units_lookup_code,
						dual_uom_deviation_high,
						dual_uom_deviation_low,
						item_type,
						description,
						html_long_description,
						ont_pricing_qty_source,
						secondary_default_ind,
						secondary_uom_name,
						tracking_quantity_ind,
						engineered_item_flag,
						atp_components_flag,
						atp_flag,
						over_shipment_tolerance,
						under_shipment_tolerance,
						over_return_tolerance,
						under_return_tolerance,
						downloadable_flag,
						electronic_flag,
						indivisible_flag,
						internal_order_enabled_flag,
						atp_rule_id,
						charge_periodicity_name,
						customer_order_enabled_flag,
						default_shipping_org_code,
						default_so_source_type,
						eligibility_compatibility_rule,
						financing_allowed_flag,
						internal_order_flag,
						picking_rule_id,
						returnable_flag,
						return_inspection_requirement,
						sales_product_type,
						back_to_back_enabled,
						shippable_item_flag,
						ship_model_complete_flag,
						so_transactions_flag,
						customer_order_flag,
						unit_weight,
						weight_uom_name,
						unit_volume,
						volume_uom_name,
						dimension_uom_name,
						unit_length,
						unit_width,
						unit_height,
						collateral_flag,
						container_item_flag,
						container_type_code,
						equipment_type,
						event_flag,
						internal_volume,
						maximum_load_weight,
						minimum_fill_percent,
						vehicle_item_flag,
						cas_number,
						hazardous_material_flag,
						process_costing_enabled_flag,
						process_execution_enabled_flag,
						process_quality_enabled_flag,
						process_supply_locator_id,
						process_supply_subinventory,
						process_yield_locator_id,
						process_yield_subinventory,
						recipe_enabled_flag,
						expense_account,
						un_number_code,
						unit_of_issue,
						rounding_factor,
						receive_close_tolerance,
						purchasing_tax_code,
						purchasing_item_flag,
						price_tolerance_percent,
						outsourced_assembly,
						outside_operation_uom_type,
						negotiation_required_flag,
						must_use_approved_vendor_flag,
						match_approval_level,
						invoice_match_option,
						list_price_per_unit,
						invoice_close_tolerance,
						hazard_class_code,
						buyer_name,
						taxable_flag,
						purchasing_enabled_flag,
						outside_operation_flag,
						market_price,
						asset_category_id,
						allow_item_desc_update_flag,
						allow_express_delivery_flag,
						allow_substitute_receipts_flag,
						allow_unordered_receipts_flag,
						days_early_receipt_allowed,
						days_late_receipt_allowed,
						receiving_routing_id,
						enforce_ship_to_location_code,
						qty_rcv_exception_code,
						qty_rcv_tolerance,
						receipt_days_exception_code,
						asset_creation_code,
						service_start_type_code,
						comms_nl_trackable_flag,
						css_enabled_flag,
						contract_item_type_code,
						standard_coverage,
						defect_tracking_on_flag,
						ib_item_instance_class,
						material_billable_flag,
						recovered_part_disp_code,
						serviceable_product_flag,
						service_starting_delay,
						service_duration,
						service_duration_period_name,
						serv_req_enabled_code,
						allow_suspend_flag,
						allow_terminate_flag,
						requires_fulfillment_loc_flag,
						requires_itm_association_flag,
						service_start_delay,
						service_duration_type_code,
						comms_activation_reqd_flag,
						serv_billing_enabled_flag,
						orderable_on_web_flag,
						back_orderable_flag,
						web_status,
						minimum_license_quantity,
						build_in_wip_flag,
						contract_manufacturing,
						wip_supply_locator_id,
						wip_supply_type,
						wip_supply_subinventory,
						overcompletion_tolerance_type,
						overcompletion_tolerance_value,
						inventory_carry_penalty,
						operation_slack_penalty,
						revision,
						style_item_flag,
						style_item_number,
						version_start_date,
						version_revision_code,
						version_label,
						start_upon_milestone_code,
						sales_product_sub_type,
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
						attribute21,
						attribute22,
						attribute23,
						attribute24,
						attribute25,
						attribute26,
						attribute27,
						attribute28,
						attribute29,
						attribute30,
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
						attribute_date1,
						attribute_date2,
						attribute_date3,
						attribute_date4,
						attribute_date5,
						attribute_timestamp1,
						attribute_timestamp2,
						attribute_timestamp3,
						attribute_timestamp4,
						attribute_timestamp5,
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
						global_attribute_number1,
						global_attribute_number2,
						global_attribute_number3,
						global_attribute_number4,
						global_attribute_number5,
						global_attribute_date1,
						global_attribute_date2,
						global_attribute_date3,
						global_attribute_date4,
						global_attribute_date5,
						prc_bu_name,
						force_purchase_lead_time_flag,
						replacement_type,
						buyer_email_address,
						default_expenditure_type,
						hard_pegging_level,
						comn_supply_prj_demand_flag,
						enable_iot_flag,
						packaging_string,
						create_supply_after_date,
						create_fixed_asset,
						under_compl_tolerance_type,
						under_compl_tolerance_value,
						repair_transaction_name,
						new_primary_uom_name,
						new_secondary_uom_name,
						null,
						null,
						null,
						null,
						null
						FROM XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_EXT ';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG: ' || SQL%rowcount);
            END IF;

		 --TABLE2
            IF gv_oci_file_name_item_categories LIKE '%EgpItemCategories%' THEN
                dbms_output.put_line('Creating external table XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_EXT');
                dbms_output.put_line(' XXCNV_PDH_C027_ITEM_CATEGORIES_GP_EXT : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_item_categories);
                dbms_cloud.create_external_table(
                    table_name      => 'XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_EXT',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_item_categories,
                    format          =>
                            JSON_OBJECT(
                                'skipheaders' VALUE '1',
                                'type' VALUE 'csv',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'ignoremissingcolumns' VALUE 'true',
                                'conversionerrors' VALUE 'store_null',
                                        'blankasnull' VALUE 'true'
                            ),
                    column_list     => 'transaction_type   varchar2(10),
                        batch_id  number(18),
                        batch_number varchar2(40),
                        item_number   varchar2(300),
                        organization_code varchar2(18),
                        category_set_name varchar2(30),
                        category_name  varchar2(250),
                        category_code  varchar2(820),
                        old_category_name  varchar2(250),
                        old_category_code  varchar2(820),
                        source_system_code varchar2(30) ,
                        source_system_reference varchar2(255),
                        start_date date,
                        end_date date '
                );

                dbms_output.put_line(' External table XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_EXT is created');
                EXECUTE IMMEDIATE 'INSERT INTO XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG (

					    transaction_type ,
                        batch_id,
                        batch_number,
                        item_number,
                        organization_code,
                        category_set_name,
                        category_name,
                        category_code,
                        old_category_name,
                        old_category_code,
                        source_system_code,
                        source_system_reference,
                        start_date,
                        end_date,
						file_name,
						error_message,
						import_status,
						file_reference_identifier ,
                        source_system 
						)
						select 
						transaction_type,
                        batch_id,
                        batch_number,
                        item_number,
                        organization_code,
                        category_set_name,
                        category_name,
                        category_code,
                        old_category_name,
                        old_category_code,
                        source_system_code,
                        source_system_reference,
                        start_date,
                        end_date,
                        null,
                        null,
                        null,
						null,
						null
						FROM XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_EXT ';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG: ' || SQL%rowcount);
            END IF;

		--TABLE3
            IF gv_oci_file_name_item_effs LIKE '%EgoItemIntfEff%' THEN
                dbms_output.put_line('Creating external table XXCNV_PDH_C027_EGO_ITEM_EFF_EXT');
                dbms_output.put_line(' XXCNV_PDH_C027_EGO_ITEM_EFF_EXT : '
                                     || gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_item_effs);
                dbms_cloud.create_external_table(
                    table_name      => 'XXCNV_PDH_C027_EGO_ITEM_EFF_EXT',
                    credential_name => gv_credential_name,
                    file_uri_list   => gv_oci_file_path
                                     || '/'
                                     || gv_oci_file_name_item_effs,
                    format          =>
                            JSON_OBJECT(
                                'skipheaders' VALUE '1',
                                'type' VALUE 'csv',
                                'rejectlimit' VALUE 'UNLIMITED',
                                'ignoremissingcolumns' VALUE 'true',
                                'conversionerrors' VALUE 'store_null',
                                        'blankasnull' VALUE 'true'
                            ),
                    column_list     => ' 
                     transaction_type		    varchar2(10)
,batch_id				    number
,batch_number			    varchar2(40)
,item_number			    varchar2(820)
,organization_code		    varchar2(18)
,source_system_code		    varchar2(30)
,source_system_reference	varchar2(255)
,context_code			    varchar2(80)
,attribute_char1			varchar2(4000)
,attribute_char2	        varchar2(4000)
,attribute_char3	        varchar2(4000)
,attribute_char4	        varchar2(4000)
,attribute_char5	        varchar2(4000)
,attribute_char6	        varchar2(4000)
,attribute_char7	        varchar2(4000)
,attribute_char8	        varchar2(4000)
,attribute_char9	        varchar2(4000)
,attribute_char10		    varchar2(4000)
,attribute_char11		    varchar2(4000)
,attribute_char12		    varchar2(4000)
,attribute_char13		    varchar2(4000)
,attribute_char14		    varchar2(4000)
,attribute_char15		    varchar2(4000)
,attribute_char16           varchar2(4000)
,attribute_char17           varchar2(4000)
,attribute_char18           varchar2(4000)
'
                );

                dbms_output.put_line('External table XXCNV_PDH_C027_EGO_ITEM_EFF_EXT is created');
                EXECUTE IMMEDIATE 'INSERT INTO XXCNV_PDH_C027_EGO_ITEM_EFF_STG (
					     transaction_type		
						,batch_id				
						,batch_number			
						,item_number			
						,organization_code		
						,source_system_code		
						,source_system_reference	
						,context_code			
						,attribute_char1			
						,attribute_char2			
						,attribute_char3			
						,attribute_char4			
						,attribute_char5			
						,attribute_char6			
						,attribute_char7			
						,attribute_char8			
						,attribute_char9			
						,attribute_char10		
						,attribute_char11		
						,attribute_char12		
						,attribute_char13		
						,attribute_char14		
						,attribute_char15		
						,attribute_char16
						,file_name
						,error_message
						,import_status
						,file_reference_identifier
                        ,source_system 
						)
						select 
					    transaction_type		
						,batch_id				
						,batch_number			
						,item_number			
						,organization_code		
						,source_system_code		
						,source_system_reference	
						,context_code			
						,attribute_char1			
						,attribute_char2			
						,attribute_char3			
						,attribute_char4			
						,attribute_char5			
						,attribute_char6			
						,attribute_char7			
						,attribute_char8			
						,attribute_char9			
						,attribute_char10		
						,attribute_char11		
						,attribute_char12		
						,attribute_char13		
						,attribute_char14		
						,attribute_char15		
						,attribute_char16
                        ,null
                        ,null
                        ,null
						,null
						,null
						FROM XXCNV_PDH_C027_EGO_ITEM_EFF_EXT';
                p_loading_status := gv_status_success;
                dbms_output.put_line('Inserted records in XXCNV_PDH_C027_EGO_ITEM_EFF_STG: ' || SQL%rowcount);
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error creating external table: ' || sqlerrm);
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
                xxcnv_pdh_c027_egp_system_items_stg
            WHERE
                file_reference_identifier IS NULL;

            dbms_output.put_line('Log:Inserted Records in the XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG from OCI Source Folder: ' || lv_row_count
            );
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_picked,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_item,
                p_attribute1        => NULL,
                p_attribute2        => lv_row_count,
                p_process_reference => NULL
            );

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

        BEGIN
        -- Count the number of rows in the external table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_pdh_c027_egp_item_categories_stg
            WHERE
                file_reference_identifier IS NULL;

            dbms_output.put_line('Log:Inserted Records in the XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG from OCI Source Folder: ' || lv_row_count
            );
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_picked,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_item_categories,
                p_attribute1        => NULL,
                p_attribute2        => lv_row_count,
                p_process_reference => NULL
            );

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG: ' || sqlerrm);
                p_loading_status := gv_status_failure;
                RETURN;
        END;

        BEGIN
        -- Count the number of rows in the external table
            SELECT
                COUNT(*)
            INTO lv_row_count
            FROM
                xxcnv_pdh_c027_ego_item_eff_stg
            WHERE
                file_reference_identifier IS NULL;

            dbms_output.put_line('Log:Inserted Records in the xxcnv_pdh_c027_ego_item_eff_stg from OCI Source Folder: ' || lv_row_count
            );
            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id     => gv_conversion_id,
                p_execution_id      => gv_execution_id,
                p_execution_step    => gv_status_picked,
                p_boundary_system   => gv_boundary_system,
                p_file_path         => gv_oci_file_path,
                p_file_name         => gv_oci_file_name_item,
                p_attribute1        => NULL,
                p_attribute2        => lv_row_count,
                p_process_reference => NULL
            );

            p_loading_status := gv_status_success;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('Error counting rows in xxcnv_pdh_c027_ego_item_eff_stg: ' || sqlerrm);
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
        BEGIN
            SELECT
                to_char(sysdate, 'YYYYMMDDHHMMSS')
            INTO gv_batch_id
            FROM
                dual;

            BEGIN
                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_system_items_stg
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
                    xxcnv_pdh_c027_egp_system_items_stg
                WHERE
                    execution_id = gv_execution_id;

                IF lv_row_count <> 0 THEN 

		  -- Initialize ERROR_MESSAGE to an empty string if it is NULL
                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            error_message = ''
                        WHERE
                            error_message IS NULL;

                    END;
                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            error_message = error_message || '|Item_number should not be NULL'
                        WHERE
                            item_number IS NULL
                            AND file_reference_identifier IS NULL
                            AND execution_id = gv_execution_id;

                        dbms_output.put_line('Item_number is validated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg e
                        SET
                            error_message = error_message || '|Duplicate item_number exists'
                        WHERE
                            EXISTS (
                                SELECT
                                    1
                                FROM
                                    xxcnv_pdh_c027_egp_system_items_stg i
                                WHERE
                                        i.item_number = e.item_number
                                    AND i.execution_id = gv_execution_id
                                    AND i.file_reference_identifier IS NULL
                                GROUP BY
                                    i.item_number
                                HAVING
                                    COUNT(*) > 1
                            )
                            AND e.execution_id = gv_execution_id;

                        dbms_output.put_line('Duplicate item_number validation completed');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            error_message = error_message || '|Organization Code should not be NULL'
                        WHERE
                            organization_code IS NULL
                            AND file_reference_identifier IS NULL
                            AND execution_id = gv_execution_id;

                        dbms_output.put_line('organization_code is validated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            error_message = error_message || '|Description should not be NULL'
                        WHERE
                            description IS NULL
                            AND file_reference_identifier IS NULL
                            AND execution_id = gv_execution_id;

                        dbms_output.put_line('organization_code is validated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            error_message = error_message || '|primary_uom_name should not be NULL'
                        WHERE
                            primary_uom_name IS NULL
                            AND primary_uom_name = 'NULL'
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('primary_uom_name is validated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            primary_uom_name = 'Each'
                        WHERE
                                primary_uom_name = 'each'
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('primary_uom_name is updated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            error_message = error_message || '|User Item Type should not be NULL'
                        WHERE
                            item_type IS NULL
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('User Item Type is validated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg s
                        SET
                            s.template_name = (
                                SELECT
                                    i.template_name
                                FROM
                                    xxcnv_item_template_mapping i
                                WHERE
                                        1 = 1
                                    AND i.item_type = s.item_type
                            )
                        WHERE
                            s.item_type IS NOT NULL
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('template_name is validated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            oc_item_type = (
                                SELECT
                                    type
                                FROM
                                    xxcnv_item_type_mapping
                                WHERE
                                    upper(name) = upper(item_type)
                            )
                        WHERE
                            item_type IS NOT NULL
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('User Item Type is updated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            error_message = error_message || '|User Item Type should not be NULL after the transformation'
                        WHERE
                            oc_item_type IS NULL
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('User Item Type is validated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            description = replace(description, '"', '')
                        WHERE
                                1 = 1
                            AND description IS NOT NULL
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('Description is trimmed');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            long_description = replace(long_description, '"', '')
                        WHERE
                                1 = 1
                            AND long_description IS NOT NULL
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('Description is trimmed');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            description = substr(description, 1, 236)
                        WHERE
                                1 = 1
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('Description is trimmed');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            description = '"'
                                          || description
                                          || '"'
                        WHERE
                            description LIKE '%,%'
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('Description is updated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            long_description = '"'
                                               || long_description
                                               || '"'
                        WHERE
                            long_description LIKE '%,%'
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('Long Description is updated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            item_number = '"'
                                          || item_number
                                          || '"'
                        WHERE
                            item_number LIKE '%,%'
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('Item_number is updated');
                    END; 


    -- Updating constant values --

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            transaction_type = 'CREATE',
                            item_class_name = 'Kaseya Item Class',
                            current_phase_code = 'Production',
                            inventory_item_status_code = 'Active',
                            list_price_per_unit = 0;

                        dbms_output.put_line('Constant fields are updated');
                    END;

        -- Update import_status based on error_message
                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
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
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
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
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            source_system = gv_boundary_system
                        WHERE
                            file_reference_identifier IS NULL
                            AND execution_id = gv_execution_id;

                        dbms_output.put_line('source_system is updated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            file_name = gv_oci_file_name_item
                        WHERE
                            file_reference_identifier IS NULL
                            AND execution_id = gv_execution_id;

                        dbms_output.put_line('file_name column is updated');
                    END;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
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
	  -- Check if there are any error messages
                    SELECT
                        COUNT(*)
                    INTO lv_error_count
                    FROM
                        xxcnv_pdh_c027_egp_system_items_stg
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
                            p_file_name         => gv_oci_file_name_item,
                            p_attribute1        => gv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );
                    END IF;

                    BEGIN
                        UPDATE xxcnv_pdh_c027_egp_system_items_stg
                        SET
                            file_reference_identifier = gv_execution_id
                                                        || '_'
                                                        || gv_status_success
                        WHERE
                            error_message IS NULL
                            AND file_reference_identifier IS NULL;

                        dbms_output.put_line('file_reference_identifier column is updated');
                    END;

	 -- Check if there are any error messages
                    SELECT
                        COUNT(*)
                    INTO lv_error_count
                    FROM
                        xxcnv_pdh_c027_egp_system_items_stg
                    WHERE
                        error_message IS NULL
                        AND execution_id = gv_execution_id;

                    IF
                        lv_error_count > 0
                        AND gv_oci_file_name_item IS NOT NULL
                    THEN
                        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                            p_conversion_id     => gv_conversion_id,
                            p_execution_id      => gv_execution_id,
                            p_execution_step    => gv_status_validated,
                            p_boundary_system   => gv_boundary_system,
                            p_file_path         => gv_oci_file_path,
                            p_file_name         => gv_oci_file_name_item,
                            p_attribute1        => gv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );
                    END IF;

                    IF gv_oci_file_name_item IS NULL THEN
                        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                            p_conversion_id     => gv_conversion_id,
                            p_execution_id      => gv_execution_id,
                            p_execution_step    => gv_file_not_found,
                            p_boundary_system   => gv_boundary_system,
                            p_file_path         => gv_oci_file_path,
                            p_file_name         => gv_oci_file_name_item,
                            p_attribute1        => gv_batch_id,
                            p_attribute2        => NULL,
                            p_process_reference => NULL
                        );
                    END IF;

                ELSE
                    dbms_output.put_line('No Data is found in interface tables. Data is not loaded from ext to stg ');
                END IF;

            END;

        END;


  -- validation 2
        BEGIN
            BEGIN
                UPDATE xxcnv_pdh_c027_egp_item_categories_stg
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
                xxcnv_pdh_c027_egp_item_categories_stg
            WHERE
                execution_id = gv_execution_id;

            IF lv_row_count <> 0 THEN
                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
                    SET
                        error_message = ''
                    WHERE
                        error_message IS NULL;

                END;
                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
                    SET
                        error_message = error_message || '|Item_number should not be NULL'
                    WHERE
                        item_number IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Item_number is validated');
                END;

                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
                    SET
                        item_number = '"'
                                      || item_number
                                      || '"'
                    WHERE
                        item_number LIKE '%,%'
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Item_number is updated');
                END;

                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
                    SET
                        error_message = error_message || '|ORGANIZATION_CODE should not be NULL'
                    WHERE
                        organization_code IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('ORGANIZATION_CODE is validated');
                END;

    --updating constant values 

                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
                    SET
                        category_set_name = 'Purchasing Catalog',
                        transaction_type = 'CREATE'
                    WHERE
                            1 = 1
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Constant values are updated ');
                END;

                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
                    SET
                        error_message = error_message || '|CATEGORY_NAME should not be NULL'
                    WHERE
                        category_name IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('CATEGORY_NAME is validated');
                END;

                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
                    SET
                        error_message = error_message || 'Child record failed because Parent failed'
                    WHERE
                        item_number IN (
                            SELECT
                                item_number
                            FROM
                                xxcnv_pdh_c027_egp_system_items_stg
                            WHERE
                                    import_status = 'ERROR'
                                AND error_message IS NOT NULL
                                AND execution_id = gv_execution_id
                        )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
                    SET
                        source_system = gv_boundary_system;

                    dbms_output.put_line('source_system is updated');
                END;
                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
                    SET
                        file_name = gv_oci_file_name_item_categories
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('file_name column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
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
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
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
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
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

                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_pdh_c027_egp_item_categories_stg
                WHERE
                    error_message IS NOT NULL
                    AND execution_id = gv_execution_id;

                IF lv_error_count > 0 THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_failed,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_item_categories,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

                BEGIN
                    UPDATE xxcnv_pdh_c027_egp_item_categories_stg
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

                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_pdh_c027_egp_item_categories_stg
                WHERE
                    error_message IS NULL
                    AND execution_id = gv_execution_id;

                IF
                    lv_error_count > 0
                    AND gv_oci_file_name_item_categories IS NOT NULL
                THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_validated,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_item_categories,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

                IF gv_oci_file_name_item_categories IS NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_file_not_found,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_item_categories,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

            ELSE
                dbms_output.put_line('No Data is found in interface tables. Data is not loaded from ext to stg ');
            END IF;

            BEGIN
                UPDATE xxcnv_pdh_c027_egp_item_categories_stg r
                SET
                    error_message = nvl(error_message, '')
                                    || 'Parent Record failed at validation'
                WHERE
                    r.item_number IN (
                        SELECT
                            i.item_number
                        FROM
                            xxcnv_pdh_c027_egp_system_items_stg i
                        WHERE
                                i.import_status = 'ERROR'
                            AND i.execution_id = gv_execution_id
                    )
                    AND r.file_reference_identifier IS NULL;

                dbms_output.put_line('Errors propagated from XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG to XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG'
                );
            END;

        END;

 -- validation 3
        BEGIN
            BEGIN
                UPDATE xxcnv_pdh_c027_ego_item_eff_stg
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
                xxcnv_pdh_c027_ego_item_eff_stg
            WHERE
                execution_id = gv_execution_id;

            IF lv_row_count <> 0 THEN
                BEGIN
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
                    SET
                        error_message = ''
                    WHERE
                        error_message IS NULL;

                END;
                BEGIN
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
                    SET
                        error_message = error_message || '|Item_number should not be NULL'
                    WHERE
                        item_number IS NULL
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Item_number is validated');
                END;

                BEGIN
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
                    SET
                        item_number = '"'
                                      || item_number
                                      || '"'
                    WHERE
                        item_number LIKE '%,%'
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Item_number is updated');
                END;

                BEGIN
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
                    SET
                        attribute_char2 = (
                            SELECT
                                coa_oc_desc
                            FROM
                                xxmap.xxmap_gl_e001_kaseya_ns_productline
                            WHERE
                                ns_productline_attribute_1 = attribute_char2
                        )
                    WHERE
                        attribute_char2 IS NOT NULL
                        AND file_reference_identifier IS NULL;

                    dbms_output.put_line('Attribute_char2 is validated');
                END;

    --updating constant values 

                BEGIN
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
                    SET
                        transaction_type = 'CREATE',
                        organization_code = 'KAS_ITM_MST',
                        context_code = 'Item Additional Attributes'
                    WHERE
                            1 = 1
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('Constant values are updated ');
                END;

                BEGIN
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
                    SET
                        error_message = error_message || 'Child record failed because Parent failed'
                    WHERE
                        item_number IN (
                            SELECT
                                item_number
                            FROM
                                xxcnv_pdh_c027_egp_system_items_stg
                            WHERE
                                    import_status = 'ERROR'
                                AND error_message IS NOT NULL
                                AND execution_id = gv_execution_id
                        )
                        AND file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                END;

                BEGIN
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
                    SET
                        source_system = gv_boundary_system;

                    dbms_output.put_line('source_system is updated');
                END;
                BEGIN
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
                    SET
                        file_name = gv_oci_file_name_item_effs
                    WHERE
                        file_reference_identifier IS NULL
                        AND execution_id = gv_execution_id;

                    dbms_output.put_line('file_name column is updated');
                END;

                BEGIN
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
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
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
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
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
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

                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_pdh_c027_ego_item_eff_stg
                WHERE
                    error_message IS NOT NULL
                    AND execution_id = gv_execution_id;

                IF lv_error_count > 0 THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_failed,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_item_effs,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

                BEGIN
                    UPDATE xxcnv_pdh_c027_ego_item_eff_stg
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

                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_pdh_c027_ego_item_eff_stg
                WHERE
                    error_message IS NULL
                    AND execution_id = gv_execution_id;

                IF
                    lv_error_count > 0
                    AND gv_oci_file_name_item_effs IS NOT NULL
                THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_status_validated,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_item_effs,
                        p_attribute1        => gv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );
                END IF;

                IF gv_oci_file_name_item_effs IS NULL THEN
                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id     => gv_conversion_id,
                        p_execution_id      => gv_execution_id,
                        p_execution_step    => gv_file_not_found,
                        p_boundary_system   => gv_boundary_system,
                        p_file_path         => gv_oci_file_path,
                        p_file_name         => gv_oci_file_name_item_effs,
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
-- COMMENT   : This procedure is used for creating the FBDI CSV file after all validations.
================================================================================================================================= */
    PROCEDURE create_fbdi_file_prc IS
        lv_error_count NUMBER;
        lv_batch_id    VARCHAR2(200);
    BEGIN

--table1

        BEGIN
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_pdh_c027_egp_item_categories_stg
                WHERE
                    execution_id = gv_execution_id;

            END;
            BEGIN
                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_pdh_c027_egp_item_categories_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Error count for batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_error_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG  batch_id: ' || lv_batch_id);
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking error_message column for XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG  batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
            END;

            IF lv_error_count > 0 THEN
                BEGIN
                    dbms_output.put_line('FilePath: '
                                         || replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder));
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                         || '/'
                                         || lv_batch_id
                                         || gv_oci_file_name_item_categories,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'header' VALUE FALSE,
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600'
                                ),
                        query           => 'SELECT 
                                         TRANSACTION_TYPE,
										  BATCH_ID ,
										  BATCH_NUMBER,
										  ITEM_NUMBER ,
										  ORGANIZATION_CODE,
										  CATEGORY_SET_NAME,
										  CATEGORY_NAME,
										  CATEGORY_CODE,
										  OLD_CATEGORY_NAME,
										  OLD_CATEGORY_CODE,
										  SOURCE_SYSTEM_CODE,
										  SOURCE_SYSTEM_REFERENCE,
										  TO_CHAR(START_DATE, ''YYYY/MM/DD'') AS START_DATE,
										  TO_CHAR(END_DATE, ''YYYY/MM/DD'') AS END_DATE            
										  FROM XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG
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
                                       || gv_oci_file_name_item_categories
                                       || '.csv',
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for  XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                END;
            ELSE
                dbms_output.put_line('Process Stopped for XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG batch_id '
                                     || lv_batch_id
                                     || ': Error message columns contain data.');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
        END; 

--table2

        BEGIN
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_pdh_c027_ego_item_eff_stg
                WHERE
                    execution_id = gv_execution_id;

            END;
            BEGIN
                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_pdh_c027_ego_item_eff_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Error count for batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_error_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for xxcnv_pdh_c027_ego_item_eff_stg batch_id: ' || lv_batch_id);
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking error_message column for xxcnv_pdh_c027_ego_item_eff_stg batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
            END;

            IF lv_error_count > 0 THEN
                BEGIN
                    dbms_output.put_line('FilePath: '
                                         || replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder));
                    dbms_cloud.export_data(
                        credential_name => gv_credential_name,
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                         || '/'
                                         || lv_batch_id
                                         || gv_oci_file_name_item_effs,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'header' VALUE FALSE,
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600'
                                ),
                        query           => 'SELECT 
											 transaction_type		
											,batch_id				
											,batch_number			
											,item_number			
											,organization_code		
											,source_system_code		
											,source_system_reference	
											,context_code			
											,attribute_char1			
											,attribute_char2			
											,attribute_char3			
											,attribute_char4	
                                            /* Commented for v1.1
											,NULL			
											,NULL			
											,NULL			
											,NULL			
											,NULL			
											,NULL		
											,NULL		
											,NULL		
											,NULL	
                                            */
                                            /* start for v1.1 */
                                            ,NULL attribute_char5
                                            ,NULL attribute_char6
                                            ,NULL attribute_char7
                                            ,NULL attribute_char8
                                            ,NULL attribute_char9
                                            ,NULL attribute_char10
                                            ,NULL attribute_char11
                                            ,NULL attribute_char12
                                            ,NULL attribute_char13 
                                             /* end for v1.1 */
											,attribute_char14		
											,attribute_char15		
											,attribute_char16          
										    FROM xxcnv_pdh_c027_ego_item_eff_stg
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
                                       || gv_oci_file_name_item_effs
                                       || '.csv',
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for xxcnv_pdh_c027_ego_item_eff_stg batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                END;
            ELSE
                dbms_output.put_line('Process Stopped for xxcnv_pdh_c027_ego_item_eff_stg batch_id '
                                     || lv_batch_id
                                     || ': Error message columns contain data.');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred: ' || sqlerrm);
        END; 

--table3
        BEGIN
            lv_error_count := 0;
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_pdh_c027_egp_system_items_stg
                WHERE
                    execution_id = gv_execution_id;

            END;
            BEGIN
                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                SELECT
                    COUNT(*)
                INTO lv_error_count
                FROM
                    xxcnv_pdh_c027_egp_system_items_stg
                WHERE
                        batch_id = lv_batch_id
                    AND file_reference_identifier = gv_execution_id
                                                    || '_'
                                                    || gv_status_success;

                dbms_output.put_line('Error count for batch_id '
                                     || lv_batch_id
                                     || ': '
                                     || lv_error_count);
            EXCEPTION
                WHEN no_data_found THEN
                    dbms_output.put_line('No data found for XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG  batch_id: ' || lv_batch_id);
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking error_message column for XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG  batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
            END;

            IF lv_error_count > 0 THEN
                BEGIN
                    dbms_cloud.export_data(
                        credential_name => 'OCI$RESOURCE_PRINCIPAL',
                        file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder)
                                         || '/'
                                         || lv_batch_id
                                         || gv_oci_file_name_item,
                        format          =>
                                JSON_OBJECT(
                                    'type' VALUE 'csv',
                                    'header' VALUE FALSE,
                                    'trimspaces' VALUE 'rtrim',
                                    'maxfilesize' VALUE '629145600'
                                ),
                        query           => 'SELECT 
											transaction_type,
											batch_id,
											batch_number,
											item_number,
											outside_process_service_flag,
											organization_code,
											description,
											template_name,
											source_system_code,
											source_system_reference,
											source_system_reference_desc,
											item_class_name,
											primary_uom_name,
											current_phase_code,
											inventory_item_status_code,
											new_item_class_name,
											asset_tracked_flag,
											allow_MAIN_PRCtenance_asset_flag,
											enable_genealogy_tracking_flag,
											asset_class,
											eam_item_type,
											eam_activity_type_code,
											eam_activity_cause_code,
											eam_act_notification_flag,
											eam_act_shutdown_status,
											eam_activity_source_code,
											costing_enabled_flag,
											std_lot_size,
											inventory_asset_flag,
											default_include_in_rollup_flag,
											order_cost,
											vmi_minimum_days,
											vmi_fixed_order_quantity,
											vmi_minimum_units,
											asn_autoexpire_flag,
											carrying_cost,
											consigned_flag,
											fixed_days_supply,
											fixed_lot_multiplier,
											fixed_order_quantity,
											forecast_horizon,
											inventory_planning_code,
											safety_stock_planning_method,
											demand_period,
											days_of_cover,
											min_minmax_quantity,
											max_minmax_quantity,
											minimum_order_quantity,
											maximum_order_quantity,
											planner_code,
											planning_make_buy_code,
											source_subinventory,
											source_type,
											so_authorization_flag,
											subcontracting_component,
											vmi_forecast_type,
											vmi_maximum_units,
											vmi_maximum_days,
											source_organization_code,
											restrict_subinventories_code,
											restrict_locators_code,
											child_lot_flag,
											child_lot_prefix,
											child_lot_starting_number,
											child_lot_validation_flag,
											copy_lot_attribute_flag,
											expiration_action_code,
											expiration_action_interval,
											stock_enabled_flag,
											start_auto_lot_number,
											shelf_life_code,
											shelf_life_days,
											serial_number_control_code,
											serial_status_enabled,
											revision_qty_control_code,
											retest_interval,
											auto_lot_alpha_prefix,
											auto_serial_alpha_prefix,
											bulk_picked_flag,
											check_shortages_flag,
											cycle_count_enabled_flag,
											default_grade,
											grade_control_flag,
											hold_days,
											lot_divisible_flag,
											maturity_days,
											default_lot_status_id,
											default_serial_status_id,
											lot_split_enabled,
											lot_merge_enabled,
											inventory_item_flag,
											location_control_code,
											lot_control_code,
											lot_status_enabled,
											lot_substitution_enabled,
											lot_translate_enabled,
											mtl_transactions_enabled_flag,
											positive_measurement_error,
											negative_measurement_error,
											parent_child_generation_flag,
											reservable_type,
											start_auto_serial_number,
											invoicing_rule_name,
											tax_code,
											sales_account,
											payment_terms_name,
											invoice_enabled_flag,
											invoiceable_item_flag,
											accounting_rule_name,
											auto_created_config_flag,
											replenish_to_order_flag,
											pick_components_flag,
											base_item_number,
											effectivity_control,
											config_orgs,
											config_match,
											config_model_type,
											bom_item_type,
											cum_manufacturing_lead_time,
											preprocessing_lead_time,
											cumulative_total_lead_time,
											fixed_lead_time,
											variable_lead_time,
											full_lead_time,
											lead_time_lot_size,
											postprocessing_lead_time,
											ato_forecast_control,
											critical_component_flag,
											acceptable_early_days,
											create_supply_flag,
											days_tgt_inv_supply,
											days_tgt_inv_window,
											days_max_inv_supply,
											days_max_inv_window,
											demand_time_fence_code,
											demand_time_fence_days,
											drp_planned_flag,
											end_assembly_pegging_flag,
											exclude_from_budget_flag,
											mrp_calculate_atp_flag,
											mrp_planning_code,
											planned_inv_point_flag,
											planning_time_fence_code,
											planning_time_fence_days,
											preposition_point,
											release_time_fence_code,
											release_time_fence_days,
											repair_leadtime,
											repair_yield,
											repair_program,
											rounding_control_type,
											shrinkage_rate,
											substitution_window_code,
											substitution_window_days,
											trade_item_descriptor,
											allowed_units_lookup_code,
											dual_uom_deviation_high,
											dual_uom_deviation_low,
											oc_item_type,
											long_description,
											html_long_description,
											ont_pricing_qty_source,
											secondary_default_ind,
											secondary_uom_name,
											tracking_quantity_ind,
											engineered_item_flag,
											atp_components_flag,
											atp_flag,
											over_shipment_tolerance,
											under_shipment_tolerance,
											over_return_tolerance,
											under_return_tolerance,
											downloadable_flag,
											electronic_flag,
											indivisible_flag,
											internal_order_enabled_flag,
											atp_rule_id,
											charge_periodicity_name,
											customer_order_enabled_flag,
											default_shipping_org_code,
											default_so_source_type,
											eligibility_compatibility_rule,
											financing_allowed_flag,
											internal_order_flag,
											picking_rule_id,
											returnable_flag,
											return_inspection_requirement,
											sales_product_type,
											back_to_back_enabled,
											shippable_item_flag,
											ship_model_complete_flag,
											so_transactions_flag,
											customer_order_flag,
											unit_weight,
											weight_uom_name,
											unit_volume,
											volume_uom_name,
											dimension_uom_name,
											unit_length,
											unit_width,
											unit_height,
											collateral_flag,
											container_item_flag,
											container_type_code,
											equipment_type,
											event_flag,
											internal_volume,
											maximum_load_weight,
											minimum_fill_percent,
											vehicle_item_flag,
											cas_number,
											hazardous_material_flag,
											process_costing_enabled_flag,
											process_execution_enabled_flag,
											process_quality_enabled_flag,
											process_supply_locator_id,
											process_supply_subinventory,
											process_yield_locator_id,
											process_yield_subinventory,
											recipe_enabled_flag,
											expense_account,
											un_number_code,
											unit_of_issue,
											rounding_factor,
											receive_close_tolerance,
											purchasing_tax_code,
											purchasing_item_flag,
											price_tolerance_percent,
											outsourced_assembly,
											outside_operation_uom_type,
											negotiation_required_flag,
											must_use_approved_vendor_flag,
											match_approval_level,
											invoice_match_option,
											list_price_per_unit,
											invoice_close_tolerance,
											hazard_class_code,
											buyer_name,
											taxable_flag,
											purchasing_enabled_flag,
											outside_operation_flag,
											market_price,
											asset_category_id,
											allow_item_desc_update_flag,
											allow_express_delivery_flag,
											allow_substitute_receipts_flag,
											allow_unordered_receipts_flag,
											days_early_receipt_allowed,
											days_late_receipt_allowed,
											receiving_routing_id,
											enforce_ship_to_location_code,
											qty_rcv_exception_code,
											qty_rcv_tolerance,
											receipt_days_exception_code,
											asset_creation_code,
											service_start_type_code,
											comms_nl_trackable_flag,
											css_enabled_flag,
											contract_item_type_code,
											standard_coverage,
											defect_tracking_on_flag,
											ib_item_instance_class,
											material_billable_flag,
											recovered_part_disp_code,
											serviceable_product_flag,
											service_starting_delay,
											service_duration,
											service_duration_period_name,
											serv_req_enabled_code,
											allow_suspend_flag,
											allow_terminate_flag,
											requires_fulfillment_loc_flag,
											requires_itm_association_flag,
											service_start_delay,
											service_duration_type_code,
											comms_activation_reqd_flag,
											serv_billing_enabled_flag,
											orderable_on_web_flag,
											back_orderable_flag,
											web_status,
											minimum_license_quantity,
											build_in_wip_flag,
											contract_manufacturing,
											wip_supply_locator_id,
											wip_supply_type,
											wip_supply_subinventory,
											overcompletion_tolerance_type,
											overcompletion_tolerance_value,
											inventory_carry_penalty,
											operation_slack_penalty,
											revision,
											style_item_flag,
											style_item_number,
											version_start_date,
											version_revision_code,
											version_label,
											start_upon_milestone_code,
											sales_product_sub_type,
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
											attribute21,
											attribute22,
											attribute23,
											attribute24,
											attribute25,
											attribute26,
											attribute27,
											attribute28,
											attribute29,
											attribute30,
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
											attribute_date1,
											attribute_date2,
											attribute_date3,
											attribute_date4,
											attribute_date5,
											attribute_timestamp1,
											attribute_timestamp2,
											attribute_timestamp3,
											attribute_timestamp4,
											attribute_timestamp5,
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
											global_attribute_number1,
											global_attribute_number2,
											global_attribute_number3,
											global_attribute_number4,
											global_attribute_number5,
											global_attribute_date1,
											global_attribute_date2,
											global_attribute_date3,
											global_attribute_date4,
											global_attribute_date5,
											prc_bu_name,
											force_purchase_lead_time_flag,
											replacement_type,
											buyer_email_address,
											default_expenditure_type,
											hard_pegging_level,
											comn_supply_prj_demand_flag,
											enable_iot_flag,
											packaging_string,
											create_supply_after_date,
											create_fixed_asset,
											under_compl_tolerance_type,
											under_compl_tolerance_value,
											repair_transaction_name,
											new_primary_uom_name,
											new_secondary_uom_name
                                            FROM XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG
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
                                       || gv_oci_file_name_item,
                        p_attribute1        => lv_batch_id,
                        p_attribute2        => NULL,
                        p_process_reference => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG batch_id '
                                             || lv_batch_id
                                             || ': '
                                             || sqlerrm);
                END;
            ELSE
                dbms_output.put_line('Process Stopped for XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG batch_id '
                                     || lv_batch_id
                                     || ': Error message columns contain data.');
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('An error occurred ON FBDI Procedure: ' || sqlerrm);
        END;

    END create_fbdi_file_prc;

/*==============================================================================================================================
-- PROCEDURE : CREATE_RECON_REPORT_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating properties file.
================================================================================================================================= */
    PROCEDURE create_recon_report_prc IS
        lv_batch_id VARCHAR2(50);
    BEGIN
        BEGIN
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_pdh_c027_egp_system_items_stg
                WHERE
                    execution_id = gv_execution_id;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('No Data Found');
            END;

            BEGIN
                dbms_cloud.export_data(
                    credential_name => gv_credential_name,
                    file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                     || '/'
                                     || lv_batch_id
                                     || gv_oci_file_name_item,
                    format          =>
                            JSON_OBJECT(
                                'type' VALUE 'csv',
                                'header' VALUE TRUE,
                                'maxfilesize' VALUE '629145600'
                            ),
                    query           => 'SELECT 
                                            transaction_type,
											batch_id,
											batch_number,
											item_number,
											outside_process_service_flag,
											organization_code,
											description,
											template_name,
											source_system_code,
											source_system_reference,
											source_system_reference_desc,
											item_class_name,
											primary_uom_name,
											current_phase_code,
											inventory_item_status_code,
											new_item_class_name,
											asset_tracked_flag,
											allow_MAIN_PRCtenance_asset_flag,
											enable_genealogy_tracking_flag,
											asset_class,
											eam_item_type,
											eam_activity_type_code,
											eam_activity_cause_code,
											eam_act_notification_flag,
											eam_act_shutdown_status,
											eam_activity_source_code,
											costing_enabled_flag,
											std_lot_size,
											inventory_asset_flag,
											default_include_in_rollup_flag,
											order_cost,
											vmi_minimum_days,
											vmi_fixed_order_quantity,
											vmi_minimum_units,
											asn_autoexpire_flag,
											carrying_cost,
											consigned_flag,
											fixed_days_supply,
											fixed_lot_multiplier,
											fixed_order_quantity,
											forecast_horizon,
											inventory_planning_code,
											safety_stock_planning_method,
											demand_period,
											days_of_cover,
											min_minmax_quantity,
											max_minmax_quantity,
											minimum_order_quantity,
											maximum_order_quantity,
											planner_code,
											planning_make_buy_code,
											source_subinventory,
											source_type,
											so_authorization_flag,
											subcontracting_component,
											vmi_forecast_type,
											vmi_maximum_units,
											vmi_maximum_days,
											source_organization_code,
											restrict_subinventories_code,
											restrict_locators_code,
											child_lot_flag,
											child_lot_prefix,
											child_lot_starting_number,
											child_lot_validation_flag,
											copy_lot_attribute_flag,
											expiration_action_code,
											expiration_action_interval,
											stock_enabled_flag,
											start_auto_lot_number,
											shelf_life_code,
											shelf_life_days,
											serial_number_control_code,
											serial_status_enabled,
											revision_qty_control_code,
											retest_interval,
											auto_lot_alpha_prefix,
											auto_serial_alpha_prefix,
											bulk_picked_flag,
											check_shortages_flag,
											cycle_count_enabled_flag,
											default_grade,
											grade_control_flag,
											hold_days,
											lot_divisible_flag,
											maturity_days,
											default_lot_status_id,
											default_serial_status_id,
											lot_split_enabled,
											lot_merge_enabled,
											inventory_item_flag,
											location_control_code,
											lot_control_code,
											lot_status_enabled,
											lot_substitution_enabled,
											lot_translate_enabled,
											mtl_transactions_enabled_flag,
											positive_measurement_error,
											negative_measurement_error,
											parent_child_generation_flag,
											reservable_type,
											start_auto_serial_number,
											invoicing_rule_name,
											tax_code,
											sales_account,
											payment_terms_name,
											invoice_enabled_flag,
											invoiceable_item_flag,
											accounting_rule_name,
											auto_created_config_flag,
											replenish_to_order_flag,
											pick_components_flag,
											base_item_number,
											effectivity_control,
											config_orgs,
											config_match,
											config_model_type,
											bom_item_type,
											cum_manufacturing_lead_time,
											preprocessing_lead_time,
											cumulative_total_lead_time,
											fixed_lead_time,
											variable_lead_time,
											full_lead_time,
											lead_time_lot_size,
											postprocessing_lead_time,
											ato_forecast_control,
											critical_component_flag,
											acceptable_early_days,
											create_supply_flag,
											days_tgt_inv_supply,
											days_tgt_inv_window,
											days_max_inv_supply,
											days_max_inv_window,
											demand_time_fence_code,
											demand_time_fence_days,
											drp_planned_flag,
											end_assembly_pegging_flag,
											exclude_from_budget_flag,
											mrp_calculate_atp_flag,
											mrp_planning_code,
											planned_inv_point_flag,
											planning_time_fence_code,
											planning_time_fence_days,
											preposition_point,
											release_time_fence_code,
											release_time_fence_days,
											repair_leadtime,
											repair_yield,
											repair_program,
											rounding_control_type,
											shrinkage_rate,
											substitution_window_code,
											substitution_window_days,
											trade_item_descriptor,
											allowed_units_lookup_code,
											dual_uom_deviation_high,
											dual_uom_deviation_low,
											item_type,
											long_description,
											html_long_description,
											ont_pricing_qty_source,
											secondary_default_ind,
											secondary_uom_name,
											tracking_quantity_ind,
											engineered_item_flag,
											atp_components_flag,
											atp_flag,
											over_shipment_tolerance,
											under_shipment_tolerance,
											over_return_tolerance,
											under_return_tolerance,
											downloadable_flag,
											electronic_flag,
											indivisible_flag,
											internal_order_enabled_flag,
											atp_rule_id,
											charge_periodicity_name,
											customer_order_enabled_flag,
											default_shipping_org_code,
											default_so_source_type,
											eligibility_compatibility_rule,
											financing_allowed_flag,
											internal_order_flag,
											picking_rule_id,
											returnable_flag,
											return_inspection_requirement,
											sales_product_type,
											back_to_back_enabled,
											shippable_item_flag,
											ship_model_complete_flag,
											so_transactions_flag,
											customer_order_flag,
											unit_weight,
											weight_uom_name,
											unit_volume,
											volume_uom_name,
											dimension_uom_name,
											unit_length,
											unit_width,
											unit_height,
											collateral_flag,
											container_item_flag,
											container_type_code,
											equipment_type,
											event_flag,
											internal_volume,
											maximum_load_weight,
											minimum_fill_percent,
											vehicle_item_flag,
											cas_number,
											hazardous_material_flag,
											process_costing_enabled_flag,
											process_execution_enabled_flag,
											process_quality_enabled_flag,
											process_supply_locator_id,
											process_supply_subinventory,
											process_yield_locator_id,
											process_yield_subinventory,
											recipe_enabled_flag,
											expense_account,
											un_number_code,
											unit_of_issue,
											rounding_factor,
											receive_close_tolerance,
											purchasing_tax_code,
											purchasing_item_flag,
											price_tolerance_percent,
											outsourced_assembly,
											outside_operation_uom_type,
											negotiation_required_flag,
											must_use_approved_vendor_flag,
											match_approval_level,
											invoice_match_option,
											list_price_per_unit,
											invoice_close_tolerance,
											hazard_class_code,
											buyer_name,
											taxable_flag,
											purchasing_enabled_flag,
											outside_operation_flag,
											market_price,
											asset_category_id,
											allow_item_desc_update_flag,
											allow_express_delivery_flag,
											allow_substitute_receipts_flag,
											allow_unordered_receipts_flag,
											days_early_receipt_allowed,
											days_late_receipt_allowed,
											receiving_routing_id,
											enforce_ship_to_location_code,
											qty_rcv_exception_code,
											qty_rcv_tolerance,
											receipt_days_exception_code,
											asset_creation_code,
											service_start_type_code,
											comms_nl_trackable_flag,
											css_enabled_flag,
											contract_item_type_code,
											standard_coverage,
											defect_tracking_on_flag,
											ib_item_instance_class,
											material_billable_flag,
											recovered_part_disp_code,
											serviceable_product_flag,
											service_starting_delay,
											service_duration,
											service_duration_period_name,
											serv_req_enabled_code,
											allow_suspend_flag,
											allow_terminate_flag,
											requires_fulfillment_loc_flag,
											requires_itm_association_flag,
											service_start_delay,
											service_duration_type_code,
											comms_activation_reqd_flag,
											serv_billing_enabled_flag,
											orderable_on_web_flag,
											back_orderable_flag,
											web_status,
											minimum_license_quantity,
											build_in_wip_flag,
											contract_manufacturing,
											wip_supply_locator_id,
											wip_supply_type,
											wip_supply_subinventory,
											overcompletion_tolerance_type,
											overcompletion_tolerance_value,
											inventory_carry_penalty,
											operation_slack_penalty,
											revision,
											style_item_flag,
											style_item_number,
											version_start_date,
											version_revision_code,
											version_label,
											start_upon_milestone_code,
											sales_product_sub_type,
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
											attribute21,
											attribute22,
											attribute23,
											attribute24,
											attribute25,
											attribute26,
											attribute27,
											attribute28,
											attribute29,
											attribute30,
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
											attribute_date1,
											attribute_date2,
											attribute_date3,
											attribute_date4,
											attribute_date5,
											attribute_timestamp1,
											attribute_timestamp2,
											attribute_timestamp3,
											attribute_timestamp4,
											attribute_timestamp5,
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
											global_attribute_number1,
											global_attribute_number2,
											global_attribute_number3,
											global_attribute_number4,
											global_attribute_number5,
											global_attribute_date1,
											global_attribute_date2,
											global_attribute_date3,
											global_attribute_date4,
											global_attribute_date5,
											prc_bu_name,
											force_purchase_lead_time_flag,
											replacement_type,
											buyer_email_address,
											default_expenditure_type,
											hard_pegging_level,
											comn_supply_prj_demand_flag,
											enable_iot_flag,
											packaging_string,
											create_supply_after_date,
											create_fixed_asset,
											under_compl_tolerance_type,
											under_compl_tolerance_value,
											repair_transaction_name,
											new_primary_uom_name,
											new_secondary_uom_name,
											file_name,
											error_message,
											import_status,
											source_system
                                            FROM XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG
											where import_status = '''
                             || 'ERROR'
                             || '''
											and execution_id  =  '''
                             || gv_execution_id
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
                    p_file_path         => replace(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                    p_file_name         => lv_batch_id
                                   || '_'
                                   || gv_oci_file_name_item,
                    p_attribute1        => lv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error exporting data to CSV for  XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
            END;

        END;

        BEGIN
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_pdh_c027_egp_item_categories_stg
                WHERE
                    execution_id = gv_execution_id;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('No Data Found');
            END;

            BEGIN
                dbms_cloud.export_data(
                    credential_name => gv_credential_name,
                    file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                     || '/'
                                     || lv_batch_id
                                     || gv_oci_file_name_item_categories,
                    format          =>
                            JSON_OBJECT(
                                'type' VALUE 'csv',
                                'header' VALUE TRUE,
                                'maxfilesize' VALUE '629145600'
                            ),
                    query           => 'SELECT 
                                          BATCH_ID ,
										  BATCH_NUMBER,
										  ITEM_NUMBER ,
										  ORGANIZATION_CODE,
										  CATEGORY_SET_NAME,
										  CATEGORY_NAME,
										  CATEGORY_CODE,
										  OLD_CATEGORY_NAME,
										  OLD_CATEGORY_CODE,
										  SOURCE_SYSTEM_CODE,
										  SOURCE_SYSTEM_REFERENCE,
										  START_DATE,
										  END_DATE,
										  file_name,
										  error_message,
										  import_status,
										  source_system
                                          FROM XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG
										  where import_status = '''
                             || 'ERROR'
                             || '''
										  and execution_id  =  '''
                             || gv_execution_id
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
                                   || gv_oci_file_name_item_categories,
                    p_attribute1        => lv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error exporting data to CSV for  XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
            END;

        END;

--table 3

        BEGIN
            BEGIN
                SELECT DISTINCT
                    batch_id
                INTO lv_batch_id
                FROM
                    xxcnv_pdh_c027_ego_item_eff_stg
                WHERE
                    execution_id = gv_execution_id;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('No Data Found');
            END;

            BEGIN
                dbms_cloud.export_data(
                    credential_name => gv_credential_name,
                    file_uri_list   => replace(gv_oci_file_path, gv_source_folder, gv_recon_folder)
                                     || '/'
                                     || lv_batch_id
                                     || gv_oci_file_name_item_effs,
                    format          =>
                            JSON_OBJECT(
                                'type' VALUE 'csv',
                                'header' VALUE TRUE,
                                'maxfilesize' VALUE '629145600'
                            ),
                    query           => '  SELECT 
										  transaction_type		
										  ,batch_id				
										  ,batch_number			
										  ,item_number			
										  ,organization_code		
										  ,source_system_code		
										  ,source_system_reference	
										  ,context_code			
										  ,attribute_char1			
										  ,attribute_char2			
										  ,attribute_char3			
										  ,attribute_char4			
										  ,attribute_char5			
										  ,attribute_char6			
										  ,attribute_char7			
										  ,attribute_char8			
										  ,attribute_char9			
										  ,attribute_char10		
										  ,attribute_char11		
										  ,attribute_char12		
										  ,attribute_char13		
										  ,attribute_char14		
										  ,attribute_char15		
										  ,attribute_char16
										  ,file_name
										  ,error_message
										  ,import_status
										  ,source_system
                                          ,FROM xxcnv_pdh_c027_ego_item_eff_stg
										  where import_status = '''
                             || 'ERROR'
                             || '''
										  and execution_id  =  '''
                             || gv_execution_id
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
                                   || gv_oci_file_name_item_effs,
                    p_attribute1        => lv_batch_id,
                    p_attribute2        => NULL,
                    p_process_reference => NULL
                );

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('Error exporting data to CSV for xxcnv_pdh_c027_ego_item_eff_stg batch_id '
                                         || lv_batch_id
                                         || ': '
                                         || sqlerrm);
            END;

        END;

    END create_recon_report_prc;

END xxcnv_pdh_c027_item_conversion_pkg;