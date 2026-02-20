/******************************************************************************************
	PURPOSE : Create schema and GRANT Privileges script to xxcnv_run user.
    Change History																	    
    Developer        Date         Version     Comments and changes made				    
    -------------   ------       ----------  ----------------------------------------------
    Phanindra        25-06-2025      1.0         Initial Development    
    Satya            29-06-2025      1.1         Added select on stage tables 
	Satya            02-08-2025      1.2         Adding the select,insert,update,delete on staging tables
   ****************************************************************************************/
GRANT select,insert,update,delete ON xxcnv.xxcnv_cmn_conversion_execution to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_cmn_conversion_metadata to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_cmn_conversion_execution_details to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_gl_le_bu_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_fa_location_segment_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_fa_asset_category_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_ap_payment_terms_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_ap_payment_method_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_gl_currency_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_gl_ledger_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_gl_cvr_violation_tbl to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_ap_distribution_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_ap_supplier_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_ap_c008_contract_supplier_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_item_type_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_item_template_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_ap_c012_sup_bus_class_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_supplier_branch_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_contract_type_name_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_c008_procurement_contracts_orgid_table to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_c008_procurement_contracts_contracttype_id_table to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_c008_procurement_contracts_party_id_table to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_item_list_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_item_category_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_reqbu_shiporg_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_employee_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_c009_po_lines_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_c009_po_headers_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_receipts_quantity_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_ap_tax_details_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_po_ship_to_location_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxcnv.xxcnv_ap_c039_invoiceattachments_mapping to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_TEMP_COA_COMB_DATA to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_COA_NSERP_DATA to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_KASEYA_NS_ACCTCC to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_CONCUR_VENDOR_VALUES_REF to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_KASEYA_NS_FUTURE3 to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_KASEYA_NS_FUTURE2 to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_KASEYA_NS_FUTURE1 to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_KASEYA_NS_INTERCOMPANY to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_KASEYA_NS_LOCATION to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_KASEYA_NS_PRODUCTLINE to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_KASEYA_NS_ACCOUNT to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_KASEYA_NS_COSTCENTER to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_KASEYA_NS_DIVISON to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.XXMAP_GL_E001_KASEYA_NS_COMPANY to xxcnv_run;
GRANT select on sys.dba_scheduler_jobs to xxcnv_run;
GRANT select on sys.dba_scheduler_job_run_details to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C003_POZ_SUPPLIERS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C003_POZ_SUPPLIER_ADDRESSES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C003_POZ_SUPPLIER_SITES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C003_POZ_SUP_BUS_CLASS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C003_POZ_SUP_CONTACTS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C003_POZ_SUP_CONT_ADDR_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C003_POZ_SUP_SITE_ASSIGN_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C004_IBY_TEMP_EXT_BANK_ACCTS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C004_IBY_TEMP_EXT_PAYEES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C004_IBY_TEMP_PMT_INSTR_USES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C005_AP_INVOICES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C005_AP_INVOICE_LINES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C006_POZ_SUPPLIERS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C006_POZ_SUPPLIER_ADDRESSES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C006_POZ_SUPPLIER_SITES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C006_POZ_SUP_SITE_ASSIGN_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C012_SUPPLIER_BUS_CLASS_ATTACHMENTS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C040_AP_INVOICES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_AP_C040_AP_INVOICE_LINES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_FA_C013_FA_MASSADD_DIST_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_FA_C013_FA_MASSADD_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_GL_C001_GL_INTERFACE_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_GL_C002_GL_INTERFACE_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_GL_C002_PA_GL_INTERFACE_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_GL_C008_GL_INTERFACE_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_GL_C009_GL_INTERFACE_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_GL_C010_GL_INTERFACE_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_GL_C026_GL_INTERFACE_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_PDH_C027_EGO_ITEM_EFF_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_PO_C007_PO_DISTRIBUTIONS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_PO_C007_PO_HEADERS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_PO_C007_PO_LINES_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_PO_C007_PO_LINE_LOCATIONS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_PO_C008_CONTRACTS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_PO_C009_PO_RECEIPTS_HEADERS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_STG to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.xxcnv_po_category_mapping to xxcnv_run;
GRANT select,insert,update,delete ON XXCNV.XXCNV_PO_RECEIPTS_ITEM_LIST_MAPPING to xxcnv_run;
GRANT select,insert,update,delete ON xxmap.xxmap_gl_e001_exclude_accounts to xxcnv_run;
GRANT execute on xxcnv.xxcnv_ap_c004_load_sup_branch_mapping_prc to xxcnv_run;
GRANT select ON XXCNV.XXCNV_PO_C008_CONTRACT_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C003_POZ_SUPPLIERS_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C003_POZ_SUPPLIER_ADDRESSES_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C003_POZ_SUPPLIER_SITES_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C003_POZ_SUP_CONTACTS_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C003_POZ_SUP_SITE_ASSIGN_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C003_POZ_SUP_BUS_CLASS_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C003_POZ_SUP_CONT_ADDR_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C012_SUPPLIER_BUS_CLASS_ATTACHMENTS_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_PO_C009_PO_RECEIPTS_HEADERS_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_PO_C009_PO_RECEIPTS_TRANSACTIONS_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_PO_RECEIPTS_QUANTITY_MAPPING_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C005_AP_INVOICES_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C005_AP_INVOICES_LINES_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C006_POZ_SUPPLIERS_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C006_POZ_SUPPLIER_ADDRESSES_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C006_POZ_SUPPLIER_SITES_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C006_POZ_SUP_SITE_ASSIGN_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_PDH_C027_EGP_SYSTEM_ITEMS_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_PDH_C027_EGP_ITEM_CATEGORIES_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_PDH_C027_EGO_ITEM_EFF_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_FA_C013_FA_MASSADD_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_GL_C002_PA_GL_INTERFACE_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_GL_C001_GL_INTERFACE_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_GL_C013_GL_INTERFACE_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C040_AP_INVOICES_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_GL_C002_GL_INTERFACE_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_GL_C026_GL_INTERFACE_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_GL_C009_GL_INTERFACE_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_GL_C008_GL_INTERFACE_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_GL_C003_PA_GL_INTERFACE_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_GL_C006_PA_GL_INTERFACE_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_SUPPLIER_BRANCH_MAPPING_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C004_IBY_TEMP_EXT_PAYEES_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C004_IBY_TEMP_EXT_BANK_ACCTS_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_AP_C004_IBY_TEMP_PMT_INSTR_USES_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_GL_C010_GL_INTERFACE_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_PO_C007_PO_HEADERS_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_PO_C007_PO_LINES_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_PO_C007_PO_LINE_LOCATIONS_EXT to xxcnv_run;
GRANT select ON XXCNV.XXCNV_PO_C007_PO_DISTRIBUTIONS_EXT to xxcnv_run;


/
BEGIN
    FOR t IN (
        SELECT
            owner,
            table_name
        FROM
            all_tables
        WHERE
            owner IN ( 'XXCNV','XXMAP' )
    ) LOOP
        EXECUTE IMMEDIATE 'GRANT SELECT ON '
                          || t.owner
                          || '.'
                          || t.table_name
                          || ' TO XXCNV_RUN';
    END LOOP;
END;
/
