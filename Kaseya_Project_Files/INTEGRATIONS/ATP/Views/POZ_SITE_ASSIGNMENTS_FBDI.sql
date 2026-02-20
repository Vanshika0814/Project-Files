--------------------------------------------------------
--  DDL for View POZ_SITE_ASSIGNMENTS_FBDI
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."POZ_SITE_ASSIGNMENTS_FBDI" ("BATCH_ID", "IMPORT_ACTION", "SUPPLIER_NAME", "SUPPLIER_SITE", "PROCUREMENT_BU", "CLIENT_BU", "BILL_TO_BU", "SHIP_TO_LOCATION", "BILL_TO_LOCATION", "USE_WITHHOLDING_TAX", "WITHHOLDING_TAX_GROUP", "LIABILITY_DISTRIBUTION", "PREPAYMENT_DISTRIBUTION", "BILLS_PAYABLE_DISTRIBUTION", "DISTRIBUTION_SET", "INACTIVE_DATE") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  select distinct
        batch_id,
        import_action,
        supplier_name,
        supplier_site,
        procurement_bu,
        client_bu,
        bill_to_bu,
        ship_to_location,
        bill_to_location,
        use_withholding_tax,
        withholding_tax_group,
        liability_distribution,
        prepayment_distribution,
        bills_payable_distribution,
        distribution_set,
        '' inactive_date
    from
        xxsupcnv.poz_site_assignments_ns
    where
        nvl(error_flag, 'N') = 'N'
;
/