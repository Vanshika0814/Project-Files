--------------------------------------------------------
--  DDL for View POZ_SUPP_CONTACT_ADDRESSES_FBDI
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."POZ_SUPP_CONTACT_ADDRESSES_FBDI" ("BATCH_ID", "IMPORT_ACTION", "SUPPLIER_NAME", "ADDRESS_NAME", "FIRST_NAME", "LAST_NAME", "EMAIL") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  select distinct
        batch_id,
        import_action,
        supplier_name,
        address_name,
        first_name,
        last_name,
        email
    from
        xxsupcnv.poz_supp_contact_addresses_ns psca
    where
        nvl(error_flag, 'N') = 'N'
;
/