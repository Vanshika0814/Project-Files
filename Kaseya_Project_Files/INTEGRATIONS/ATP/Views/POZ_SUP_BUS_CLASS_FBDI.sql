--------------------------------------------------------
--  DDL for View POZ_SUP_BUS_CLASS_FBDI
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."POZ_SUP_BUS_CLASS_FBDI" ("BATCH_ID", "IMPORT_ACTION", "SUPPLIER_NAME", "CLASSIFICATION", "CLASSIFICATION_NEW", "SUBCLASSIFICATION", "CERTIFYING_AGENCY", "CERTIFYING_AGENCY_NEW", "CREATE_CERTIFYING_AGENCY", "CERTIFICATE_NUMBER", "CERTIFICATE_NUMBER_NEW", "START_DATE", "EXPIRATION_DATE", "NOTES", "PROVIDED_BY_FIRST_NAME", "PROVIDED_BY_LAST_NAME", "PROVIDED_BY_E_MAIL", "CONFIRMED_ON") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  select
        batch_id,
        import_action,
        supplier_name,
        classification,
        classification_new,
        subclassification,
        certifying_agency,
        certifying_agency_new,
        create_certifying_agency,
        certificate_number,
        certificate_number_new,
        to_char(start_date, 'YYYY/MM/DD') start_date,
        expiration_date,
        notes,
        provided_by_first_name,
        provided_by_last_name,
        provided_by_e_mail,
        ''                                confirmed_on
    from
        xxsupcnv.poz_sup_bus_class_ns
    where
        nvl(error_flag, 'N') = 'N'
;
/