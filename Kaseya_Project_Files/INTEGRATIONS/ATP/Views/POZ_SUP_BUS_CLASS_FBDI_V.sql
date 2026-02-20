--------------------------------------------------------
--  DDL for View POZ_SUP_BUS_CLASS_FBDI_V
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."POZ_SUP_BUS_CLASS_FBDI_V" ("BATCH_ID", "IMPORT_ACTION", "SUPPLIER_NAME", "CLASSIFICATION", "CLASSIFICATION_NEW", "SUBCLASSIFICATION", "CERTIFYING_AGENCY", "CERTIFYING_AGENCY_NEW", "CREATE_CERTIFYING_AGENCY", "CERTIFICATE_NUMBER", "CERTIFICATE_NUMBER_NEW", "START_DATE", "EXPIRATION_DATE", "NOTES", "PROVIDED_BY_FIRST_NAME", "PROVIDED_BY_LAST_NAME", "PROVIDED_BY_E_MAIL", "CONFIRMED_ON") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  select
        ''       batch_id,
        'CREATE' import_action,
        a.supplier_name,
        a.classification,
        ''       classification_new,
        ''       subclassification,
        ''       certifying_agency,
        ''       certifying_agency_new,
        ''       create_certifying_agency,
        ''       certificate_number,
        ''       certificate_number_new,
        a.start_date,
        ''       expiration_date,
        a.notes,
        a.provided_by_first_name,
        a.provided_by_last_name,
        a.provided_by_e_mail,
        a.confirmed_on
    from
        poz_sup_bus_class_fbdi a
    union
    select
        ''       batch_id,
        'CREATE' import_action,
        a.supplier_name,
        b.classification,
        ''       classification_new,
        ''       subclassification,
        ''       certifying_agency,
        ''       certifying_agency_new,
        ''       create_certifying_agency,
        ''       certificate_number,
        ''       certificate_number_new,
        a.start_date,
        ''       expiration_date,
        a.notes,
        a.provided_by_first_name,
        a.provided_by_last_name,
        a.provided_by_e_mail,
        a.confirmed_on
    from
        poz_sup_bus_class_fbdi    a,
        supplier_attachments_list b,
        poz_suppliers_ns          c
    where
            c.supplier_name = a.supplier_name
        and b.lastbutonedirectory = c.supplier_number
;
/