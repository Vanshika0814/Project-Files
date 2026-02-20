--------------------------------------------------------
--  DDL for View POZ_SUP_BUS_CLASS_ATTACHMENTS_V
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."POZ_SUP_BUS_CLASS_ATTACHMENTS_V" ("BATCH_ID", "IMPORT_ACTION", "NAME", "CLASSIFICATION", "SUBCLASSIFICATION", "CERTIFYING_AGENCY", "CERTIFICATE_NUMBER", "CATEGORY", "TYPE", "FILE_TEXT_URL", "FILE_ATTACHMENTS_ZIP", "DESCRIPTION") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  SELECT distinct
    '' batch_id,
    'CREATE' import_action,
    b.supplier_number name,
    a.classification,
   '' subclassification,
   '' certifying_agency,
 ''   certificate_number,
   'FROM_SUPPLIER' category,
   'FILE' type,
   replace(a.DIRECTORYPATH,'\','/')  file_text_url,
   'Netsuite_Vendor_Attachment.zip' file_attachments_zip,
    a.FILENAME description
FROM
    SUPPLIER_ATTACHMENTS_LIST a, poz_suppliers_ns b
WHERE
    a.lastbutonedirectory = b.supplier_number
;