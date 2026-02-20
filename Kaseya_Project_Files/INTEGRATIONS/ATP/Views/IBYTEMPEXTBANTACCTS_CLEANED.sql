--------------------------------------------------------
--  DDL for View IBYTEMPPMTINSTRUSES_V
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."IBYTEMPPMTINSTRUSES_V" ("IMPORT_BATCH_IDENTIFIER", "PAYEE_IDENTIFIER", "PAYEE_BANK_ACCOUNT_IDENTIFIER", "PAYEE_BANK_ACCOUNT_ASSIGNMENT_IDENTIFIER", "PRIMARY_FLAG", "ACCOUNT_ASSIGNMENT_START_DATE", "ACCOUNT_ASSIGNMENT_END_DATE") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  SELECT distinct
    '' import_batch_identifier,
    a.PAYEE_IDENTIFIER payee_identifier,
     a.PAYEE_IDENTIFIER ||'001' payee_bank_account_identifier,
    a.PAYEE_IDENTIFIER || '002' payee_bank_account_assignment_identifier,
   'Y'  primary_flag,
   '' account_assignment_start_date,
   '' account_assignment_end_date
 FROM
   IBYTEMPEXTBANTACCTS_CLEANED a
;
/