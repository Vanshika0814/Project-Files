--------------------------------------------------------
--  DDL for View IBYTEMPEXTBANKACCTS_V
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."IBYTEMPEXTBANKACCTS_V" ("IMPORT_BATCH_IDENTIFIER", "PAYEE_IDENTIFIER", "PAYEE_BANK_ACCOUNT_IDENTIFIER", "BANK_NAME", "BRANCH_NAME", "ACCOUNT_COUNTRY_CODE", "ACCOUNT_NAME", "ACCOUNT_NUMBER", "ACCOUNT_CURRENCY_CODE", "ALLOW_INTERNATIONAL_PAYMENTS", "ACCOUNT_START_DATE", "ACCOUNT_END_DATE", "IBAN", "CHECK_DIGITS", "ACCOUNT_ALTERNATE_NAME", "ACCOUNT_TYPE_CODE", "ACCOUNT_SUFFIX", "ACCOUNT_DESCRIPTION", "AGENCY_LOCATION_CODE", "EXCHANGE_RATE_AGREEMENT_NUMBER", "EXCHANGE_RATE_AGREEMENT_TYPE", "EXCHANGE_RATE", "SECONDARY_ACCOUNT_REFERENCE", "ATTRIBUTE_CATEGORY", "ATTRIBUTE_1", "ATTRIBUTE_2", "ATTRIBUTE_3", "ATTRIBUTE_4", "ATTRIBUTE_5", "ATTRIBUTE_6", "ATTRIBUTE_7", "ATTRIBUTE_8", "ATTRIBUTE_9", "ATTRIBUTE_10", "ATTRIBUTE_11", "ATTRIBUTE_12", "ATTRIBUTE_13", "ATTRIBUTE_14", "ATTRIBUTE_15") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  SELECT distinct
    '' import_batch_identifier,
    a.PAYEE_IDENTIFIER payee_identifier,
    a.PAYEE_IDENTIFIER ||'001' payee_bank_account_identifier,
    a.bank_name,
    a.branch_name,
    a.account_country_code,
    a.account_name,
    a.account_number,
    '' account_currency_code,
    '' allow_international_payments,
    '' account_start_date,
    '' account_end_date,
    a.iban,
    '' check_digits,
    '' account_alternate_name,
    '' account_type_code,
    a.account_suffix,
    '' account_description,
    '' agency_location_code,
    '' exchange_rate_agreement_number,
    '' exchange_rate_agreement_type,
    '' exchange_rate,
    '' secondary_account_reference,
    '' attribute_category,
    a.attribute_1,
     a.attribute_2,
    '' attribute_3,
    '' attribute_4,
    '' attribute_5,
    '' attribute_6,
    '' attribute_7,
    '' attribute_8,
    '' attribute_9,
    '' attribute_10,
    '' attribute_11,
    '' attribute_12,
    '' attribute_13,
    '' attribute_14,
    '' attribute_15
FROM    
   IBYTEMPEXTBANTACCTS_CLEANED a
--   where ACCOUNT_NUMBER like '%+%'
;
/