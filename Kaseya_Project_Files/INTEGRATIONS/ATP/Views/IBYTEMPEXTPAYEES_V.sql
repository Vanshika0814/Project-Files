--------------------------------------------------------
--  DDL for View IBYTEMPEXTPAYEES_V
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."IBYTEMPEXTPAYEES_V" ("IMPORT_BATCH_IDENTIFIER", "PAYEE_IDENTIFIER", "BUSINESS_UNIT_NAME", "SUPPLIER_NUMBER", "SUPPLIER_SITE", "PAY_EACH_DOCUMENT_ALONE", "PAYMENT_METHOD_CODE", "DELIVERY_CHANNEL_CODE", "SETTLEMENT_PRIORITY", "REMIT_DELIVERY_METHOD", "REMIT_ADVICE_EMAIL", "REMIT_ADVICE_FAX", "BANK_INSTRUCTIONS_1", "BANK_INSTRUCTIONS_2", "BANK_INSTRUCTION_DETAILS", "PAYMENT_REASON_CODE", "PAYMENT_REASON_COMMENTS", "PAYMENT_MESSAGE1", "PAYMENT_MESSAGE2", "PAYMENT_MESSAGE3", "BANK_CHARGE_BEARER_CODE") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  SELECT distinct
    ''                  import_batch_identifier,
    b.PAYEE_IDENTIFIER               payee_identifier,
    b.business_unit_name,
    b.supplier_number       supplier_number,
    ''                  supplier_site,
    ''                  pay_each_document_alone,
    nvl(c.payment_method,'EFT') payment_method_code,
    ''                  delivery_channel_code,
    ''                  settlement_priority,
    ''                  remit_delivery_method,
    b.remit_advice_email,
    ''                  remit_advice_fax,
    ''                  bank_instructions_1,
    ''                  bank_instructions_2,
    ''                  bank_instruction_details,
    ''                  payment_reason_code,
    b.payment_reason_comments,
    ''                  payment_message1,
    ''                  payment_message2,
    ''                  payment_message3,
    ''                  bank_charge_bearer_code
FROM
    IBYTEMPEXTPAYEES_CLEANED b,
    stg_poz_suppliers    c
WHERE
    b.supplier_number = c.supplier_number
;
/