--------------------------------------------------------
--  DDL for Package Body XXINT_PO_I029_CONTRACTS_ID_UPD_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "XXINT"."XXINT_PO_I029_CONTRACTS_ID_UPD_PKG" AS

/* *************************************************************************************************************************/
/*  CHANGE HISTORY                                                                                                         */
/*  VERSION   WHO            CREATION DATE   CHANGE (INCLUDE BUG# IF APPLY)                                                */
/*  -------   -------------  ------------   ------------------------------------                                           */
/*  1.0       Pallavi R N   05-JULY-2025     Updating ID's to Contracts Staging table                                      */
/*	1.1       Pallavi R N   09-SEP-2025      Handling Currency code errors in validation 								   */
/***************************************************************************************************************************/



    PROCEDURE xxint_po_i029_id_upd_prc(
    IN_ITEM_REC_ORG_TBL    IN ITEM_REC_ORG_TBL,
    IN_ITEM_REC_CON_TBL    IN ITEM_REC_CON_TBL,
    IN_ITEM_REC_VENDOR_TBL IN ITEM_REC_VENDOR_TBL,
    IN_INSTANCE_ID            IN VARCHAR2,
    OUT_STATUS  OUT   VARCHAR2
) IS
v_err_msg VARCHAR2(200);

BEGIN
    -- Update org_id for matching orgname and batch
    FOR i IN 1 .. IN_ITEM_REC_ORG_TBL.COUNT LOOP
        UPDATE xxint.xxint_po_i029_contracts_tbl
           SET org_id = IN_ITEM_REC_ORG_TBL(i).orgid
         WHERE org_name = IN_ITEM_REC_ORG_TBL(i).orgname
           AND oic_batch_id = IN_INSTANCE_ID;
    END LOOP;

    -- Update contract_type_id for matching contracttypename and batch
    FOR i IN 1 .. IN_ITEM_REC_CON_TBL.COUNT LOOP
        UPDATE xxint.xxint_po_i029_contracts_tbl
           SET contract_type_id = IN_ITEM_REC_CON_TBL(i).contracttypeid
         WHERE contract_type_name = IN_ITEM_REC_CON_TBL(i).contracttypename
           AND oic_batch_id = IN_INSTANCE_ID;
    END LOOP;

    -- Update party_id for matching vendornumber and batch
    FOR i IN 1 .. IN_ITEM_REC_VENDOR_TBL.COUNT LOOP
        UPDATE xxint.xxint_po_i029_contracts_tbl
           SET party_id = IN_ITEM_REC_VENDOR_TBL(i).partyid
         WHERE VENDOR_NUMBER = IN_ITEM_REC_VENDOR_TBL(i).vendornumber
           AND oic_batch_id = IN_INSTANCE_ID;
    END LOOP;

    -- Set status and error_message if any id is not found (i.e., still NULL)
    UPDATE xxint.xxint_po_i029_contracts_tbl
       SET status = 'ERROR',
           error_message = 'One or more reference IDs (org_id, party_id, contract_type_id) not found'
     WHERE oic_batch_id = IN_INSTANCE_ID
       AND (org_id IS NULL OR party_id IS NULL OR contract_type_id IS NULL);
       
    UPDATE xxint.xxint_po_i029_contracts_tbl
       SET status = 'ERROR',
           error_message = 'Currency Code is not valid'
     WHERE oic_batch_id = IN_INSTANCE_ID
       AND party_id IS NOT NULL
  AND org_id IS NOT NULL
  AND contract_type_id IS NOT NULL
  AND currency_code IS NULL;

    DBMS_OUTPUT.PUT_LINE('Staging table updated for batch: ' || IN_INSTANCE_ID);

EXCEPTION
    WHEN OTHERS THEN
    v_err_msg := 'Exception: ' || SUBSTR(SQLERRM, 1, 200);
        UPDATE xxint.xxint_po_i029_contracts_tbl
           SET status = 'ERROR',
               error_message = v_err_msg
         WHERE oic_batch_id = IN_INSTANCE_ID;
         DBMS_OUTPUT.PUT_LINE('Error occurred: ' || v_err_msg);
		OUT_STATUS:='Error';

END xxint_po_i029_id_upd_prc;

END XXINT_PO_I029_CONTRACTS_ID_UPD_PKG;

/
