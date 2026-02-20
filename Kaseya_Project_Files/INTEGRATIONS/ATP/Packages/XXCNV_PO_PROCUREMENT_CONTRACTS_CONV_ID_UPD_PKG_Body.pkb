create or replace PACKAGE BODY XXCNV.XXCNV_PO_C008_PROCUREMENT_CONTRACTS_CONV_ID_UPD_PKG
AS

/* *************************************************************************************************************************/
/*  CHANGE HISTORY                                                                                                         */
/*  VERSION   WHO            CREATION DATE   CHANGE (INCLUDE BUG# IF APPLY)                                                */
/*  -------   -------------  ------------   ------------------------------------                                           */
/*  1.0       Pallavi R N   06-MAY-2025     Updating ID's to conversion table 
    1.1       Bhargavi.K    26-Jul-2025     Removed XXCNV. at line 102                                             */
/*																														   */
/***************************************************************************************************************************/

PROCEDURE xxcnv_po_conv_id_upd_prc(po_prc_response OUT VARCHAR2)
IS
lv_err_msg VARCHAR2(4000);
lv_prc_response VARCHAR2(100):=NULL;
ln_err_cnt NUMBER:=0;
ln_org_upd_cnt NUMBER:=0;
ln_type_upd_cnt NUMBER:=0;
ln_party_upd_cnt NUMBER:=0;
BEGIN

    BEGIN
	     UPDATE XXCNV_PO_C008_Contracts_stg con
         SET orgid = (SELECT org.orgid 
                      FROM xxcnv_po_c008_procurement_contracts_orgid_table org
                      WHERE org.orgname = con.orgname)
         WHERE EXISTS (SELECT 1 
                      FROM xxcnv_po_c008_procurement_contracts_orgid_table org1
                      WHERE org1.orgname = con.orgname)
         AND orgid IS NULL;

		 ln_org_upd_cnt := SQL%ROWCOUNT;

	EXCEPTION
       WHEN OTHERS THEN
       lv_err_msg :='Exception at updating orgid block : '||SQLERRM ||' - Error Place - ' || DBMS_UTILITY.format_error_backtrace;
	   dbms_output.put_line(lv_err_msg);
       ln_err_cnt := ln_err_cnt+1;
       ROLLBACK;
	END;

   BEGIN
	     UPDATE XXCNV_PO_C008_Contracts_stg con
         SET contracttypeid = (SELECT contract.contracttypeid 
                               FROM xxcnv_po_c008_procurement_contracts_contracttype_id_table contract
                               WHERE contract.contracttypename = con.contracttypename)
         WHERE EXISTS (SELECT 1 
                       FROM xxcnv_po_c008_procurement_contracts_contracttype_id_table contract1
                       WHERE contract1.contracttypename = con.contracttypename)
         AND contracttypeid IS NULL;

		 ln_type_upd_cnt := SQL%ROWCOUNT;

	EXCEPTION
       WHEN OTHERS THEN
       lv_err_msg :='Exception at updating contracttypeid block : '||SQLERRM ||' - Error Place - ' || DBMS_UTILITY.format_error_backtrace;
	   dbms_output.put_line(lv_err_msg);
       ln_err_cnt := ln_err_cnt+1;
       ROLLBACK;
	END;	

	BEGIN
	     UPDATE XXCNV_PO_C008_Contracts_stg con
         SET partyid = (SELECT party.partyid 
                        FROM xxcnv_po_c008_procurement_contracts_party_id_table party
                        WHERE party.partyname = con.partyname)
         WHERE EXISTS (SELECT 1 
                       FROM xxcnv_po_c008_procurement_contracts_party_id_table party1
                       WHERE party1.partyname = con.partyname)
         AND partyid IS NULL;

		 ln_party_upd_cnt := SQL%ROWCOUNT;

	EXCEPTION
       WHEN OTHERS THEN
       lv_err_msg :='Exception at updating partyid block : '||SQLERRM ||' - Error Place - ' || DBMS_UTILITY.format_error_backtrace;
	   dbms_output.put_line(lv_err_msg);
       ln_err_cnt := ln_err_cnt+1;
       ROLLBACK;
	END;

	IF ln_err_cnt > 0 THEN
       lv_prc_response := 'Error';
       ROLLBACK;
    ELSE 
       lv_prc_response := 'Success';
       COMMIT;
    END IF;	

	dbms_output.put_line('No. of records updated orgupdatedcount : '||ln_org_upd_cnt|| ' ** typeupdatedcount : '||ln_type_upd_cnt||' ** partyupdatedcount : '||ln_party_upd_cnt);
	po_prc_response := lv_prc_response;

EXCEPTION
   WHEN OTHERS THEN
    lv_err_msg :='Exception at xxcnv_po_conv_id_upd_prc PRC: '||SQLERRM ||' - Error Place - ' || DBMS_UTILITY.format_error_backtrace;
	dbms_output.put_line(lv_err_msg);
    po_prc_response:='Error';
    ROLLBACK;
END xxcnv_po_conv_id_upd_prc;

END XXCNV_PO_C008_PROCUREMENT_CONTRACTS_CONV_ID_UPD_PKG;