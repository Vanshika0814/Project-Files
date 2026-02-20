create or replace PACKAGE XXCNV.XXCNV_PO_C008_PROCUREMENT_CONTRACTS_CONV_ID_UPD_PKG
AS

/* *************************************************************************************************************************/
/*  CHANGE HISTORY                                                                                                         */
/*  VERSION   WHO            CREATION DATE   CHANGE (INCLUDE BUG# IF APPLY)                                                */
/*  -------   -------------  ------------   ------------------------------------                                           */
/*  1.0       Pallavi R N   06-MAY-2025     Updating ID's to conversion table  
    1.1       Bhargavi.K    26-Jul-2025     Removed XXCNV. at line 15                                            */
/*																														   */
/***************************************************************************************************************************/

PROCEDURE xxcnv_po_conv_id_upd_prc(po_prc_response OUT VARCHAR2);

END XXCNV_PO_C008_PROCUREMENT_CONTRACTS_CONV_ID_UPD_PKG;