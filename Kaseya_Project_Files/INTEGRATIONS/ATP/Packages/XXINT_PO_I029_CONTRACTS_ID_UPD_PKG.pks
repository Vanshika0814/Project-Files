--------------------------------------------------------
--  File created - Monday-July-21-2025   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package XXINT_PO_I029_CONTRACTS_ID_UPD_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "XXINT"."XXINT_PO_I029_CONTRACTS_ID_UPD_PKG" AS

/* *************************************************************************************************************************/
/*  CHANGE HISTORY                                                                                                         */
/*  VERSION   WHO            CREATION DATE   CHANGE (INCLUDE BUG# IF APPLY)                                                */
/*  -------   -------------  ------------   ------------------------------------                                           */
/*  1.0       Pallavi R N   05-JULY-2025     Updating ID's to Contracts Staging table                                              */
/*																														   */
/***************************************************************************************************************************/


TYPE ITEM_REC_ORG IS RECORD (ORGID NUMBER,ORGNAME VARCHAR2(100));

TYPE ITEM_REC_ORG_TBL IS TABLE OF ITEM_REC_ORG;

TYPE ITEM_REC_CON IS RECORD (CONTRACTTYPEID NUMBER,CONTRACTTYPENAME VARCHAR2(100));

TYPE ITEM_REC_CON_TBL IS TABLE OF ITEM_REC_CON;

TYPE ITEM_REC_VENDOR IS RECORD (PARTYID NUMBER,VENDORNUMBER VARCHAR2(100));

TYPE ITEM_REC_VENDOR_TBL IS TABLE OF ITEM_REC_VENDOR;

     PROCEDURE xxint_po_i029_id_upd_prc(

        IN_ITEM_REC_ORG_TBL IN ITEM_REC_ORG_TBL,
		IN_ITEM_REC_CON_TBL IN ITEM_REC_CON_TBL,
		IN_ITEM_REC_VENDOR_TBL IN ITEM_REC_VENDOR_TBL,

		IN_INSTANCE_ID  IN VARCHAR2,

        OUT_STATUS  OUT   VARCHAR2

    );

END XXINT_PO_I029_CONTRACTS_ID_UPD_PKG;

/
