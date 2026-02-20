create or replace PACKAGE XXCNV.XXCNV_PO_C008_PROCUREMENT_CONTRACTS_CONVERSION_PKG IS
  /* TODO enter package declarations (types, exceptions, methods etc) here */ 
/**************************************************************
    NAME              :     XXCNV.XXCNV_PO_C008_PROCUREMENT_CONTRACTS_CONVERSION_PKG SPEC
    PURPOSE           :     SPEC Of Procedures IMPORT_DATA_FROM_OCI_TO_EXT_TO_STG
 -- Modification History
 -- Developer          Date         Version     Comments and changes made
 -- -------------   ------       ----------  -------------------------------------------
 --  Bhargavi.K  28-May-2025      1.0         Initial Development
 --  Bhargavi.K  26-Jul-2025      1.1         Removed XXCNV. at line 27
    **************************************************************/


PROCEDURE MAIN_PRC( p_RICE_ID              IN    VARCHAR2,
                     p_execution_id   IN       VARCHAR2,
                     p_boundary_system      IN    VARCHAR2,
            p_file_name       IN    VARCHAR2);

PROCEDURE IMPORT_DATA_FROM_OCI_TO_STG_PRC( p_loading_status  OUT  VARCHAR2);

PROCEDURE DATA_VALIDATIONS_PRC;

PROCEDURE CREATE_FBDI_FILE_PRC;

PROCEDURE CREATE_RECON_REPORT_PRC;

END XXCNV_PO_C008_PROCUREMENT_CONTRACTS_CONVERSION_PKG;