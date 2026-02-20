create or replace PACKAGE       xxcnv.XXCNV_AP_C012_SUPPLIER_ATTACHMENT_CONVERSION_PKG AS

	/**************************************************************
    NAME              :     XXCNV_AP_C012_SUPPLIER_ATTACHMENT_CONVERSION_PKG SPEC
    PURPOSE           :     SPEC Of Procedures 
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------      ------       ----------  -------------------------------------------
	-- 	Phanindra 	     03-Mar-2025  	    1.0         Initial Development
    **************************************************************/

PROCEDURE MAIN_PRC(p_rice_id in varchar2,
               p_execution_id in varchar2,
               p_boundary_system in varchar2,
               p_file_name in varchar2);

PROCEDURE IMPORT_DATA_FROM_OCI_TO_STG_PRC( p_loading_status  OUT 	VARCHAR2                            
									  );


PROCEDURE DATA_VALIDATIONS_PRC;

PROCEDURE CREATE_FBDI_FILE_PRC;

PROCEDURE CREATE_PROPERTIES_FILE_PRC;

PROCEDURE CREATE_RECON_REPORT_PRC;

END XXCNV_AP_C012_SUPPLIER_ATTACHMENT_CONVERSION_PKG;