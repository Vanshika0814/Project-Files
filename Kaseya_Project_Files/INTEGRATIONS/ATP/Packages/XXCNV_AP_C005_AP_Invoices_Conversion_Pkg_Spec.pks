create or replace PACKAGE       XXCNV.XXCNV_AP_C005_AP_INVOICES_CONVERSION_PKG   AS 

	/**************************************************************
    NAME              :     XXCNV.XXCNV_AP_C005_AP_INVOICES_CONVERSION_PKG
    PURPOSE           :     SPEC Of Procedures 
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -------------------------------------------
	-- Bhargavi.K	  24-Oct-2025  	    1.0         Initial Development
        -- Bhargavi.K     26-Jul-2025       1.1         Removed XXCNV. at line 29
    **************************************************************/
PROCEDURE MAIN_PRC(p_rice_id in varchar2,
               p_execution_id in varchar2,
               p_boundary_system in varchar2,
               p_file_name in varchar2);

PROCEDURE IMPORT_DATA_FROM_OCI_TO_STG_PRC( p_loading_status  OUT 	VARCHAR2                            
									  );


PROCEDURE data_validations_prc;

PROCEDURE COA_TARGET_SEGMENTS_HEADER_PRC;

PROCEDURE create_fbdi_file_prc;

PROCEDURE create_atp_validation_recon_report_prc;

END XXCNV_AP_C005_AP_INVOICES_CONVERSION_PKG;