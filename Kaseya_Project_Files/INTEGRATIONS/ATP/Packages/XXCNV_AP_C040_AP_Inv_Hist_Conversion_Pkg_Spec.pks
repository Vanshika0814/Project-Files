create or replace PACKAGE       XXCNV.XXCNV_AP_C040_AP_INV_HIST_CONVERSION_PKG   AS 

	/**************************************************************
    NAME              :     XXCNV.XXCNV_AP_C040_AP_INV_HIST_CONVERSION_PKG
    PURPOSE           :     SPEC Of Procedures 
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -------------------------------------------
	-- 	Bhargavi.K	  24-OCt-2024  	    1.0         Initial Development
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

END XXCNV_AP_C040_AP_INV_HIST_CONVERSION_PKG;