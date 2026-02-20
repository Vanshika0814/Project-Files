create or replace PACKAGE  xxcnv.xxcnv_gl_c001_gl_summary_conversion_pkg IS

	/**************************************************************
    NAME              :     GL_CONVERSION_PKG SPEC
    PURPOSE           :     SPEC Of Procedures
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -------------------------------------------
	-- 	Priyanka Kadam	  27-Feb-2024  	    1.0         Initial Development
    --	Satya Pavani      01-Sep-2025       1.1       LTCI-7741 - Period name change as the scope for PROD changed and cvr_proc
    ************************************************************/

PROCEDURE main_prc ( p_rice_id 	            IN  		VARCHAR2,
                 p_execution_id 		IN  	    VARCHAR2,
                 p_boundary_system      IN  		VARCHAR2,
			     p_file_name 		    IN  		VARCHAR2);

PROCEDURE import_data_from_oci_to_stg_prc ( p_loading_status  OUT 	VARCHAR2 
									  );

PROCEDURE data_validations_prc;

PROCEDURE create_fbdi_file_prc;

PROCEDURE coa_target_segments_prc;

PROCEDURE load_balancing_line_prc;

PROCEDURE cvr_rule_check_prc;  -- v1.1 Added the cvr procedure

PROCEDURE  create_atp_validation_recon_report_prc;

PROCEDURE create_properties_file_prc;

END xxcnv_gl_c001_gl_summary_conversion_pkg;