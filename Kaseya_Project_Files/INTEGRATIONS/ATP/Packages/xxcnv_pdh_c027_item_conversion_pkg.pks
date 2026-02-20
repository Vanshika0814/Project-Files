create or replace PACKAGE xxcnv.xxcnv_pdh_c027_item_conversion_pkg IS

	/**************************************************************
    NAME              :     xxcnv_pdh_c027_item_conversion_pkg SPEC
    PURPOSE           :     SPEC Of Procedures 
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -------------------------------------------
	-- 	Satya Pavani	  28-Aug-2024  	    1.0         Initial Development
    **************************************************************/

    PROCEDURE main_prc (
        p_rice_id         IN VARCHAR2,
        p_execution_id    IN VARCHAR2,
        p_boundary_system IN VARCHAR2,
        p_file_name       IN VARCHAR2
    );

    PROCEDURE import_data_from_oci_to_stg_prc (
        p_loading_status OUT VARCHAR2
    );

    PROCEDURE data_validations_prc;

    PROCEDURE create_fbdi_file_prc;

    PROCEDURE create_recon_report_prc;

END xxcnv_pdh_c027_item_conversion_pkg;