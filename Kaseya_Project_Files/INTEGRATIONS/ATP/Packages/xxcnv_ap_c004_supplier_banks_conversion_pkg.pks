create or replace PACKAGE xxcnv.xxcnv_ap_c004_supplier_banks_conversion_pkg IS

  /* TODO enter package declarations (types, exceptions, methods etc) here */ 
/**************************************************************
    NAME              :     SP_CONVERSION_PACKAGE_CNV SPEC
    PURPOSE           :     SPEC Of Procedures import_data_from_oci_to_stg_prc
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -------------------------------------------
	-- 	Priyanka Kadam  27-Mar-2025  	    1.0         Initial Development
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

    PROCEDURE create_atp_validation_recon_report_prc;

END xxcnv_ap_c004_supplier_banks_conversion_pkg;