CREATE OR REPLACE PACKAGE xxcnv.xxcnv_ap_c003_supplier_conversion_pkg IS
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

END xxcnv_ap_c003_supplier_conversion_pkg;