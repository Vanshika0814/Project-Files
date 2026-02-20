create or replace PACKAGE 	xxcnv.xxcnv_gl_c002_gl_journals_reconcile_pkg IS

/*************************************************************************************
    NAME              :     GL_RECONCILE_PKG Spec
    PURPOSE           :     Package Spec.
	-- Modification History
	-- Developer				Date				Version     Comments and changes made
	-- -------------			----------			----------  ----------------------------
	-- Chandra Mouli Gupta		11-Aug-2025  	    1.0         Initial Development
	****************************************************************************************/
/*create procedure for truncating data from all 3 tables*/

PROCEDURE truncate_file_from_oci_prc(
p_status OUT VARCHAR2,
p_message OUT VARCHAR2
);

PROCEDURE import_source_file_from_oci_to_stg_prc (
    p_src_file_path IN VARCHAR2,
    p_src_file_name IN VARCHAR2,
    p_status OUT VARCHAR2,
    p_message OUT VARCHAR2
);

PROCEDURE import_transformed_file_from_oci_to_stg_prc (
    p_trans_file_path IN VARCHAR2,
    p_trans_file_name IN VARCHAR2,
    p_status OUT VARCHAR2,
    p_message OUT VARCHAR2
);

PROCEDURE import_reconciled_file_from_oci_to_stg_prc (
    p_recon_file_path IN varchar2,
    p_recon_file_name IN varchar2,
    p_status OUT VARCHAR2,
    p_message OUT VARCHAR2
);

PROCEDURE compare_source_transformed_recon_file_prc (
    p_oicInstanceId IN VARCHAR2,
    p_conversionId IN VARCHAR2,
    p_iterationNumber IN VARCHAR2,
    p_status OUT VARCHAR2,
    p_message OUT VARCHAR2
);

PROCEDURE compare_transformed_recon_file_prc(
    p_oicInstanceId IN VARCHAR2,
    p_conversionId IN VARCHAR2,
    p_iterationNumber IN VARCHAR2,
    p_status OUT VARCHAR2,
    p_message OUT VARCHAR2
);

END xxcnv_gl_c002_gl_journals_reconcile_pkg;