create or replace PACKAGE       XXINT.XXINT_PO_I030_SCM_NSDATA_PKG AS
/******************************************************************************************
	  NAME              :     XXINT.XXINT_PO_I030_SCM_NSDATA_PKG SPEC
      PURPOSE           :     SPEC Of procedures for managing get the NS id's for the
							  corresponding Oracle data.
    Change History.
    Developer        Date         Version     Comments and changes made
    -------------   ------       ----------  ---------------------------
    Harish.V        13-04-2025      1.0         Initial Development.
    Harish.V        22-07-2025      1.1         Changed the package parameters from IN to P.
   ****************************************************************************************/
    TYPE item_rec IS RECORD (
            erp_po_num   VARCHAR2(50),
            po_line_id   VARCHAR2(50),
            erp_item_num VARCHAR2(50)
    );
    TYPE item_rec_tbl IS
        TABLE OF item_rec;
    PROCEDURE get_header_master_data_prc (
        p_erp_number        IN VARCHAR2,
        p_supplier_erp_name IN VARCHAR2,
        p_soldto_le_name    IN VARCHAR2,
        p_location_name     IN VARCHAR2,
        p_by_emp_num        IN VARCHAR2,
        p_req_emp_num       IN VARCHAR2,
        p_currency_code     IN VARCHAR2,
        p_supplier_ns_id    OUT VARCHAR2,
        p_soldto_le_id      OUT VARCHAR2,
        p_location_ns_id    OUT VARCHAR2,
        p_by_emp_ns_id      OUT VARCHAR2,
        p_req_emp_ns_id     OUT VARCHAR2,
        p_currency_ns_id    OUT VARCHAR2,
        p_status            OUT VARCHAR2,
        p_error_msg         OUT VARCHAR2
    );

    PROCEDURE get_lines_master_data_prc (
        p_item_rec_tbl IN item_rec_tbl,
        p_status       OUT VARCHAR2,
        p_error_msg    OUT VARCHAR2
    );

END xxint_po_i030_scm_nsdata_pkg;