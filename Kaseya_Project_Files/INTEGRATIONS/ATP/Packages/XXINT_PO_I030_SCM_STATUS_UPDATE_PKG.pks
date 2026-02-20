create or replace PACKAGE       XXINT.XXINT_PO_I030_SCM_STATUS_UPDATE_PKG AS
/********************************************************************************************
	NAME              :     XXINT.XXINT_PO_I030_SCM_STATUS_UPDATE_PKG SPEC.
    PURPOSE           :     SPEC Of procedures for managing status update of PO_Header
                            & PO_Lines tables.
    Change Histoy.
    Developer        Date         Version     Comments and changes made
    -------------   ------       ----------  ------------------------------
    Harish.V        13-04-2025      1.0         Initial Development.
   *******************************************************************************************/
    PROCEDURE update_po_header_line_details_prc (
        p_erp_po_num      IN VARCHAR2,
        p_nspo_number     IN VARCHAR2,
        p_nspo_id         IN NUMBER,
        p_status          IN VARCHAR2,
        p_msg             IN VARCHAR2,
        p_last_updated_by IN VARCHAR2
    );

    PROCEDURE update_po_header_status_prc (
        p_erp_po_num IN VARCHAR2
    );

END xxint_po_i030_scm_status_update_pkg;