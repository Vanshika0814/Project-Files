create or replace PACKAGE BODY       XXINT.XXINT_PO_I030_SCM_STATUS_UPDATE_PKG AS
/********************************************************************************************
	NAME              :     XXINT.XXINT_PO_I030_SCM_STATUS_UPDATE_PKG body.
    PURPOSE           :     This package is the detailed body of all the procedures.
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
    ) IS
    BEGIN
    -- UPDATE PO_HEADER_TBL
        UPDATE xxint_po_i030_po_header_tbl
        SET
            ns_po_num = p_nspo_number,
            ns_po_id =
                CASE
                    WHEN p_nspo_id IS NOT NULL THEN
                        p_nspo_id
                    ELSE
                        NULL
                END,
            status = p_status,
            message =
                CASE
                    WHEN p_msg IS NOT NULL THEN
                        p_msg
                    ELSE
                        message
                END,
            last_updated_by = p_last_updated_by,
            last_update_date = sysdate
        WHERE
            erp_po_num = p_erp_po_num;


    -- UPDATE PO_LINE_TBL
        UPDATE xxint_po_i030_po_lines_tbl
        SET
            status = p_status,
            ns_po_id =
                CASE
                    WHEN p_nspo_id IS NOT NULL THEN
                        p_nspo_id
                    ELSE
                        NULL
                END,
            message =
                CASE
                    WHEN p_msg IS NOT NULL THEN
                        p_msg
                    ELSE
                        message
                END,
            last_updated_by = p_last_updated_by,
            last_update_date = sysdate
        WHERE
            erp_po_num = p_erp_po_num;

    END update_po_header_line_details_prc;

    PROCEDURE update_po_header_status_prc (
        p_erp_po_num IN VARCHAR2
    ) IS
    BEGIN
        UPDATE xxint.xxint_po_i030_po_header_tbl
        SET
            status = (
                CASE
                    WHEN EXISTS (
                        SELECT
                            1
                        FROM
                            xxint.xxint_po_i030_po_lines_tbl
                        WHERE
                                status = 'ERROR'
                            AND erp_po_num = p_erp_po_num
                    ) THEN
                        'ERROR'
                    ELSE
                        'SUCCESS'
                END
            ),
            message = (
                CASE
                    WHEN EXISTS (
                        SELECT
                            1
                        FROM
                            xxint.xxint_po_i030_po_lines_tbl
                        WHERE
                                status = 'ERROR'
                            AND erp_po_num = p_erp_po_num
                    ) THEN
                        ''
                    ELSE
                        'PROCESSED'
                END
            )
        WHERE
            erp_po_num = p_erp_po_num;

    END update_po_header_status_prc;

END xxint_po_i030_scm_status_update_pkg;