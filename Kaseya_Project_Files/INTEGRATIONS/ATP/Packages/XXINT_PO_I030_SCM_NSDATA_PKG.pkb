create or replace PACKAGE BODY       XXINT.XXINT_PO_I030_SCM_NSDATA_PKG AS
/********************************************************************************************
	NAME              :     XXINT.XXINT_PO_I030_SCM_NSDATA_PKG body
    PURPOSE           :     This package is the detailed body of all the procedures.
    Change Histoy.    
    Developer        Date         Version     Comments and changes made
    -------------   ------       ----------  ------------------------------
    Harish.V        13-04-2025      1.0         Initial Development.
    Harish.V        22-07-2025      1.1         Changed the package parameters from IN to P.
   *******************************************************************************************/

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
    ) AS
    BEGIN
        p_error_msg := '';
        BEGIN
            SELECT
                netsuite_id
            INTO p_supplier_ns_id
            FROM
                xxmap.xxmap_nsoc_supplier_master_data
            WHERE
                erp_supplier_name = p_supplier_erp_name;

            p_status := 'SUCCESS';
        EXCEPTION
            WHEN OTHERS THEN
                p_status := 'ERROR';
                p_error_msg := 'supplier:' || p_supplier_erp_name;
        END;

        BEGIN
            SELECT
                netsuite_id
            INTO p_soldto_le_id
            FROM
                xxmap.xxmap_nsoc_legal_entity_master_data
            WHERE
                erp_legalentity_name = p_soldto_le_name;

            p_status := 'SUCCESS';
        EXCEPTION
            WHEN OTHERS THEN
                p_status := 'ERROR';
                p_error_msg := p_error_msg
                               || ',Sold-To Legal Entity:'
                               || p_soldto_le_name;
        END;

        BEGIN
            SELECT
                netsuite_id
            INTO p_location_ns_id
            FROM
                xxmap.xxmap_nsoc_location_master_data
            WHERE
                erp_location_name = p_location_name;

            p_status := 'SUCCESS';
        EXCEPTION
            WHEN OTHERS THEN
                p_status := 'ERROR';
                p_error_msg := p_error_msg
                               || ',Location:'
                               || p_location_name;
        END;

        BEGIN
            SELECT
                netsuite_id
            INTO p_by_emp_ns_id
            FROM
                xxmap.xxmap_nsoc_employee_master_data
            WHERE
                erp_employee_num = p_by_emp_num;

            p_status := 'SUCCESS';
        EXCEPTION
            WHEN OTHERS THEN
                p_status := 'ERROR';
                p_error_msg := p_error_msg
                               || ',EMP_NUM:'
                               || p_by_emp_num;
        END;

        BEGIN
            SELECT
                netsuite_id
            INTO p_req_emp_ns_id
            FROM
                xxmap.xxmap_nsoc_employee_master_data
            WHERE
                erp_employee_num = p_req_emp_num;

            p_status := 'SUCCESS';
        EXCEPTION
            WHEN OTHERS THEN
                p_status := 'ERROR';
                p_error_msg := p_error_msg
                               || ',EMP_NUM:'
                               || p_req_emp_num;
        END;

        BEGIN
            SELECT
                netsuite_id
            INTO p_currency_ns_id
            FROM
                xxmap.xxmap_nsoc_currency_master_data
            WHERE
                erp_currency_code = p_currency_code;

            p_status := 'SUCCESS';
        EXCEPTION
            WHEN OTHERS THEN
                p_status := 'ERROR';
                p_error_msg := p_error_msg
                               || ',Currency:'
                               || p_currency_code;
        END;

        IF p_error_msg IS NOT NULL THEN
            p_error_msg := 'NETSUITE_ID not present for the ==>' || p_error_msg;
            p_status := 'ERROR';
            UPDATE xxint.xxint_po_i030_po_header_tbl
            SET
                status = 'ERROR',
                message = p_error_msg
            WHERE
                erp_po_num = p_erp_number;

        END IF;

    END get_header_master_data_prc;

    PROCEDURE get_lines_master_data_prc (
        p_item_rec_tbl IN item_rec_tbl,
        p_status       OUT VARCHAR2,
        p_error_msg    OUT VARCHAR2
    ) AS

   --Declare variable 
        netsuite_id VARCHAR2(50);
    BEGIN
        p_status := 'SUCCESS';
        p_error_msg := NULL;
        FOR i IN p_item_rec_tbl.first..p_item_rec_tbl.last LOOP
            BEGIN

            -- Check for netsuite_id in the xxint_po_i030_item_master table
                SELECT
                    netsuite_id
                INTO netsuite_id
                FROM
                    xxmap.xxmap_nsoc_item_master_data
                WHERE
                    item_num = p_item_rec_tbl(i).erp_item_num;

            -- Update the XXINT_PO_I030_PO_LINES_TBL table with netsuite_id and status
                UPDATE xxint_po_i030_po_lines_tbl
                SET
                    item_netsuite_id = netsuite_id,
                    status = 'NEW',
                    message = ''
                WHERE
                        erp_po_num = p_item_rec_tbl(i).erp_po_num
                    AND po_line_id = p_item_rec_tbl(i).po_line_id;

            EXCEPTION
                WHEN no_data_found THEN
                    p_status := 'ERROR';
                    p_error_msg := nvl(p_error_msg, '')
                                   || ' -NETSUITE_ID not present for the Item: '
                                   || p_item_rec_tbl(i).erp_item_num;

                    UPDATE xxint_po_i030_po_lines_tbl
                    SET
                        status = 'ERROR',
                        message = p_error_msg
                    WHERE
                            erp_po_num = p_item_rec_tbl(i).erp_po_num
                        AND po_line_id = p_item_rec_tbl(i).po_line_id;

                WHEN OTHERS THEN
                    p_status := 'ERROR';
                    p_error_msg := nvl(p_error_msg, '')
                                   || ' -An unexpected error occurred for the Item: '
                                   || p_item_rec_tbl(i).erp_item_num
                                   || ' - '
                                   || sqlerrm;

                    UPDATE xxint_po_i030_po_lines_tbl
                    SET
                        status = 'ERROR',
                        message = 'An unexpected error occurred for the Item: '
                                  || p_item_rec_tbl(i).erp_item_num
                                  || ' - '
                    WHERE
                            erp_po_num = p_item_rec_tbl(i).erp_po_num
                        AND po_line_id = p_item_rec_tbl(i).po_line_id;

            END;
        END LOOP;

        IF p_status = 'ERROR' THEN
            p_error_msg := 'Line details have the error';
        END IF;
    END get_lines_master_data_prc;

END xxint_po_i030_scm_nsdata_pkg;