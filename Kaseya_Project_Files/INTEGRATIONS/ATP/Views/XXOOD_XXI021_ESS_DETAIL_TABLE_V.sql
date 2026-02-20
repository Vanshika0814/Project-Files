CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXOOD"."XXOOD_XXI021_ESS_DETAIL_TABLE_V" (
    "REQUEST_ID",
    "STATUS",
    "NAME",
    "PROCESS_START",
    "PROCESS_END",
    "JOB_TYPE",
    "JOB_CREATED_BY",
    "LAST_UPDATED_BY",
    "CREATION_DATE",
    "LAST_UPDATE_DATE",
    "UCM_DOCUMENT_ID",
    "PARENTJOBID"
) DEFAULT COLLATION "USING_NLS_COMP" AS
    SELECT
        request_id,
        status,
        name,
        process_start,
        process_end,
        job_type,
        job_created_by,
        last_updated_by,
        creation_date,
        last_update_date,
        ucm_document_id,
        parentjobid
    FROM
        xxood_xxi021_ess_bicc_detail_table
    WHERE
            job_type = 'ESS'
        AND trunc(last_update_date) BETWEEN trunc(sysdate) - 29 AND trunc(sysdate)
    ORDER BY
        CAST(request_id AS NUMBER);