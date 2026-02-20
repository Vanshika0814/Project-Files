ALTER TABLE xxmap.xxmap_gl_job_parameters_ref MODIFY (
    ledger_id VARCHAR2(200),
    ledger_name VARCHAR2(200),
    data_access_set_id VARCHAR2(200),
    ledger_category_code VARCHAR2(200),
    segment1 VARCHAR2(200)
);



ALTER TABLE xxmap.xxmap_gl_sources_ref MODIFY (
    source_id VARCHAR2(200),
    source_name VARCHAR2(200)
);