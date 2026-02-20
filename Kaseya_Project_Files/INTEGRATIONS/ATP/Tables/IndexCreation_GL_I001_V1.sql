--------------------------------------------------------
--  DDL for Index XXINT_GL_I001_JOURNALS_STG_N7
--------------------------------------------------------

CREATE INDEX "XXINT"."XXINT_GL_I001_JOURNALS_STG_N7" ON
    "XXINT"."XXINT_GL_I001_JOURNALS_STG" (
        "PARENT_INSTANCE_ID",
        "SOURCE_FILE_NAME"
    );