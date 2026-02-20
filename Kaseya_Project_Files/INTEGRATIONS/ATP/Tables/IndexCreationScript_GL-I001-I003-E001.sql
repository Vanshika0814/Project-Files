--------------------------------------------------------
--  DDL for Index XXMAP_GL_E001_KASEYA_NS_SEGMENTS_N1
--------------------------------------------------------

  CREATE INDEX "XXMAP"."XXMAP_GL_E001_KASEYA_NS_SEGMENTS_N1" ON "XXMAP"."XXMAP_GL_E001_COA_NSERP_DATA" ("NS_SEGMENT1", "NS_SEGMENT2", "NS_SEGMENT3", "NS_SEGMENT4", "NS_SEGMENT5", "NS_SEGMENT6", "NS_SEGMENT7", "NS_SEGMENT8", "NS_SEGMENT9", "NS_SEGMENT10") ;

--------------------------------------------------------
--  DDL for Index XXINT_GL_I003_ADP_JOURNALS_STG_N3
--------------------------------------------------------

  CREATE INDEX "XXINT"."XXINT_GL_I003_ADP_JOURNALS_STG_N3" ON "XXINT"."XXINT_GL_I003_ADP_JOURNALS_STG" ("LOAD_REQUEST_ID") ;


--------------------------------------------------------
--  DDL for Index XXINT_GL_I001_JOURNALS_STG_N4
--------------------------------------------------------

  CREATE INDEX "XXINT"."XXINT_GL_I001_JOURNALS_STG_N4" ON "XXINT"."XXINT_GL_I001_JOURNALS_STG" ("NS_SEGMENT1", "NS_SEGMENT2", "NS_SEGMENT3", "NS_SEGMENT4", "NS_SEGMENT5", "NS_SEGMENT6", "NS_SEGMENT7", "NS_SEGMENT8", "NS_SEGMENT9", "NS_SEGMENT10") ;


--------------------------------------------------------
--  DDL for Index XXINT_GL_I001_JOURNALS_STG_N5
--------------------------------------------------------

  CREATE INDEX "XXINT"."XXINT_GL_I001_JOURNALS_STG_N5" ON "XXINT"."XXINT_GL_I001_JOURNALS_STG" ("LOAD_REQUEST_ID") ;


--------------------------------------------------------
--  DDL for Index XXINT_GL_I001_JOURNALS_STG_N6
--------------------------------------------------------

  CREATE INDEX "XXINT"."XXINT_GL_I001_JOURNALS_STG_N6" ON "XXINT"."XXINT_GL_I001_JOURNALS_STG" ("CURRENT_INSTANCE_ID", "STATUS") ;
