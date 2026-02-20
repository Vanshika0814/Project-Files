--Insert script for GL_I001 metadata table
--Thursday 10/30/2025
INSERT INTO XXCNV.XXCNV_CMN_CONVERSION_METADATA(
ID,Type,DESCRIPTION,PROCESS_AREA,MODULE,BOUNDARY_SYSTEM,DATA_OBJECT,PRIORITY,TRANSFORMER_TYPE,TRANSFORMER_ROUTINE,STOP_PROCESSING,FILE_SERVER_PATH,OBJ_STORAGE_PATH,SOURCE_COA,TARGET_COA)
VALUES
(
'GL_I001'
,'Inbound'
,'NS Journals Integration'
,'RTR'
,'GL'
,'NetSuite'
,'NS_Journals'
,'High'
,'PL/SQL'
,'xxcnv_gl_i001_gl_journals_conversion_pkg.main_prc'
,'PARTIAL'
,'/KSY_HOME/FIN/GL/GL_I001/Extract2'
,'https://objectstorage.us-ashburn-1.oraclecloud.com/n/id8thgcxl2q7/b/ksy-fusion-cnv-bucket-prod/o/'
,'NetSuite'
,'ERP Cloud'
);

COMMIT;