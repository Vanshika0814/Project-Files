create or replace PACKAGE xxcnv.XXCNV_FA_C013_FA_CONVERSION_PKG IS

	/**************************************************************
    NAME              :     XXCNV_FA_013_FA_CONVERSIONS_PKG SPEC
    PURPOSE           :     SPEC Of Procedures 
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -------------------------------------------
	-- 	PHanindra Kumar	  10-Mar-2024  	    1.0         Initial Development
    **************************************************************/


PROCEDURE MAIN_PRC( p_RICE_ID 	            IN  		VARCHAR2,
                 p_execution_id 		IN  	    VARCHAR2,
                 p_boundary_system      IN  		VARCHAR2,
			     p_file_name 		    IN  		VARCHAR2);


PROCEDURE IMPORT_DATA_FROM_OCI_TO_STG_PRC( p_loading_status  OUT 	VARCHAR2);

PROCEDURE DATA_VALIDATIONS_PRC;

PROCEDURE CREATE_FBDI_FILE_PRC;

PROCEDURE COA_TARGET_SEGMENTS_PRC;

PROCEDURE CREATE_PROPERTIES_FILE_PRC;

PROCEDURE  CREATE_RECON_REPORT_PRC;

END XXCNV_FA_C013_FA_CONVERSION_PKG;