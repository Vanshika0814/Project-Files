create or replace PACKAGE XXCNV.XXCNV_PO_C007_OPEN_PURCHASE_ORDER_CONVERSION_PKG IS
 
	/**************************************************************
    NAME              :     XXCNV.XXCNV_PO_C007_OPEN_PURCHASE_ORDER_CONVERSION_PKG
    PURPOSE           :     SPEC Of Procedures 
    -- Modification History
    -- Developer          Date         Version     Comments and changes made
    -- -------------     ------       ----------  -------------------------------------------
    -- 	Bhargavi.K	   27-Mar-2025 	    1.0         Initial Development
    --  Bhargavi.K         26-Jul-2025      1.1         Removed XXCNV. at line 25
    **************************************************************/

PROCEDURE MAIN_PRC ( p_RICE_ID 	            IN  		VARCHAR2,
                 p_execution_id 		IN  	    VARCHAR2,
                 p_boundary_system      IN  		VARCHAR2,
			     p_file_name 		    IN  		VARCHAR2);



PROCEDURE IMPORT_DATA_FROM_OCI_TO_STG_PRC( p_loading_status  OUT 	VARCHAR2                            
									  );
PROCEDURE data_validations_prc;
 PROCEDURE create_fbdi_file_prc;
 PROCEDURE CREATE_RECON_REPORT_PRC;
END XXCNV_PO_C007_OPEN_PURCHASE_ORDER_CONVERSION_PKG;


