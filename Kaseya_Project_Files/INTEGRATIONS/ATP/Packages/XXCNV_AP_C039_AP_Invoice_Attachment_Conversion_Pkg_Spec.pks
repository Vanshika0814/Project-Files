create or replace PACKAGE       XXCNV.XXCNV_AP_C039_AP_Invoices_Attachments_CONVERSION_PKG   AS 

	/**************************************************************
    NAME              :     XXCNV_AP_C039_AP_Invoices_Attachments_CONVERSION_PKG
    PURPOSE           :     SPEC Of Procedures 
	-- Modification History
	-- Developer          Date             Version     Comments and changes made
	-- -------------   ------            ----------  -------------------------------------------
	-- Bhargavi.K	      17-JUL-2025  	 1.0         Initial Development
    -- Bhargavi.K         26-JUL-2025    1.1         Removed XXCNV. at line22
	-- Bhargavi.k         28-Jul-2025    1.2         Added slash at the end of the comments
    **************************************************************/
PROCEDURE MAIN_PRC(p_rice_id in varchar2,
               p_execution_id in varchar2,
               p_boundary_system in varchar2,
               p_file_name in varchar2);

PROCEDURE IMPORT_DATA_FROM_OCI_TO_STG_PRC( p_loading_status  OUT 	VARCHAR2                            
									  );



END XXCNV_AP_C039_AP_Invoices_Attachments_CONVERSION_PKG ;