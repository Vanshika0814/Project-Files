create or replace PACKAGE xxcnv.xxcnv_cmn_conversion_wrapper_pkg IS	
	
	/*************************************************************************************
    NAME              :     main_prc
    PURPOSE           :     Main procedure called by OIC to execute Wrapper	
	Modification History
	Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -----------------------------------------
	Pendala Satya Pavani	   24-Feb-2025  	   1.0         Initial Development
	****************************************************************************************/

    PROCEDURE main_prc (
        p_execution_id        IN VARCHAR2,
        p_file_name           IN VARCHAR2,
        p_object_path         IN VARCHAR2,
        p_rice_id             IN VARCHAR2,
        p_transformer_routine IN VARCHAR2,
        p_boundary_system     IN VARCHAR2,
        p_output_status       OUT VARCHAR2
    );
    END xxcnv_cmn_conversion_wrapper_pkg;