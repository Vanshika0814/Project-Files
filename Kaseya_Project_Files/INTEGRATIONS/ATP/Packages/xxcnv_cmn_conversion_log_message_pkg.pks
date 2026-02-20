create or replace PACKAGE       xxcnv.xxcnv_cmn_conversion_log_message_pkg
IS
	/****************************************************************************************
    NAME                 :     write_log_prc
    PURPOSE           	 :     This procedure is called by OIC to log the execution details
	Modification History :     

	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -------------------------------------------
	Pendala Satya Pavani	   24-Feb-2025  	   1.0         Initial Development
    ****************************************************************************************/

	PROCEDURE write_log_prc(
				p_conversion_id 		IN	VARCHAR2,
				p_execution_id			IN	VARCHAR2,
				p_execution_step		IN	VARCHAR2,
				p_boundary_system 		IN	VARCHAR2,
				p_file_path				IN	VARCHAR2,
				p_file_name				IN	VARCHAR2,
				p_process_reference		IN	VARCHAR2,
				p_attribute1		    IN	VARCHAR2,						
				p_attribute2		    IN	VARCHAR2					
	);

END xxcnv_cmn_conversion_log_message_pkg;