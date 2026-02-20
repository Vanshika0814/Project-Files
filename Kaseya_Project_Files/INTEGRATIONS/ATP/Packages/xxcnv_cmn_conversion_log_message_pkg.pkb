create or replace PACKAGE BODY xxcnv.xxcnv_cmn_conversion_log_message_pkg
IS
   /******************************************************************************************
    NAME                 :     write_log_prc
    PURPOSE           	 :     This procedure is called by OIC to log the execution details
	Modification History :     

	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -------------------------------------------
	Pendala Satya Pavani	   24-Feb-2025  	   1.0         Initial Development
    ******************************************************************************************/

	PROCEDURE write_log_prc (
				p_conversion_id 	IN	VARCHAR2,
				p_execution_id		IN	VARCHAR2,
				p_execution_step	IN	VARCHAR2,
				p_boundary_system 	IN	VARCHAR2,
				p_file_path			IN	VARCHAR2,
				p_file_name			IN	VARCHAR2,
				p_process_reference	IN	VARCHAR2,
				p_attribute1		IN	VARCHAR2,
				p_attribute2		IN	VARCHAR2			
	)
	IS
		ln_detail_count NUMBER	:= 0;
	BEGIN
		SELECT 	COUNT(*)
		INTO 	ln_detail_count 
		FROM 	xxcnv_cmn_conversion_execution_details
		WHERE	execution_id = p_execution_id;

		dbms_output.put_line('Count ln_detail_count in log table:'||ln_detail_count);
		IF	ln_detail_count = 0 THEN			
			dbms_output.put_line('Adding log in xxcnv_cmn_conversion_execution table');			
			BEGIN
				INSERT INTO xxcnv_cmn_conversion_execution (
							execution_id,
							conversion_id,
							boundary_system,
							file_path,
							file_name,
							file_timestamp,
							start_timestamp,
							end_timestamp,
							status,
							creation_date,
							last_update_date,
							created_by,
							last_updated_by

				)
				VALUES (
							p_execution_id,
							p_conversion_id,
							p_boundary_system,
							p_file_path,
							p_file_name,
							NULL,  	--TBD
							NULL,	--TBD
							NULL,	--TBD
							p_execution_step,
							SYSDATE,
							SYSDATE,
							0,
							0	
				);

			EXCEPTION
				WHEN OTHERS
				THEN
					dbms_output.put_line('Exception in adding log in xxcnv_cmn_conversion_execution table'  
										|| SQLCODE
										|| '->'
										|| SUBSTR (SQLERRM, 1, 3000)
										|| '->'
										|| DBMS_UTILITY.format_error_backtrace);
			END;
		ELSE
			UPDATE xxcnv_cmn_conversion_execution
			SET status 		= p_execution_step,
				file_path 	= NVL( p_file_path, file_path),
				file_name	= NVL( p_file_name, file_name),
				last_update_date = SYSDATE,
				last_updated_by = 0
			WHERE 	execution_id = p_execution_id;
		END IF;

		BEGIN
			dbms_output.put_line('Adding log in xxcnv_cmn_conversion_execution_details table. Step: '||p_execution_step);
			INSERT INTO xxcnv_cmn_conversion_execution_details(
									execution_id,
									execution_step,
									start_timestamp,
									end_timestamp,
									process_ref,
									file_path,
									file_name,
									file_timestamp,                       -- added by Roja on 22-08-2024
									attribute1,
									attribute2,
									creation_date,
									last_update_date,
									created_by,
									last_updated_by
			)
			VALUES (
									p_execution_id,
									p_execution_step,
									NULL,  	--TBD
									NULL,  	--TBD
									p_process_reference,
									p_file_path,
									p_file_name,
									NULL,                                 -- added by Roja on 22-08-2024
									p_attribute1,						  --ver 1.1
									p_attribute2,						  --ver 1.1
									SYSDATE,
									SYSDATE,
									0,
									0									
			);

		EXCEPTION
			WHEN OTHERS
			THEN
				dbms_output.put_line('Exception in adding log in xxcnv_cmn_conversion_execution_details table. Step: '||p_execution_step
										||' '
										|| SQLCODE
										|| '->'
										|| SUBSTR (SQLERRM, 1, 3000)
										|| '->'
										|| DBMS_UTILITY.format_error_backtrace);
			END;		
		COMMIT;
	EXCEPTION
		WHEN OTHERS
		THEN
			NULL;
	END write_log_prc;

END xxcnv_cmn_conversion_log_message_pkg;