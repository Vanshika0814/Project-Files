create or replace PACKAGE BODY xxcnv.xxcnv_cmn_conversion_wrapper_pkg IS	

	/*************************************************************************************
    NAME              :     main_prc
    PURPOSE           :     Main procedure called by OIC to execute Wrapper	
	Modification History
	Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -----------------------------------------
	Pendala Satya Pavani	   24-Feb-2025  	   1.0         Initial Development
	****************************************************************************************/
    gc_status_staged CONSTANT VARCHAR2(50) := 'File_Picked_From_OCI_Server';
--gc_boundary_system		CONSTANT	VARCHAR2(10)		:= 'HT_PS_FSCM';

    PROCEDURE main_prc (
        p_execution_id        IN VARCHAR2,
        p_file_name           IN VARCHAR2,
        p_object_path         IN VARCHAR2,
        p_rice_id             IN VARCHAR2,
        p_transformer_routine IN VARCHAR2,
        p_boundary_system     IN VARCHAR2,
        p_output_status       OUT VARCHAR2
    ) IS

        lv_dbms_job_name       VARCHAR2(100) := NULL;
        lv_error_message       VARCHAR2(3000) := NULL;
        lv_sql                 VARCHAR2(3000) := NULL;
        lv_conversion_id       VARCHAR2(100) := p_rice_id;
        lv_execution_id        VARCHAR2(100) := p_execution_id;
        lv_boundary_system     VARCHAR2(100) := p_boundary_system;
        lv_file_name           VARCHAR2(2000) := p_file_name;
        lv_transformer_routine VARCHAR2(200) := p_transformer_routine;
    BEGIN
        lv_sql := 'BEGIN '
                  || lv_transformer_routine
                  || '('
                  || chr(39)
                  || lv_conversion_id
                  || chr(39)
                  || ','
                  || chr(39)
                  || lv_execution_id
                  || chr(39)
                  || ','
                  || chr(39)
                  || lv_boundary_system
                  || chr(39)
                  || ','
                  || chr(39)
                  || lv_file_name
                  || chr(39)
                  || ');
		       END;';

        dbms_output.put_line('lv_sql ' || lv_sql);
        lv_dbms_job_name := 'JOB_'
                            || p_rice_id
                            || to_char(sysdate, 'yyyymmddHHMMSS');
        xxcnv_cmn_conversion_log_message_pkg.write_log_prc(p_conversion_id => p_rice_id, p_execution_id => p_execution_id, p_execution_step => gc_status_staged
        , p_boundary_system => p_boundary_system, p_file_path => p_object_path,
                                                          p_file_name => p_file_name, p_process_reference => NULL, p_attribute1 => NULL
                                                          , p_attribute2 => NULL);

        dbms_scheduler.create_job(job_name => lv_dbms_job_name, job_type => 'PLSQL_BLOCK',
									--	job_action 		=>  'XX_DBMS_JOB.MAIN',

         job_action => 'BEGIN 
															 '
                                                                                                         || lv_transformer_routine
                                                                                                         || '('
                                                                                                         || CHR(39)
                                                                                                         || lv_conversion_id
                                                                                                         || CHR(39)
                                                                                                         || ','
                                                                                                         || CHR(39)
                                                                                                         || lv_execution_id
                                                                                                         || CHR(39)
                                                                                                         || ','
                                                                                                         || CHR(39)
                                                                                                         || lv_boundary_system
                                                                                                         || CHR(39)
                                                                                                         || ','
                                                                                                         || CHR(39)
                                                                                                         || lv_file_name
                                                                                                         || CHR(39)
                                                                                                         || ');
															 END;',
										--start_date 		=>  SYSDATE-1,
                                                                                                          enabled => TRUE, auto_drop => TRUE
                                                                                                         );  
	-- exec dbms_scheduler.run_job(lv_dbms_job_name);
        p_output_status := 'success' || lv_dbms_job_name;
        dbms_output.put_line(p_output_status);
        COMMIT;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            lv_error_message := sqlcode
                                || '->'
                                || substr(sqlerrm, 1, 3000)
                                || '->'
                                || dbms_utility.format_error_backtrace;

            dbms_output.put_line('Error in calling procedure xxcnv_cmn_conversion_wrapper_pkg.main_prc: ' || lv_error_message);
            p_output_status := 'fail'
                               || lv_dbms_job_name
                               || '_'
                               || lv_error_message;
            dbms_output.put_line(p_output_status);
    END main_prc;

END xxcnv_cmn_conversion_wrapper_pkg;