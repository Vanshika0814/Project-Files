--------------------------------------------------------
--  DDL for Package Body XXINT_XX_I010_COMMON_LOGGING_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "XXINT"."XXINT_XX_I010_COMMON_LOGGING_PKG" IS
/********************************************************************************************
OBJECT NAME: KSY Common Error Logger Package
DESCRIPTION: Package specification for Common Error Logging and notification framework 
Version 	Name              	Date           		Version-Description
---------------------------------------------------------------------------
<1.0>		Kunjesh Singh   	18-Feb-2025 	    1.0-Initial Draft
**********************************************************************************************/


/*****************************************************************
	OBJECT NAME: Schedule Archive Procedure
	DESCRIPTION: Wrapper Procedure for scheduling archive_prc
	Version 	Name              	    Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		CHANDRA MOULI GUPTA   	09-SEP-2025	        1.0- Initial Draft
	******************************************************************/
PROCEDURE schedule_archive_prc( 
    p_oic_instance_id IN varchar2
) AS

	lv_job_name VARCHAR2(2000);
	
BEGIN

    BEGIN
		lv_job_name := upper('"XXINT_JOB_'||p_oic_instance_id||'"');
        --schedule job for running the main procedure call and avoid dbms timeout in OIC
        --schedule without enabling it
		DBMS_SCHEDULER.CREATE_JOB(
        job_name     => lv_job_name,
        job_type     => 'STORED_PROCEDURE',
        job_action   => 'XXINT.XXINT_XX_I010_COMMON_LOGGING_PKG.archive_prc',
        number_of_arguments => 1,
        enabled      => FALSE
    );

		--set parameter value for child procedure
        DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE(
			job_name => lv_job_name,
			argument_position => 1,
			argument_value => p_oic_instance_id
		);
        
		--scheduling the job to run main procedure
		dbms_scheduler.ENABLE(lv_job_name);
        
		COMMIT;

        
        dbms_output.put_line('scheduled archive_prc successfully with JOB NAME '||lv_job_name);
               
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling schedule_archive_prc: '||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
            RETURN;
    END;

END schedule_archive_prc;

/*****************************************************************
	OBJECT NAME: Archive Procedure
	DESCRIPTION: Procedure for Archiving Data
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		KUNJESH SINGH   	18-FEB-2025	    1.0- Initial Draft
	******************************************************************/
   PROCEDURE archive_prc (
    p_oic_instance_id   IN VARCHAR2
) AS
    l_sql_select_rowid  VARCHAR2(4000);
    l_sql_count         VARCHAR2(4000);
    l_sql_insert        VARCHAR2(4000);
    l_sql_delete        VARCHAR2(4000);
    l_error_message     VARCHAR2(4000);
    l_count_check       NUMBER;

    TYPE t_rowid_tab IS TABLE OF ROWID INDEX BY PLS_INTEGER;
    l_rowids            t_rowid_tab;
    l_limit             NUMBER ;--:= '10000'; --removed hard coding
    l_rows_processed    NUMBER := 0;
    l_cursor            SYS_REFCURSOR;
BEGIN
    -- Archive Tables
    FOR j IN (
    SELECT
        table_owner,
        table_name,
        arc_table_owner,
        arc_table_name,
        tbl_creation_date_format,
        atp_db_purge_freq_days,
        attribute1
    FROM
        xxint.xxint_xx_i010_common_rice_objects_ref
    WHERE
            arc_flag = 'Y'
        AND arc_table_name IS NOT NULL
) LOOP
    BEGIN
        
        l_limit := j.attribute1;
    
        dbms_output.put_line('Archive Start for '
                             || j.table_name
                             || ' '
                             || current_timestamp);
        IF j.atp_db_purge_freq_days IS NULL THEN
            dbms_output.put_line('Skipping archive for '
                                 || j.table_name
                                 || ': ATP_DB_PURGE_FREQ_DAYS is NULL');
            CONTINUE;
        END IF;

        l_rows_processed := 0;

            -- Compose the dynamic SQL to select ROWIDs for eligible rows
            -- This is added as LIMIT can not be used with EXECUTE IMMEDIATE 
        l_sql_select_rowid := 'SELECT ROWID FROM '
                              || j.table_owner
                              || '.'
                              || j.table_name
                              || ' WHERE TO_DATE(TO_CHAR(TO_DATE(SUBSTR(CREATION_DATE,1,10),'''
                              || j.tbl_creation_date_format
                              || '''),''DD-MM-YYYY''),''DD-MM-YYYY'') < SYSDATE - '
                              || j.atp_db_purge_freq_days;

        OPEN l_cursor FOR l_sql_select_rowid;

        LOOP
            FETCH l_cursor
            BULK COLLECT INTO l_rowids LIMIT l_limit;
            EXIT WHEN l_rowids.count = 0;

                -- Build the list of quoted ROWIDs for the IN clause
            BEGIN
                    -- Bulk insert ROWIDs into temp table
                FORALL idx IN 1..l_rowids.count
                    INSERT INTO xxint.xxint_xx_i010_archive_row_id_temp VALUES ( l_rowids(idx) );

                    -- Compose the dynamic SQL to insert the batch into the archive table
                l_sql_insert := 'INSERT INTO '
                                || j.arc_table_owner
                                || '.'
                                || j.arc_table_name
                                || ' SELECT a.* FROM '
                                || j.table_owner
                                || '.'
                                || j.table_name
                                || ' a'
                                || ' JOIN XXINT.XXINT_XX_I010_ARCHIVE_ROW_ID_TEMP t ON a.ROWID = t.row_id';
                        --|| ' WHERE ROWID IN (' || l_rowid_list || ')';

                EXECUTE IMMEDIATE l_sql_insert;
                COMMIT;
                l_rows_processed := l_rows_processed + l_rowids.count;
            END;

                -- Exit if less than limit (after last batch)
            IF l_rowids.count < l_limit THEN
                EXIT;
            END IF;
        END LOOP;

        CLOSE l_cursor;
        dbms_output.put_line('Archive successful for '
                             || j.table_name
                             || '. Total rows: '
                             || l_rows_processed
                             || ' '
                             || current_timestamp);

    EXCEPTION
        WHEN OTHERS THEN
            l_error_message := 'Archive failed for '
                               || j.table_name
                               || ' : '
                               || substr(sqlerrm, 1, 3000)
                               || '->'
                               || dbms_utility.format_error_backtrace
                               || ' '
                               || current_timestamp;

            dbms_output.put_line(l_error_message);
    END;
END LOOP;

   -- Purge Stage Tables 
FOR i IN (
    SELECT
        table_owner,
        table_name,
        tbl_creation_date_format,
        atp_db_purge_freq_days,
        arc_table_owner,
        arc_table_name
    FROM
        xxint.xxint_xx_i010_common_rice_objects_ref
    WHERE
        atp_db_purge_flag = 'Y'
) LOOP
    BEGIN
        dbms_output.put_line('Purging Started for '
                             || i.table_name
                             || ' '
                             || current_timestamp);
        IF i.atp_db_purge_freq_days IS NULL THEN
            dbms_output.put_line('Skipping purge for '
                                 || i.table_name
                                 || ': ATP_DB_PURGE_FREQ_DAYS is NULL');
            CONTINUE;
        END IF;
        
        
        -- Check if table name ends with '_ARC'
        IF upper(i.table_name) LIKE '%_ARC' THEN
            -- Directly purge eligible rows (no archive check)
            l_sql_delete := 'DELETE FROM '
                            || i.table_owner
                            || '.'
                            || i.table_name
                            || ' WHERE TO_DATE(TO_CHAR(TO_DATE(SUBSTR(CREATION_DATE,1,10),'''
                            || i.tbl_creation_date_format
                            || '''),''DD-MM-YYYY''),''DD-MM-YYYY'') < SYSDATE - '
                            || i.atp_db_purge_freq_days;

            EXECUTE IMMEDIATE l_sql_delete;

            -- Update last purge date
            UPDATE xxint.xxint_xx_i010_common_rice_objects_ref
            SET
                last_atp_db_purge_date = to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS')
            WHERE
                table_name = i.table_owner
                             || '.'
                             || i.table_name;

            COMMIT;
            dbms_output.put_line('Purging successful for '
                                 || i.table_name
                                 || ' '
                                 || current_timestamp);
        ELSE
        -- Check if there are any eligible rows in the archive table
            l_sql_count := 'SELECT COUNT(1) FROM '
                           || i.arc_table_owner
                           || '.'
                           || i.arc_table_name
                           || ' WHERE TO_DATE(TO_CHAR(TO_DATE(SUBSTR(CREATION_DATE,1,10),'''
                           || i.tbl_creation_date_format
                           || '''),''DD-MM-YYYY''),''DD-MM-YYYY'') < SYSDATE - '
                           || i.atp_db_purge_freq_days;

            EXECUTE IMMEDIATE l_sql_count
            INTO l_count_check;
            IF l_count_check != 0 THEN

            -- Delete only rows that exist in the archive table
                l_sql_delete := 'DELETE FROM '
                                || i.table_owner
                                || '.'
                                || i.table_name
                                || ' s '
                                || 'WHERE TO_DATE(TO_CHAR(TO_DATE(SUBSTR(s.CREATION_DATE,1,10),'''
                                || i.tbl_creation_date_format
                                || '''),''DD-MM-YYYY''),''DD-MM-YYYY'') < SYSDATE - '
                                || i.atp_db_purge_freq_days
                                || ' AND EXISTS (SELECT 1 FROM '
                                || i.arc_table_owner
                                || '.'
                                || i.arc_table_name
                                || ' a '
                                || ' WHERE TO_DATE(TO_CHAR(TO_DATE(SUBSTR(CREATION_DATE,1,10),'''
                                || i.tbl_creation_date_format
                                || '''),''DD-MM-YYYY''),''DD-MM-YYYY'') < SYSDATE - '
                                || i.atp_db_purge_freq_days
                                || ')';

                EXECUTE IMMEDIATE l_sql_delete;

            -- Update last purge date
                UPDATE xxint.xxint_xx_i010_common_rice_objects_ref
                SET
                    last_atp_db_purge_date = to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS')
                WHERE
                    table_name = i.table_owner
                                 || '.'
                                 || i.table_name;

                COMMIT;
                dbms_output.put_line('Purging successful for '
                                     || i.table_name
                                     || ' '
                                     || current_timestamp);
            ELSE
                dbms_output.put_line('Skipping purge for '
                                     || i.table_name
                                     || ': Matching data not found in the archive table.');
            END IF;

        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            l_error_message := 'Purge failed for '
                               || i.table_name
                               || ': '
                               || sqlerrm
                               || ' '
                               || current_timestamp;

            dbms_output.put_line(l_error_message);
    END;
END LOOP;

COMMIT;

    /*x_status := 'SUCCESS';
    x_status_message := 'Archiving and purging completed. Check logs for details.';*/
    DBMS_OUTPUT.PUT_LINE('Archiving and purging completed successfully');

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Unexpected exception in archive_prc. Error Details - ' || SQLERRM||' '||CURRENT_TIMESTAMP);
        /*x_status := 'ERROR';
        x_status_message := 'Unexpected exception in archive_prc. Error Details - ' || SQLERRM;*/
END archive_prc;


	/*****************************************************************
		OBJECT NAME: Check SFTP Archive Status Procedure
		DESCRIPTION: Procedure for checking status of archival for all 
		Version 	Name              	    Date           		Version-Description
		----------------------------------------------------------------------------
		<1.0>		CHANDRA MOULI GUPTA   	15-SEP-2025	        1.0- Initial Draft
		******************************************************************/
	PROCEDURE check_sftp_archive_prc( 
		p_oic_instance_id IN varchar2,
		p_status OUT varchar2,
		p_message OUT varchar2
	) AS
		lv_total_files NUMBER := 0;
		lv_sum_distinct_child_files NUMBER := 0;
		lv_error_message varchar2(4000) :='';

	BEGIN

	BEGIN
		--Get total number of files for the given parent process id in the details table
		SELECT COUNT(1)
		INTO lv_total_files
		FROM xxint.xxint_xx_i010_sftp_archive_details_tbl
		WHERE parent_process_id = p_oic_instance_id;
        
		--Get sum of distinct totalFileForChildInstance for each current process id under the parent process id
		SELECT NVL(SUM(child_file_count),0)
		INTO lv_sum_distinct_child_files
		FROM (
			SELECT child_process_id, MAX(total_file_count) AS child_file_count
			FROM xxint.xxint_xx_i010_sftp_archive_tbl
			WHERE parent_process_id = p_oic_instance_id
			GROUP BY child_process_id
		);

		IF lv_total_files = lv_sum_distinct_child_files THEN
			DBMS_OUTPUT.PUT_LINE('Counts MATCH.');
			p_status:='Y';
		ELSE
			DBMS_OUTPUT.PUT_LINE('Counts DO NOT MATCH.');
			p_status:='N';
		END IF;

	EXCEPTION
		WHEN NO_DATA_FOUND THEN
            --p_status := 'Y';--as there are no files to check status for. If marked N, in OIC the while loop wont end
			lv_error_message := 'No records found for parent process id ' || p_oic_instance_id;
            p_message := lv_error_message;
			DBMS_OUTPUT.PUT_LINE(lv_error_message);
		WHEN OTHERS THEN
            --p_status := 'Y';
			lv_error_message := 'Unexpected error: ' || SQLERRM;
            p_message := lv_error_message;
			DBMS_OUTPUT.PUT_LINE(lv_error_message);
	END;
		
	END;

	/*****************************************************************
	OBJECT NAME: Common Logging Procedure
	DESCRIPTION: Main Procedure for Common Error Logging and notification framework 
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Kunjesh Singh   	18-Feb-2025 	    1.0-Initial Draft
	<1.1>		Devishi				28-Oct-2025			1.1- Changes related to case number: 01098700/BO-163
	******************************************************************/

    PROCEDURE logger_prc (
        p_operation      IN VARCHAR2,
        p_logger_details IN logger_type,
        x_status         OUT VARCHAR2,
        x_status_message OUT VARCHAR2
    ) AS

        l_status                     VARCHAR2(100) := '';
        l_status_message             VARCHAR2(4000) := '';
        l_interface_name             VARCHAR2(400) := p_logger_details.interface_name;
        l_interface_description      VARCHAR2(1000) := p_logger_details.interface_name;
        l_source                     VARCHAR2(200) := p_logger_details.source;
        l_target                     VARCHAR2(400) := p_logger_details.target;
		-- Fetch Payload CONTENTS                                                                                                
        l_log_flag                   VARCHAR2(20) := p_logger_details.log_flag;
        l_notify_flag                VARCHAR2(20) := p_logger_details.notify_flag;
        l_interface_rice_id          VARCHAR2(200) := p_logger_details.interface_rice_id;
        l_parent_process_id          VARCHAR2(200) := p_logger_details.parent_process_id;
        l_current_process_id         VARCHAR2(200) := p_logger_details.current_process_id;
        l_integration_status         VARCHAR2(200) := p_logger_details.status;
        l_integration_name           VARCHAR2(1000) := p_logger_details.integration_name;
        l_integration_version        VARCHAR2(200) := p_logger_details.integration_version;
        l_invoked_by                 VARCHAR2(400) := p_logger_details.invoked_by;
        l_instance                   VARCHAR2(200) := p_logger_details.instance;
        l_process_start_time         VARCHAR2(200) := p_logger_details.process_start_time;
        l_process_end_time           VARCHAR2(200) := p_logger_details.process_end_time;
        l_business_identifier_key    VARCHAR2(1000) := p_logger_details.business_identifier_key;
        l_business_identifier_value  VARCHAR2(4000) := p_logger_details.business_identifier_value;
        l_stage                      VARCHAR2(200) := p_logger_details.stage;
        l_source_file_name           VARCHAR2(4000) := p_logger_details.source_file_name; ---<Code Change 1.1>
        l_source_file_path           VARCHAR2(1000) := p_logger_details.source_file_path;
        l_file_checksum              VARCHAR2(1000) := p_logger_details.file_checksum;
        l_request_payload            CLOB := p_logger_details.request_payload;
        l_response_payload           CLOB := p_logger_details.response_payload;
        l_store_activity_stream_flag VARCHAR2(20) := p_logger_details.store_activity_stream_flag;
        l_activity_stream            CLOB := p_logger_details.activity_stream;
        l_oracle_erp_process_name    VARCHAR2(200) := p_logger_details.oracle_erp_process_name;
        l_oracle_erp_process_id      VARCHAR2(200) := p_logger_details.oracle_erp_process_id;
        l_is_fbdi_process            VARCHAR2(20) := p_logger_details.is_fbdi_process;
        l_enabled_callback           VARCHAR2(20) := p_logger_details.enabled_callback;
        l_is_final_process           VARCHAR2(20) := p_logger_details.is_final_process;
        l_ucm_upload_request_id      VARCHAR2(200) := p_logger_details.ucm_upload_request_id;
        l_ucm_upload_status          VARCHAR2(200) := p_logger_details.ucm_upload_status;
        l_load_request_id            VARCHAR2(200) := p_logger_details.load_request_id;
        l_load_request_status        VARCHAR2(200) := p_logger_details.load_request_status;
        l_import_request_id          VARCHAR2(200) := p_logger_details.import_request_id;
        l_import_request_status      VARCHAR2(200) := p_logger_details.import_request_status;
        l_callback_received          VARCHAR2(20) := p_logger_details.callback_received;
        l_file_count                 NUMBER := p_logger_details.file_count;
        l_total_batch_count          NUMBER := p_logger_details.total_batch_count;
        l_total_row_count            NUMBER := p_logger_details.total_row_count;
        l_total_success_count        NUMBER := p_logger_details.total_success_count;
        l_total_error_count          NUMBER := p_logger_details.total_error_count;
        l_header_error_code          VARCHAR2(4000) := p_logger_details.header_error_code;
        l_header_error_summary       VARCHAR2(4000) := p_logger_details.header_error_summary;
        l_header_error_details       CLOB := p_logger_details.header_error_details;
        l_batch_details              batch_details_tbl_type := p_logger_details.batch_details;
        l_batch_details_count        NUMBER;
        l_error_line_details         error_line_details_tbl_type := p_logger_details.error_line_details;
        l_error_line_count           NUMBER;
        l_custom_proc_exp EXCEPTION;
        l_invalid_payload EXCEPTION;
    BEGIN

		-- Payload Validation
        IF l_interface_rice_id IS NULL OR l_parent_process_id IS NULL OR l_current_process_id IS NULL OR l_integration_status IS NULL
        OR l_instance IS NULL OR l_integration_name IS NULL OR l_integration_version IS NULL OR l_invoked_by IS NULL OR l_interface_name
        IS NULL OR l_interface_description IS NULL OR l_source IS NULL OR l_target IS NULL OR ( (
            l_process_start_time IS NULL
            AND l_integration_status = 'START'
        ) OR (
            l_process_end_time IS NULL
            AND l_integration_status = 'SUCCESS'
        ) ) THEN
            l_status := 'MANDATORY_VALUES_MISSING_IN_PAYLOAD';
            l_status_message := 'Mandatory Values missing in the request payload of Common Logger Notification Integration. Check OIC KSY Interface Metadata Lookup Configuration.'
            ;
            l_status_message := l_status_message
                                || '. Values passed - interface_rice_id - '
                                || l_interface_rice_id
                                || ' ; parent_process_id - '
                                || l_parent_process_id
                                || ' ; current_process_id - '
                                || l_current_process_id
                                || ' ; instance - '
                                || l_instance
                                || ' ; interface_name - '
                                || l_interface_name
                                || ' ; interface_description - '
                                || l_interface_description
                                || ' ; source - '
                                || l_source
                                || ' ; target - '
                                || l_target
                                || ' ; integration_status - '
                                || l_integration_status
                                || ' ; integration_name - '
                                || l_integration_name
                                || ' ; integration_version - '
                                || l_integration_version
                                || ' ; invoked_by - '
                                || l_invoked_by
                                || ' ; process_start_time - '
                                || l_process_start_time
                                || ' ; process_end_time - '
                                || l_process_end_time;

            l_status_message := l_status_message || ', Please Note - process_start_time is mandatory when status passed is START and process_end_time is mandatory when status passed is SUCCESS/ERROR.'
            ;
            RAISE l_custom_proc_exp;
        END IF;

        IF l_integration_status = 'START' THEN
		-- Push data to AUDIT table

            INSERT INTO XXINT_XX_I010_AUDIT_LOG (
                interface_rice_id,
                interface_rice_name,
                log_flag,
                notify_flag,
                parent_process_id,
                current_process_id,
                status,
                is_final_process,
                integration_name,
                integration_version,
                invoked_by,
                instance,
                process_start_time,
                process_end_time,
                business_indetifier_key,
                business_indetifier_value,
                stage,
                source,
                target,
                source_file_name,
                source_file_path,
                source_file_checksum,
                integration_request_payload,
                integration_response_payload,
                activity_stream_flag,
                activity_stream,
                oracle_process_name,
                oracle_process_id,
                is_fbdi_process,
                enabled_callback,
                ucm_upload_request_id,
                ucm_upload_status,
                load_request_id,
                load_request_status,
                import_request_id,
                import_request_status,
                callback_received,
                file_count,
                total_batch_count,
                total_row_count,
                total_success_count,
                total_error_count,
                header_error_code,
                header_error_summary,
                header_error_details,
                creation_date,
                created_by,
                last_update_date,
                last_updated_by
            ) VALUES (
                l_interface_rice_id,	                                    --INTERFACE_RICE_ID		
                l_interface_name,                                           --INTERFACE_RICE_NAME     
                l_log_flag,                                                 --LOG_FLAG				
                l_notify_flag,	                                            --NOTIFY_FLAG                        
                l_parent_process_id,                                        --PARENT_PROCESS_ID                  
                l_current_process_id,	                                    --CURRENT_PROCESS_ID                 
                l_integration_status,	                                    --STATUS                             
                l_is_final_process,	                                        --IS_FINAL_PROCESS                   
                l_integration_name,                                         --INTEGRATION_NAME                   
                l_integration_version,                                      --INTEGRATION_VERSION                
                l_invoked_by,	                                            --INVOKED_BY                         
                l_instance,                 	                            --INSTANCE                           
                l_process_start_time,                                   --PROCESS_START_TIME                 
                l_process_end_time,                                       --PROCESS_END_TIME                   
                l_business_identifier_key,                               --BUSINESS_INDETIFIER_KEY            
                l_business_identifier_value,                               --BUSINESS_INDETIFIER_VALUE          
                l_stage,                                                   --STAGE                              
                l_source,                                               --SOURCE                             
                l_target,                                               --TARGET                             
                l_source_file_name,                                       --SOURCE_FILE_NAME		
                l_source_file_path,                                       --SOURCE_FILE_PATH                   
                l_file_checksum,	                                        --SOURCE_FILE_CHECKSUM    
                l_request_payload,                                          --INTEGRATION_REQUEST_PAYLOAD        
                l_response_payload,                                         --INTEGRATION_RESPONSE_PAYLOAD       
                l_store_activity_stream_flag,                               --ACTIVITY_STREAM_FLAG               
                l_activity_stream,                                          --ACTIVITY_STREAM                    
                l_oracle_erp_process_name,                                  --ORACLE_PROCESS_NAME                
                l_oracle_erp_process_id,                                    --ORACLE_PROCESS_ID                  
                l_is_fbdi_process,                                          --IS_FBDI_PROCESS                    
                l_enabled_callback,                                         --ENABLED_CALLBACK                   
                l_ucm_upload_request_id,                                    --UCM_UPLOAD_REQUEST_ID              
                l_ucm_upload_status,                                        --UCM_UPLOAD_STATUS                  
                l_load_request_id,                                          --LOAD_REQUEST_ID                    
                l_load_request_status,                                      --LOAD_REQUEST_STATUS                
                l_import_request_id,                                        --IMPORT_REQUEST_ID                  
                l_import_request_status,                                    --IMPORT_REQUEST_STATUS              
                l_callback_received,                                        --CALLBACK_RECEIVED                  
                l_file_count,               	                            --FILE_COUNT                         
                l_total_batch_count,                                        --TOTAL_BATCH_COUNT		
                l_total_row_count,                                          --TOTAL_ROW_COUNT                    
                l_total_success_count,                                      --TOTAL_SUCCESS_COUNT                
                l_total_error_count,                                        --TOTAL_ERROR_COUNT                  
                l_header_error_code,                                        --HEADER_ERROR_CODE                  
                l_header_error_summary,                                     --HEADER_ERROR_SUMMARY               
                l_header_error_details,                                     --HEADER_ERROR_DETAILS               
                to_char(sysdate, 'YYYY/MM/DD HH24:MI:SS'),               	--CREATION_DATE                      
                'OIC-ATP',               									--CREATED_BY                         
                to_char(sysdate, 'YYYY/MM/DD HH24:MI:SS'),               	--last_update_date                  
                'OIC-ATP'              										--LAST_UPDATED_BY 
            );

        ELSE
			-- UPDATE XXINT_XX_I010_AUDIT_LOG WHERE 
            UPDATE XXINT_XX_I010_AUDIT_LOG
            SET
                interface_rice_name = l_interface_name,
                log_flag = l_log_flag,
                notify_flag = l_notify_flag,
                status = l_integration_status,
                is_final_process = l_is_final_process,
                integration_name = l_integration_name,
                integration_version = l_integration_version,
                invoked_by = l_invoked_by,
                instance = l_instance,
                process_start_time = nvl(l_process_start_time, process_start_time),
                process_end_time = l_process_end_time,
                business_indetifier_key = l_business_identifier_key,
                business_indetifier_value = l_business_identifier_value,
                stage = l_stage,
                source = l_source,
                target = l_target,
                source_file_name = l_source_file_name,
                source_file_path = l_source_file_path,
                source_file_checksum = l_file_checksum,
                integration_request_payload = l_request_payload,
                integration_response_payload = l_response_payload,
                activity_stream_flag = l_store_activity_stream_flag,
                activity_stream = l_activity_stream,
                oracle_process_name = l_oracle_erp_process_name,
                oracle_process_id = l_oracle_erp_process_id,
                is_fbdi_process = l_is_fbdi_process,
                enabled_callback = l_enabled_callback,
                ucm_upload_request_id = l_ucm_upload_request_id,
                ucm_upload_status = l_ucm_upload_status,
                load_request_id = l_load_request_id,
                load_request_status = l_load_request_status,
                import_request_id = l_import_request_id,
                import_request_status = l_import_request_status,
                callback_received = l_callback_received,
                file_count = l_file_count,
                total_batch_count = l_total_batch_count,
                total_row_count = l_total_row_count,
                total_success_count = l_total_success_count,
                total_error_count = l_total_error_count,
                header_error_code = l_header_error_code,
                header_error_summary = l_header_error_summary,
                header_error_details = l_header_error_details,
                last_update_date = to_char(sysdate, 'YYYY/MM/DD HH24:MI:SS'),
                last_updated_by = 'OIC-ATP'
            WHERE
                    interface_rice_id = l_interface_rice_id
                AND parent_process_id = l_parent_process_id
                AND current_process_id = l_current_process_id;

        END IF;

        COMMIT;

		-- Error Details Entry Scope
        BEGIN
			-- Check for Error Records if any
            SELECT
                COUNT(*)
            INTO l_error_line_count
            FROM
                TABLE ( l_error_line_details );

            IF l_error_line_count > 0 THEN
				-- Insert Error Details Records

                FOR i IN l_error_line_details.first..l_error_line_details.last LOOP
                    INSERT INTO XXINT_XX_I010_ERROR_DETAILS_LOG (
                        interface_rice_id,
                        integration_name,
                        parent_process_id,
                        current_process_id,
                        status,
                        location,
                        error_code,
                        error_summary,
                        error_details,
                        creation_date,
                        created_by,
                        last_update_date,
                        last_updated_by
                    ) VALUES (
                        l_interface_rice_id,						 						 --INTERFACE_RICE_ID	
                        l_integration_name,						 						 --INTEGRATION_NAME   
                        l_parent_process_id,						 						 --PARENT_PROCESS_ID  
                        l_current_process_id,						 						 --CURRENT_PROCESS_ID 
                        'ERROR',            						 						 --STATUS             
                        l_error_line_details(i).error_line_location,						 --LOCATION           
                        l_error_line_details(i).error_line_code,    						 --ERROR_CODE         
                        l_error_line_details(i).error_line_summary,						 --ERROR_SUMMARY      
                        l_error_line_details(i).error_line_details,                        --ERROR_DETAILS      
                        to_char(sysdate, 'YYYY/MM/DD HH24:MI:SS'),						 --CREATION_DATE      
                        'OIC-ATP',                                						 --CREATED_BY         
                        to_char(sysdate, 'YYYY/MM/DD HH24:MI:SS'),						 --last_update_date  
                        'OIC-ATP'	                                 						 --LAST_UPDATED_BY 
                    );

                END LOOP;

                COMMIT;
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'UNEXPECTED_EXPECTION_ERROR';
                l_status_message := 'Error in Pushing Error Details . Error Details - ' || sqlerrm;
                RAISE l_custom_proc_exp;
        END;

		-- Batch Details Entry Scope
        BEGIN
			-- Check for Error Records if any
            SELECT
                COUNT(*)
            INTO l_batch_details_count
            FROM
                TABLE ( l_batch_details );

            IF l_batch_details_count > 0 THEN
                FOR i IN l_batch_details.first..l_batch_details.last LOOP
                    INSERT INTO XXINT_XX_I010_BATCH_FILE_DETAILS_LOG (
                        interface_rice_id,
                        integration_name,
                        record_indentifier,
                        parent_process_id,
                        current_process_id,
                        status,
                        message,
                        source_file_name,
                        source_file_checksum,
                        total_row_count,
                        total_success_count,
                        total_error_count,
                        creation_date,
                        created_by,
                        last_update_date,
                        last_updated_by
                    ) VALUES (
                        l_interface_rice_id                          -- INTERFACE_RICE_ID		
                        ,
                        l_integration_name                           -- INTEGRATION_NAME         
                        ,
                        l_batch_details(i).batch_identifier                             -- RECORD_INDENTIFIER (-- Can contain Source file/Source File Batch Details)
                        ,
                        l_parent_process_id                          -- PARENT_PROCESS_ID        
                        ,
                        l_current_process_id                         -- CURRENT_PROCESS_ID       
                        ,
                        l_batch_details(i).batch_status                                 -- STATUS                   
                        ,
                        l_batch_details(i).batch_status_message                         -- MESSAGE
                        ,
                        l_batch_details(i).source_file_name 	  						  -- SOURCE_FILE_NAME
                        ,
                        l_batch_details(i).source_file_checksum 						  -- SOURCE_FILE_CHECKSUM
                        ,
                        l_batch_details(i).batch_row_count                              -- TOTAL_ROW_COUNT          
                        ,
                        l_batch_details(i).batch_success_count                          -- TOTAL_SUCCESS_COUNT      
                        ,
                        l_batch_details(i).batch_error_count                            -- TOTAL_ERROR_COUNT
                        ,
                        to_char(sysdate, 'YYYY/MM/DD HH24:MI:SS')     -- CREATION_DATE      
                        ,
                        'OIC-ATP'									  -- CREATED_BY         
                        ,
                        to_char(sysdate, 'YYYY/MM/DD HH24:MI:SS')     -- last_update_date  
                        ,
                        'OIC-ATP'	          						  -- LAST_UPDATED_BY 
                    );

                END LOOP;

                COMMIT;
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'UNEXPECTED_EXPECTION_ERROR';
                l_status_message := 'Error in Pushing . Error Details - ' || sqlerrm;
                RAISE l_custom_proc_exp;
        END;

        COMMIT;
        x_status := 'SUCCESS';
        x_status_message := 'Logging Succesful';
    EXCEPTION
        WHEN l_custom_proc_exp THEN
            x_status := l_status;
            x_status_message := l_status_message;
        WHEN OTHERS THEN
            x_status := 'ERROR';
            x_status_message := 'Unexpected exception in Logging details in ATP DB. Please contact system admin.';
            x_status_message := x_status_message
                                || ';  Error Details - '
                                || sqlerrm;
    END logger_prc;

	/*****************************************************************
	OBJECT NAME: Common Notification Procedure
	DESCRIPTION: Procedure for Common notification 
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Kunjesh Singh   	18-Feb-2025 	    1.0- Initial Draft
	******************************************************************/

    PROCEDURE notification_prc (
        p_notification_details notify_type,
        x_email_address        OUT VARCHAR2,
        x_subject_line         OUT VARCHAR2,
        x_message_body         OUT CLOB,
        x_status               OUT VARCHAR2,
        x_status_message       OUT VARCHAR2
    ) AS

        l_status                     VARCHAR2(100) := '';
        l_status_message             VARCHAR2(4000) := '';
        l_interface_name             VARCHAR2(400) := p_notification_details.interface_name;
        l_interface_description      VARCHAR2(1000) := '';
        l_source                     VARCHAR2(200) := '';
        l_target                     VARCHAR2(400) := '';
        l_email_addresses            VARCHAR2(32000) := '';
        l_subject                    VARCHAR2(10000) := '';
        l_message_body               CLOB;
        l_interface_rice_id          VARCHAR2(200) := p_notification_details.interface_rice_id;
        l_parent_process_id          VARCHAR2(200) := p_notification_details.parent_process_id;
        l_current_process_id         VARCHAR2(200) := p_notification_details.current_process_id;
        l_instance                   VARCHAR2(200) := p_notification_details.instance;
        l_integration_status         VARCHAR2(200) := p_notification_details.status;
        l_oic_integration_name       VARCHAR2(1000) := p_notification_details.oic_integration_name;
        l_business_identifier_key    VARCHAR2(1000) := p_notification_details.business_identifier_key;
        l_business_identifier_value  VARCHAR2(4000) := p_notification_details.business_identifier_value;
        l_stage                      VARCHAR2(200) := p_notification_details.stage;
        l_file_name                  VARCHAR2(400) := p_notification_details.file_name;
        l_file_path                  VARCHAR2(1000) := p_notification_details.file_path;
        l_oracle_erp_process_name    VARCHAR2(200) := p_notification_details.oracle_erp_process_name;
        l_oracle_erp_process_id      VARCHAR2(200) := p_notification_details.oracle_erp_process_id;
        l_ucm_upload_request_id      VARCHAR2(200) := p_notification_details.ucm_upload_request_id;
        l_ucm_upload_request_status  VARCHAR2(200) := p_notification_details.ucm_upload_request_status;
        l_load_request_id            VARCHAR2(200) := p_notification_details.load_request_id;
        l_load_request_status        VARCHAR2(200) := p_notification_details.load_request_status;
        l_import_request_id          VARCHAR2(200) := p_notification_details.import_request_id;
        l_import_request_status      VARCHAR2(200) := p_notification_details.import_request_status;
        l_total_records_count        NUMBER := p_notification_details.total_records_count;
        l_success_records_count      NUMBER := p_notification_details.success_records_count;
        l_oracle_error_records_count NUMBER := p_notification_details.oracle_error_records_count;
        l_pre_error_records_count    NUMBER := p_notification_details.pre_error_records_count;
        l_has_attachement            VARCHAR2(20) := p_notification_details.has_attachement;
        l_log_file_ucm_url           VARCHAR2(1000) := p_notification_details.log_file_ucm_url;
        l_notification_type          VARCHAR2(100) := p_notification_details.notification_type;
        l_error_code                 VARCHAR2(1000) := p_notification_details.error_code;
        l_error_message              VARCHAR2(5000) := p_notification_details.error_message;
        l_error_details              VARCHAR2(5000) := p_notification_details.error_details;
        l_custom_proc_exp EXCEPTION;
    BEGIN
		-- Payload Validation
		-- Payload Validation
        IF l_interface_rice_id IS NULL OR l_interface_name IS NULL OR l_parent_process_id IS NULL OR l_current_process_id IS NULL OR l_integration_status
        IS NULL OR l_instance IS NULL OR l_oic_integration_name IS NULL OR l_notification_type IS NULL THEN
            l_status := 'MANDATORY_VALUES_MISSING_IN_PAYLOAD';
            l_status_message := 'Mandatory Values missing in the request payload of Common Logger Notification Integration';
            l_status_message := l_status_message
                                || '. Mandatory Values passed - interface_rice_id - '
                                || l_interface_rice_id
                                || ' ; interface_name - '
                                || l_interface_name
                                || ' ; parent_process_id - '
                                || l_parent_process_id
                                || ' ; current_process_id - '
                                || l_current_process_id
                                || ' ; instance - '
                                || l_instance
                                || ' ; integration_status - '
                                || l_integration_status
                                || ' ; oic integration_name - '
                                || l_oic_integration_name
                                || ' ; notification_type - '
                                || l_notification_type;

            l_status_message := l_status_message || ', Please Note - process_start_time is mandatory when status passed is START and process_end_time is mandatory when status passed is SUCCESS/ERROR(Ended in GLOBAL Fault/Is Final Process).'
            ;
            RAISE l_custom_proc_exp;
        END IF;


		-- Subject Line Derivation
        BEGIN
            SELECT
                l_integration_status
                || ' - '
                || upper(l_instance)
                || ' - '
                || l_interface_rice_id
                || ' - '
                || l_interface_name
                || ' - '
                || l_current_process_id
            INTO l_subject
            FROM
                dual;

            x_subject_line := l_subject;
        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'UNEXPECTED_EXPECTION_ERROR';
                l_status_message := 'Error in deriving Subject line for Notification. Error Details - ' || sqlerrm;
                RAISE l_custom_proc_exp;
        END;

		-- Message Body Derivation
        BEGIN
            IF l_notification_type = 'FBDI_IMPORT' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
													<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Source File Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Execution Status</strong></td>                    
													</tr>
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_file_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_integration_status
                                  || '</td>
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Oracle ERP Cloud execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
															<tr>
																<td
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:280px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Oracle ERP Process Name
																</td>
																<td
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:280px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Process ID
																</td>
																<td
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:280px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Status
																</td>
															</tr>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #e6e6e6; text-align: left; width:280px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Load Request</th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_load_request_id
                                  || '</td>			
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_load_request_status
                                  || '</td>
															</tr>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #e6e6e6; text-align: left; width:200px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Import Request</strong></th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_import_request_id
                                  || '</td>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_import_request_status
                                  || '</td>
															</tr>                       
														</tbody>
													</table>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:280px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Total No Records Processed</th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_total_records_count
                                  || '</td>
															</tr>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:200px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Total No Records Failed in Pre-validation</strong></th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_pre_error_records_count
                                  || '</td>
															</tr>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:200px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Total No Records Failed in Oracle ERP Cloud</strong></th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_oracle_error_records_count
                                  || '</td>
															</tr>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:200px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Total No Records Imported Successfully</strong></th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_success_records_count
                                  || '</td>
															</tr>

														</tbody>
													</table><br>

												<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;"> *Note: The base load upload program triggers multiple child requests. Please check the status of these individually by logging into ERP Cloud </p>
											';
            ELSIF l_notification_type = 'FILE_IMPORT' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
													<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Source File Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Execution Status</strong></td>                    
													</tr>
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_file_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_integration_status
                                  || '</td>
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:280px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Total No Records Processed</th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_total_records_count
                                  || '</td>
															</tr>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:200px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Total No Records Failed in Pre-validation</strong></th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_pre_error_records_count
                                  || '</td>
															</tr>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:200px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Total No Records Failed in Oracle ERP Cloud</strong></th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_oracle_error_records_count
                                  || '</td>
															</tr>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:200px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Total No Records Imported Successfully</strong></th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_success_records_count
                                  || '</td>
															</tr>

														</tbody>
													</table><br>
											';
            ELSIF l_notification_type = 'OUTBOUND' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Execution Status</strong></td>                    
													</tr>
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_integration_status
                                  || '</td>
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:280px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	Extract File Name</th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_file_name
                                  || '</td>
															</tr>
															<tr>
																<th
																	style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:200px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
																	File Directory</strong></th>
																<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
																	valign="top">'
                                  || l_file_path
                                  || '</td>
															</tr>

														</tbody>
													</table><br>
													</body>
											</html>';
            ELSIF l_notification_type = 'INBOUND_REST' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Execution Status</strong></td>                    
													</tr>
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_integration_status
                                  || '</td>
													</tr>
													</tbody>
													</table><br>

													</body>
											</html>';
            ELSIF l_notification_type = 'NO_FILE_FOUND' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
														<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Code</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Message</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Details</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_code
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_message
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_details
                                  || '</td>	
													</tr>	
														</tbody>
													</table><br>
													';
			 ELSIF l_notification_type = 'NO_DATA_FOUND' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
														<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Code</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Message</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Details</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_code
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_message
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_details
                                  || '</td>	
													</tr>	
														</tbody>
													</table><br>
													';
            ELSIF l_notification_type = 'EMPTY_FILE' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:100px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:150px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:200px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Source File Name</strong></td>
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_file_name
                                  || '</td>
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
														<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Code</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Message</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Details</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_code
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_message
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_details
                                  || '</td>	
													</tr>	
														</tbody>
													</table><br>
													';
            ELSIF l_notification_type = 'DUPLICATE_FILE' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:100px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:150px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:200px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Source File Name</strong></td>
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_file_name
                                  || '</td>
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
														<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Code</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Message</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Details</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_code
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_message
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_details
                                  || '</td>	
													</tr>	
														</tbody>
													</table><br>
													';
            ELSIF l_notification_type = 'PRE_VALIDATION_FAILURE' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:100px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:150px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>';
                IF l_file_name IS NOT NULL THEN
                    l_message_body := l_message_body || '<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:150px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Source File Name</strong></td>';
                END IF;
                l_message_body := l_message_body
                                  || '<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>';

                IF l_file_name IS NOT NULL THEN
                    l_message_body := l_message_body
                                      || '<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; 				font-family:Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                      || l_file_name
                                      || '</td>';
                END IF;

                l_message_body := l_message_body
                                  || '</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
														<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Code</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Message</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Details</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_code
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_message
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_details
                                  || '</td>	
													</tr>	
														</tbody>
													</table><br>

													<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please find the details on Pre Validation Failure in the attached reconciliation report.</p>
													';

			ELSIF l_notification_type = 'ORACLE_FBDI_PROCESS_SUBMISSION_ERROR' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:100px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:150px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>														
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
														<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Code</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Message</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Details</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_code
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_message
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_details
                                  || '</td>	
													</tr>	
														</tbody>
													</table><br>
													';	

            ELSIF l_notification_type = 'FBDI_FILE_PREPARATION_ERROR' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:100px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:150px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>														
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
														<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Code</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Message</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Details</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_code
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_message
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_details
                                  || '</td>	
													</tr>	
														</tbody>
													</table><br>
													';
            ELSIF l_notification_type = 'VALIDATION_TRANSFORMATION_FAULT' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:100px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:150px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>														
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
														<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Code</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Message</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Details</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_code
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_message
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_details
                                  || '</td>	
													</tr>	
														</tbody>
													</table><br>
													';
            ELSIF l_notification_type = 'GLOBAL_FAULT' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:100px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:150px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>														
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
														<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Code</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Message</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Details</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_code
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_message
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_details
                                  || '</td>	
													</tr>	
														</tbody>
													</table><br>
													';
            ELSIF l_notification_type = 'GENERIC_FAULT' THEN
                l_message_body := '<html>
									<body>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Dear
											Receiver,</p>
										<p>  </p>
										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">Please
											find the execution details as follows.</p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
												cellpadding="2">
													<tbody>
													<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:100px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Id</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Interface Name</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:150px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															OIC Process ID</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_rice_id
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_interface_name
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_parent_process_id
                                  || '</td>														
													</tr>
													</tbody>
													</table><br>

										<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">File execution details: </p>

										<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
														cellpadding="2">
														<tbody>
														<tr>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Code</td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Message</strong></td>
														<td
															style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
															Error Details</strong></td>														
													<tr>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_code
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_message
                                  || '</td>
														<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
															valign="top">'
                                  || l_error_details
                                  || '</td>	
													</tr>	
														</tbody>
													</table><br>
													';
            END IF;

			-- Check if UCM URL is passed
            IF l_log_file_ucm_url IS NOT NULL THEN
                l_message_body := l_message_body
                                  || '<br>
									<table style="border-collapse: collapse; text-align: left; width: 700px;" border="1" cellspacing="0"
								cellpadding="2">
									<tbody>
									<tr>
										<td
											style="padding: 3px 10px; background-color: #BAD6F6; text-align: left; width:170px; font-size: 12px; font-weight: bold; font-family: Arial, Helvetica, sans-serif; border-left: 1px solid #BAD6F6;">
											Attachment URL</td>										                  
									</tr>
									<tr>
										<td style="padding: 3px 10px; border-left: 1px solid #BAD6F6; font-size: 12px; font-family: Arial, Helvetica, sans-serif; font-weight: normal;"
											valign="top">'
                                  || '<a href="'
                                  || l_log_file_ucm_url
                                  || '">Download Output, Logs and/or Recon Report</a>'
                                  || '</td>										
									</tr>
									</tbody>
									</table><br>';

            END IF;
			-- Append End of Message Body 
            l_message_body := l_message_body || '<p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">**Please donot reply to the mailbox. Any access related issues on UCM, Please reach out to IT Support or Administrator. </p>
																			 <br>
																			 <p style="font-size: 12px; font-weight: normal; font-family: Arial, Helvetica, sans-serif;">
                                        Thanks & Regards,</br>
                                        Kaseya IT Support Team</br></br>
                                        <img src = "data:image/jpeg;base64,iVBORw0KGgoAAAANSUhEUgAAAFIAAAAsCAYAAAD2BO8qAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsMAAA7DAcdvqGQAAA35SURBVGhD7VoJcFXVGb4ESN699727vTwSkshiqTvaWovWWtAyCtNRsFWEukwtECDJSwIE1KpttFVGirad4oKK1DpoQQWyb+woCLKIrFKQgCyyBsKSFXL6/f89N7yESGzHzijJN3Pm8c6/nP985z//OecFpR3tuCDiskp1JaWoX/eJC+6uqKrr29DQ0KtP9koDog6uRjtahZZaMFBPL1qhJOfXhd/ZIM6cPVu3uvxYpTO2sLzzmLxCPVw0SQ/nPqiF869Tst+Nlmbt8BCTWtJLTy+eEcgoru8Uni96PrFQ7DpaLQh/WbBLKKMLhX/cQhHIWorPBcKXmlcRlZyzXnl4znRl0NvDlFteuwxu2nLGZkeBwDR/RvHeQNYSgX8LZWSOeKFsB5N49FSt6P3UItFxdK7Q0wr4UxmRI/wZpeKKp5eLIW9sEpPnfyEWbT10DCWgDO0ZmN16TAhLDnDxQ08r7K2Hiwsp0/xj5wt/uEBEJeeK659ZIo5X1TGRz5dtF8pv5zJ5ROLV2QvFo+9vEku3HRG1dWdYh7D/RL1YVl4l3lx9WEx8f/OZnz63bKcvXPS6lpr7sJlZ3EMOefFBS8sbqYcLD1EW+tOLmEQNGRc1KldM/2AXk7Pz0CnRZXyx6AwCB7+4Ssxdt19US/I27Tshpi0tFw/NWCtuAPHdHi0Tamo+E66Myhe+9FKUAfgeuwDZW7TPj7IRnVJwpRz+IsCoNZ394cK/+jNLG7PQax2QjX2eXSpO1dSLujNnxWNzNot7Xv5YrN2NjQocqKwRUxftFP1fWC6M9EIuAUpyjugA8nEQMZGR/rhBL4CaGvjdKqGH88bJKL7jAIl6uOAVP2VKRkmTCaOfiZm2ZCeTdqK6TqzbfVycBqkffV4hUmauF0mPlDJxpNciaS02EIkDCv5zzZS3bRnJdxvamPwBTGAzEql1RFZdnb1IHDpRw0R6+MeHu0XXCSVCuf89oQyfJ3wp+SLQzPYrG0pGYAIWLVxYbIyb7cgwvvvQU/Mf4INF1kSvedk4qWgbk7dhT6Uo3nBQHDjukvrZlyfF86XbxU2TloHIPK6DRGikj3MNGUifOP394xcLPTVvrpk57+I6wY1xJY4ezl/Jd8GIyVN96/FYGWpgtTiJrUyHRxSIvezJBWLcrI2okceZUEL+p1+Koa+uFnZmERMaDdtIX9wo47FgemrB68qoPE0Of3EhMCbncmTkJ4EJIBMHAU2ctuzE9zYxUTOwlakOxqTmiU6jkX2QmRmF4u6XVomiDQdYh7Cm/JhInfmpiM/CvZMIRaaSrwAdYBmlZ/S0/KfkkBcv1PQ5SbiAFweyFovo1EIRGlckNu+vFGcbGsTPn/+QiYzMsBhsY9r6lH2Dpq4Uy3cclXQKsXlfpaCnZOz4EtEpfT7KRHGlFs59WA7VBjA8J6CFC17sMKZYJL+9hUn54N9H+PXyVSeye0+cJyxk6PjZG8X+4+4TkrDpYK0YPG3Nlk7DZ/WVI7QxjJg3+uNdJyuIjIx/bWSiiLTIk5n+Hfk9hg4cbHl6Or677iATiadhDlpP6bVtAgTcvPto1cqrnlkpotOanuhf1fAqElFh1MP0oqo/FWx9Ej5ipLu2jaRxKxxcXab40wpqqHZGXpHOuzdmlokArjY4nT9SRuW00a3cCvSUnNtAZqk/swRkLQJpeErS6U5tLAgkkjNKKvT0oj866UX0Y+83i1BIi7dNdZJtalNtW3sxZGo/kKImcAJqJuQvQe9vtqVOt23fLVL07UH24k5qasFgPb2wAO0oX2n4XV64X08vflVPyb1Wan7zsG39GhBTE7Q1EXR0YRn6/VLUCNvUsxxbF47Ugf5CXde7SPG3EvxTW1r+MD2cN9RIL/qe7P7/wXH8V4GYCsfSQJQuDEO/T4oYjqnea1tajSfHv1d16aLHSXE7PEgijzYSFUFkIKDeCOKOuDJNYGtvj42Npp/m2xQSeiRcnpSUMCEpKWlEfHx8SHY3RXMivYw0zZgelqVu9bY06uhB1MWfsFEz2LZtOo7aBz5+41hqhmWoKSB9YHy8v+VBFaWjYag3OKY2HPqZqLvJ0B8A/90gi3JVGtHBNLUfQXcE6QYD2kOW1blJretixlxKJcqy9N6Im+6ELf0NJtrxR19JeiFLuw7f+S1NJSrW8vfz/FMssZavb1ycopMc5DmJiV0zEpISJiUmJk7u2bPbSHSf7785kSDvrl69lBj0LaDvVDuRlafRP1iaNAFIS4d8G+wbgqxPxLvkO5a+0zbU0VKVYev6NQi4GPr1NKZnw+OY6mfBYDAgVREbJm5qefBfz/VZ+kVsteibEzIMrn2IbTLGJH/Uvy9kxJxXEy1LuxPyKtjX4rM86I++An3ZaPsoDneuMnZLO4sx1mJR+ne/rruV1C0hLfGSxKnIymwicsiQIR2l23NoTqSJmmgZ2gvSIQXeYNtNyYiEbWiLYx0/kU1EHMBnuWWipsqgqB8+f0W6waCaANlWDph8s422B9nwBRFkmeoXUOMLMiZxKWTb+BAkfVOrI9/oqyW/3Geo6x3HMbAbbpKxckMGjyAfkcC408iGGuY7oysykvzF4vAkW8j3ou22Lb3B88O8ONFXJfVI6pvUrWs6Wjihe8IPpcum8IhkIpBVWKE1+MSKMAk1+P57qdoicKL/E3pzbCNmYCAQ/X1MKjFoqD+G3VwKxg1cW+TqaiOZAJfAKrRRbgnxdafrVCyyBmq0tTsgk99iXbI31IV2wHcz+cY17EbYLXVJ8ZOvP5BvEL2icfFMNZ/6PFDpQQzlLKexDW0AuqOwcCWwexnj96M4EMMl+LwVsX/sLRZuMVPIB23xuLg43u4tohmRciXcT6zQ9u6K4pOqLYJIkP9sAh0nO7ZwBZNharsoAxzD93gEkXsTAkpQqjcBbztTqyRd6O0hX1LEMEIxvTw5PjejKwq1bzxlL/u2tMMgpPH9jK0/CP1cekDyFnSpaJ1pYVihGUxTvw36Z+X4S9DV+t+9I7c2EYjA9nqkotVj1Z6TqhcE30cNbSA+74PPe7G9RhJZHIylHgxpWjxIf9DLCpoYJrUUesP9IE66YSAzhpIdZ5eB7PP7r7B0vXdkQ2zbWW5pdUQaHVS4qp1i35QMtpYs3VG2vkL+OBZb/6Ps9hBDB58V0O6EbKhj+u+xTV8W/NSTPua/ETqdXdULoDmR2EZjsILzGzOT+nCSSfXzgPr3SwS6BLrHeDug5niNJ8V+1cNGyOhl2wq2mLrWmxQ1l1j1kGOrM4OB6MvJJyYykWXu2LUgvBLtJCZ1Et9P4N8nIHNrGelgAV07Ldfzi3jmch9va3W77DuJhfNqXBTmmUbxwK6a7RAz1UyvbjIHpkoZ3/p/cYkkkiZlICis8KXow+RcZ2jHLVwRpEkjcOccBrta3lI0KAJGRs9Cexm1803y605APUwZSzZxVItM7e+Y/C4a0z1M3Aa7rbGxWlfoZpEdx0SEuZ+yNfbTKV1Hn7bhG0q+saAj2M6NpcI0TTtI25T9c3aVkR4BBD3NC8+N9T9B31uI7SXEMRukn+FFNtVNUP/viTRN/xDqx53sfnw/w1npTnIHEcxGLnwIbENj4CDHNJXGP1tiEhZsy0kO/4dw7blaihgB1EfeTqaWg7GZLJ4ssgTba8i5CWqLuSbi7nd+06+lZhgG/6VPQ/mwLd8BsuOYbN8wXM8me9+pjJBebCBwGXbBMekfB6qaEQopfpIRyDctENn8z0RGvmxwYv2ZA5IyDLAoGFT4nheLE5q2GslB8unmz0YU7P6QneFJwD8FJ0XNQafnZvLDi2KpU9yY3IzA504iXeq2CszlDW9xQcIy+N7IsZvaPrp+kQ5dx7z5gsS1bBgB7KbxbE9zw6MEXRc8cBkXIlJRuvswaDE5PEem+hpJcOIlIcgTUlZHBwS6+VVCLwzKVpIFyQ6rb9udr6HrheNod3iLAXSAnz4Y/4CXkdhe9MenKGTqMpdYIgHXFFzOqZ+tkCE0Bu0CkMU7yAMOmV8gzrPkj2P2/FrqdKlCGYfLeeOWPoAYbpSiKJSC/rA7THNluaF+jv7W/6IoiWz80aIpkbR6qGmWtoNXiHWYmAySIehcKtAUMCZVBZLLQEQ+Jn7SnYR6hIiE7BSdvLYdMwA+6NWwnbY09Aq9sTlo2MXiOUi+ccL3Rc07zhlNMiw2fJah7118rob9aX4ImL5HSN8DXdDhZ4tcYDdmlA48fW+XKkocnoUY+7Og48qhfxh+czHGAve7VsMxwwceJ3scR2n990teWUt1n2AgBdv5ASlqBMikVeIXBROJZhi++zjDLG05DUirTvbsw9Kq+b0NUmmyFByuGLikq4M8Ha/FypWHzR74HCaHZNB7HT7Wc2zSP52obEuLbmmV3qspEhiHSxKNywtkqeuU7k23p2PE3A7ydkXGTr5BIMXxa/je6X5Xq72ScEHQAYFJPISJjsHkU5sdKI1wDO0OyNMwUDICzUC7i/phb6FvJL5Ph5/3MPCzKOoyq/z9PBs6CKiO0snqGPpkZOs7CHYObKYR6Ri3xT9A8aEFgmEzBcTQafo+21hqJsa5Xqo1AV5WN6AmVrtE8sI+IUVNQGPSb62IbyZ0Z+Em8Zg3f7rWIanSiJuIUtS20EXX47CgRzjTkLW066SoHV8D/GNHEp5/yK5sykau+fJi3o6vh06og2/gJC5G6VjBWxoNJNajvPxM6rSjNdALBkQeDAXlYYRMBJkNOHEflyrt+DoIhUJ+1MRJyMCF2M7LkJWzcVjcLcXtaEebgKL8B0AE5hU94YObAAAAAElFTkSuQmCC">
                                        </br>
                                    </p>
																	 </body>
																 </html>';
            x_message_body := l_message_body;
        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'UNEXPECTED_EXPECTION_ERROR';
                l_status_message := 'Error in preparing Message body for Notification. Error Details - ' || sqlerrm;
                RAISE l_custom_proc_exp;
        END;

        x_status := 'SUCCESS';
        x_status_message := 'Succesful Execution.';
    EXCEPTION
        WHEN l_custom_proc_exp THEN
            x_status := l_status;
            x_status_message := l_status_message;
            x_status_message := x_status_message
                                || ';  Error Details - '
                                || sqlerrm;
        WHEN OTHERS THEN
            x_status := 'ERROR';
            x_status_message := 'Unexpected exception in Preparing Notification. Please contact system admin.';
            x_status_message := x_status_message
                                || ';  Error Details - '
                                || sqlerrm;
    END notification_prc;


/*****************************************************************
	OBJECT NAME: DB objects Log Procedure
	DESCRIPTION: Procedure for Logging related to DB objects
VERSION 	NAME              	DATE           			VERSION-DESCRIPTION
----------------------------------------------------------------------------
<1.0>		Kunjesh   			18-Feb-2025    			1.0- INITIAL DRAFT
******************************************************************/
    PROCEDURE DB_LOGGING_DETAILS_PRC (
	    p_log_flag            IN VARCHAR2, 
        p_interface_rice_id   IN VARCHAR2,
        p_integration_name    IN VARCHAR2,
        p_parent_process_id   IN VARCHAR2,
        p_current_process_id  IN VARCHAR2,
        p_db_object_name      IN VARCHAR2,
        p_message             IN VARCHAR2,
		p_ATTRIBUTE1          IN VARCHAR2, 
		p_ATTRIBUTE2          IN VARCHAR2,
		p_ATTRIBUTE3          IN VARCHAR2, 
		p_ATTRIBUTE4          IN VARCHAR2,
		p_ATTRIBUTE5          IN VARCHAR2
    )
	AS 
	BEGIN 
	IF p_log_flag ='Y' THEN 

	INSERT INTO XXINT_XX_I010_DB_EXECUTION_LOG(
	interface_rice_id   ,
	integration_name    ,
	parent_process_id   ,
	current_process_id  ,
	db_object_name      ,
	message             ,
	ATTRIBUTE1          ,
	ATTRIBUTE2          ,
	ATTRIBUTE3          ,
	ATTRIBUTE4          ,
	ATTRIBUTE5          
	) 
	VALUES 
	(
	p_interface_rice_id  ,
	p_integration_name   ,
	p_parent_process_id  ,
	p_current_process_id ,
	p_db_object_name     ,
	p_message            ,
    p_ATTRIBUTE1         , 
    p_ATTRIBUTE2         ,
    p_ATTRIBUTE3         ,
    p_ATTRIBUTE4         ,
	p_ATTRIBUTE5
	);
	COMMIT;
	END IF;
    EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
    END;

END XXINT_XX_I010_COMMON_LOGGING_PKG;

/