--------------------------------------------------------
--  DDL for Package XXINT_XX_I010_COMMON_LOGGING_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "XXINT"."XXINT_XX_I010_COMMON_LOGGING_PKG" IS
/********************************************************************************************
OBJECT NAME: XXINT_XX_I010_COMMON_LOGGING_PKG
DESCRIPTION: Package specification for Common framework (Logging)
VERSION 	NAME              	DATE           			VERSION-DESCRIPTION
----------------------------------------------------------------------------
<1.0>		Kunjesh   			18-Feb-2025    			1.0- INITIAL DRAFT
<1.1>		Devishi				28-Oct-2025				1.1- Changes related to case number: 01098700/BO-163
******************************************************************/
	-- For Common Logger
    TYPE batch_details_type IS RECORD (
        batch_identifier     VARCHAR2(200),
        batch_status         VARCHAR2(200),
        batch_status_message VARCHAR2(4000),
        source_file_name     VARCHAR2(400),
        source_file_checksum VARCHAR2(400),
        batch_row_count      NUMBER,
        batch_success_count  NUMBER,
        batch_error_count    NUMBER
    );
    TYPE batch_details_tbl_type IS
        TABLE OF batch_details_type;

	-- For Common Logger
    TYPE error_line_details_type IS RECORD (
        error_identifier    VARCHAR2(200),
        error_line_location VARCHAR2(200),
        error_line_code     VARCHAR2(400),
        error_line_details  CLOB,
        error_line_summary  VARCHAR2(4000)
    );
    TYPE error_line_details_tbl_type IS
        TABLE OF error_line_details_type;

	-- For Common Logger
    TYPE logger_type IS RECORD (
        log_flag                   VARCHAR2(20),
        notify_flag                VARCHAR2(20),
        interface_rice_id          VARCHAR2(200),
        interface_name             VARCHAR2(1000),
        source                     VARCHAR2(200),
        target                     VARCHAR2(200),
        parent_process_id          VARCHAR2(200),
        current_process_id         VARCHAR2(200),
        status                     VARCHAR2(200),
        integration_name           VARCHAR2(1000),
        integration_version        VARCHAR2(200),
        invoked_by                 VARCHAR2(400),
        instance                   VARCHAR2(200),
        process_start_time         VARCHAR2(200),
        process_end_time           VARCHAR2(200),
        business_identifier_key    VARCHAR2(1000),
        business_identifier_value  VARCHAR2(4000),
        stage                      VARCHAR2(200),
        source_file_name           VARCHAR2(4000), ---<Code Change 1.1>
        source_file_path           VARCHAR2(1000),
        file_checksum              VARCHAR2(1000),
        request_payload            CLOB,
        response_payload           CLOB,
        store_activity_stream_flag VARCHAR2(20),
        activity_stream            CLOB,
        oracle_erp_process_name    VARCHAR2(200),
        oracle_erp_process_id      VARCHAR2(200),
        is_fbdi_process            VARCHAR2(20),
        enabled_callback           VARCHAR2(20),
        is_final_process           VARCHAR2(20),
        ucm_upload_request_id      VARCHAR2(200),
        ucm_upload_status          VARCHAR2(200),
        load_request_id            VARCHAR2(200),
        load_request_status        VARCHAR2(200),
        import_request_id          VARCHAR2(200),
        import_request_status      VARCHAR2(200),
        callback_received          VARCHAR2(20),
        file_count                 NUMBER,
        total_batch_count          NUMBER,
        total_row_count            NUMBER,
        total_success_count        NUMBER,
        total_error_count          NUMBER,
        header_error_code          VARCHAR2(4000),
        header_error_summary       VARCHAR2(4000),
        header_error_details       CLOB,
        batch_details              batch_details_tbl_type,
        error_line_details         error_line_details_tbl_type
    );

	-- For Common Notification
    TYPE notify_type IS RECORD (
        interface_rice_id          VARCHAR2(200),
        interface_name             VARCHAR2(1000),
        parent_process_id          VARCHAR2(200),
        current_process_id         VARCHAR2(200),
        status                     VARCHAR2(200),
        instance                   VARCHAR2(200),
        oic_integration_name       VARCHAR2(1000),
        business_identifier_key    VARCHAR2(1000),
        business_identifier_value  VARCHAR2(4000),
        stage                      VARCHAR2(200),
        file_name                  VARCHAR2(400),
        file_path                  VARCHAR2(1000),
        error_code                 VARCHAR2(1000),
        error_message              VARCHAR2(5000),
        error_details              VARCHAR2(5000),
        oracle_erp_process_name    VARCHAR2(200),
        oracle_erp_process_id      VARCHAR2(200),
        ucm_upload_request_id      VARCHAR2(200),
        ucm_upload_request_status  VARCHAR2(200),
        load_request_id            VARCHAR2(200),
        load_request_status        VARCHAR2(200),
        import_request_id          VARCHAR2(200),
        import_request_status      VARCHAR2(200),
        total_records_count        NUMBER,
        success_records_count      NUMBER,
        oracle_error_records_count NUMBER,
        pre_error_records_count    NUMBER,
        has_attachement            VARCHAR2(20),
        log_file_ucm_url           VARCHAR2(1000),
        notification_type          VARCHAR2(100)
    );

/*****************************************************************
	OBJECT NAME: Schedule Archive Procedure
	DESCRIPTION: Wrapper Procedure for scheduling archive_prc
	Version 	Name              	    Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		CHANDRA MOULI GUPTA   	09-SEP-2025	        1.0- Initial Draft
	******************************************************************/
PROCEDURE schedule_archive_prc( 
    p_oic_instance_id IN varchar2
);


/*****************************************************************
	OBJECT NAME: Archive Procedure
	DESCRIPTION: Procedure for archiving Data from ATP DB Tables
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		KUNJESH SINGH   	18-FEB-2025	    1.0- Initial Draft
	******************************************************************/

PROCEDURE archive_prc (
        p_oic_instance_id   IN VARCHAR2
        --p_limit				IN VARCHAR2,
        --x_status            OUT VARCHAR2,
        --x_status_message    OUT VARCHAR2
    );

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
);

	/*****************************************************************
	OBJECT NAME: Common Logging Procedure
	DESCRIPTION: Main Procedure for Common Audit and Logging
VERSION 	NAME              	DATE           			VERSION-DESCRIPTION
----------------------------------------------------------------------------
<1.0>		Kunjesh   			18-Feb-2025    			1.0- INITIAL DRAFT
******************************************************************/
    PROCEDURE logger_prc (
        p_operation      IN VARCHAR2,
        p_logger_details IN logger_type,
        x_status         OUT VARCHAR2,
        x_status_message OUT VARCHAR2
    );

	/*****************************************************************
	OBJECT NAME: Common Notification Procedure
	DESCRIPTION: Procedure for Common notification 
VERSION 	NAME              	DATE           			VERSION-DESCRIPTION
----------------------------------------------------------------------------
<1.0>		Kunjesh   			18-Feb-2025    			1.0- INITIAL DRAFT
******************************************************************/
    PROCEDURE notification_prc (
        p_notification_details notify_type,
        x_email_address        OUT VARCHAR2,
        x_subject_line         OUT VARCHAR2,
        x_message_body         OUT CLOB,
        x_status               OUT VARCHAR2,
        x_status_message       OUT VARCHAR2
    );

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

    );
END XXINT_XX_I010_COMMON_LOGGING_PKG;

/