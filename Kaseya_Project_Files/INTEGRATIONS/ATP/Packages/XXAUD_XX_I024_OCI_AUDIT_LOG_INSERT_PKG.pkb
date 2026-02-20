/**************************************************************
    NAME              :     xxaud_xx_i024_oci_audit_log_insert_pkg
    PURPOSE           :     Package Body
	
	 Developer          Date         Version     Comments and changes made
	 -------------      ------      ----------  -------------------------------------------
	 Vaishnavi	       11/18/2025      1.0		   Intial Development
    **************************************************************/
create or replace PACKAGE BODY xxaud.xxaud_xx_i024_oci_audit_log_insert_pkg AS

    PROCEDURE load_logs_from_payload (
        p_rows OUT PLS_INTEGER
    ) IS
    BEGIN
        MERGE INTO xxaud_xx_i024_oci_event_audit_logs tgt
        USING (
            SELECT
                jt.event_type,
                jt.cloud_events_version,
                jt.event_type_version,
                jt.source,
                jt.event_id,
                jt.event_time,
                jt.compartment_name,
                jt.resource_name,
                jt.resource_id,
                JSON_ARRAY(jt.identity_audit FORMAT JSON RETURNING json)     AS identity_audit,
                JSON_ARRAY(jt.request FORMAT JSON RETURNING json)            AS request,
                JSON_ARRAY(jt.response FORMAT JSON RETURNING json)           AS response,
                JSON_ARRAY(jt.state_change FORMAT JSON RETURNING json)       AS state_change,
                JSON_ARRAY(jt.additional_details FORMAT JSON RETURNING json) AS additional_details
            FROM
                xxaud_xx_i024_oci_auditlogs_stg s,
                JSON_TABLE ( s.audit_logs, '$[*]'
                        COLUMNS (
                            event_type VARCHAR2 ( 1000 ) PATH '$.eventType',
                            cloud_events_version VARCHAR2 ( 1000 ) PATH '$.cloudEventsVersion',
                            event_type_version VARCHAR2 ( 1000 ) PATH '$.eventTypeVersion',
                            source VARCHAR2 ( 1000 ) PATH '$.source',
                            event_id VARCHAR2 ( 1000 ) PATH '$.eventId',
                            event_time DATE PATH '$.eventTime',
                            compartment_name VARCHAR2 ( 1000 ) PATH '$.data.compartmentName',
                            resource_name VARCHAR2 ( 1000 ) PATH '$.data.resourceName',
                            resource_id VARCHAR2 ( 1000 ) PATH '$.data.resourceId',
                            identity_audit CLOB FORMAT JSON PATH '$.data.identity' NULL ON ERROR,
                            request CLOB FORMAT JSON PATH '$.data.request' NULL ON ERROR,
                            response CLOB FORMAT JSON PATH '$.data.response' NULL ON ERROR,
                            state_change CLOB FORMAT JSON PATH '$.data.stateChange' NULL ON ERROR,
                            additional_details CLOB FORMAT JSON PATH '$.data.additionalDetails' NULL ON ERROR
                        )
                    )
                jt
        ) src ON ( tgt.event_id = src.event_id )
        WHEN MATCHED THEN UPDATE
        SET tgt.event_type = src.event_type,
            tgt.cloud_events_version = src.cloud_events_version,
            tgt.event_type_version = src.event_type_version,
            tgt.source = src.source,
            tgt.event_time = src.event_time,
            tgt.compartment_name = src.compartment_name,
            tgt.resource_name = src.resource_name,
            tgt.resource_id = src.resource_id,
            tgt.identity_audit = src.identity_audit,
            tgt.request = src.request,
            tgt.response = src.response,
            tgt.state_change = src.state_change,
            tgt.additional_details = src.additional_details
        WHEN NOT MATCHED THEN
        INSERT (
            event_type,
            cloud_events_version,
            event_type_version,
            source,
            event_id,
            event_time,
            compartment_name,
            resource_name,
            resource_id,
            identity_audit,
            request,
            response,
            state_change,
            additional_details )
        VALUES
            ( src.event_type,
              src.cloud_events_version,
              src.event_type_version,
              src.source,
              src.event_id,
              src.event_time,
              src.compartment_name,
              src.resource_name,
              src.resource_id,
              src.identity_audit,
              src.request,
              src.response,
              src.state_change,
              src.additional_details );

        p_rows := SQL%rowcount;
	
	--Delete from XXAUD_XX_I024_OCI_AUDITLOGS_STG;

        COMMIT;

  --  WHERE jt.action IS NOT NULL;  -- skip array elements missing $.data.identity

    EXCEPTION
        WHEN OTHERS THEN
            raise_application_error(-20001, 'oci_audit_ingest.load_from_payload failed: ' || sqlerrm);
    END load_logs_from_payload;

END xxaud_xx_i024_oci_audit_log_insert_pkg;