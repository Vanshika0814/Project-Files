		/**************************************************************
    NAME              :     xxood.XXOOD_OIC_AUDITLOGS_V
    PURPOSE           :     View to Store OIC Audit Logs
	
	 Developer          Date         Version     Comments and changes made
	 -------------   ------       ----------  -------------------------------------------
     Narashima        26-Sept-2025       1.0             Initial Development
	 Vaishnavi	     07-oct-2025        1.1				Changes to invoke by column
    **************************************************************/
CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXOOD"."XXOOD_OIC_AUDITLOGS_V" (
    "ID",
    "CURRENT_PROCESS_ID",
    "INTEGRATION_NAME",
    "STATUS",
    "ERROR_CODE",
    "ERROR_SUMMARY",
    "BUSINESS_INDETIFIER_KEY",
    "BUSINESS_INDETIFIER_VALUE",
    "PROCESS_START_TIME",
    "PROCESS_END_TIME",
    "INTERFACE_RICE_NAME",
    "INTEGRATION_VERSION",
    "CREATION_DATE",
    "CREATED_BY",
    "LAST_UPDATED_BY",
    "LAST_UPDATE_DATE",
    "INVOKED_BY",
    "TRACK"
) DEFAULT COLLATION "USING_NLS_COMP" AS
    SELECT
        ROWNUM                                               AS id,
        a.current_process_id,
        a.integration_name,
        CASE
            WHEN a.status = 'SUBMITTED' THEN
                'SUCCESS'
            ELSE
                upper(a.status)
        END                                                  AS status,
        b.error_code,
      --  b.error_details,
        b.error_summary,
        a.business_indetifier_key,
        a.business_indetifier_value,
        a.process_start_time,
        a.process_end_time,
        a.interface_rice_name,
        a.integration_version,
        TO_DATE(a.creation_date, 'YYYY-MM-DD HH24:MI:SS')    AS creation_date,
        a.created_by,
        a.last_updated_by,
        TO_DATE(a.last_update_date, 'YYYY-MM-DD HH24:MI:SS') AS last_update_date,
        a.invoked_by,
        CASE
            WHEN substr(a.interface_rice_id, 0, 2) IN ( 'AP', 'GL', 'CM', 'IC' ) THEN
                'RTR'
            WHEN substr(a.interface_rice_id, 0, 6) IN ( 'KSY_GL', 'KSY_AP', 'KSY_CM', 'KSY_IC' ) THEN
                'RTR'
            WHEN substr(a.interface_rice_id, 0, 6) IN ( 'KSY_PO' ) THEN
                'PTP'
            WHEN substr(a.interface_rice_id, 0, 3) IN ( 'INT' ) THEN
                'HTR'
            WHEN substr(a.interface_rice_id, 0, 6) IN ( 'KSY_OM', 'KSY_AR' ) THEN
                'OTC'
            WHEN substr(a.interface_rice_id, 0, 2) IN ( 'OM', 'AR' ) THEN
                'OTC'
            WHEN substr(a.interface_rice_id, 0, 3) IN ( 'PDH', 'MFG' ) THEN
                'PTM'
            WHEN a.interface_rice_id IN ( 'XX_I022', 'XX_I020' ) THEN
                'INTERNAL'
            WHEN a.integration_name LIKE '%XX%' THEN
                'COMMON'
            ELSE
                'NA'
        END                                                  AS track
    FROM
        xxint.xxint_xx_i010_audit_log         a
        LEFT JOIN xxint.xxint_xx_i010_error_details_log b ON a.current_process_id = b.current_process_id;

GRANT DELETE ON "XXOOD"."XXOOD_OIC_AUDITLOGS_V" TO "KSY_ATP_ADMIN_USR";

GRANT INSERT ON "XXOOD"."XXOOD_OIC_AUDITLOGS_V" TO "KSY_ATP_ADMIN_USR";

GRANT SELECT ON "XXOOD"."XXOOD_OIC_AUDITLOGS_V" TO "KSY_ATP_ADMIN_USR";

GRANT UPDATE ON "XXOOD"."XXOOD_OIC_AUDITLOGS_V" TO "KSY_ATP_ADMIN_USR";

GRANT INSERT ON "XXOOD"."XXOOD_OIC_AUDITLOGS_V" TO "KSY_ATP_DEVELOPER_USR";

GRANT SELECT ON "XXOOD"."XXOOD_OIC_AUDITLOGS_V" TO "KSY_ATP_DEVELOPER_USR";

GRANT UPDATE ON "XXOOD"."XXOOD_OIC_AUDITLOGS_V" TO "KSY_ATP_DEVELOPER_USR";

GRANT SELECT ON "XXOOD"."XXOOD_OIC_AUDITLOGS_V" TO "KSY_ATP_VIEWER_USR";