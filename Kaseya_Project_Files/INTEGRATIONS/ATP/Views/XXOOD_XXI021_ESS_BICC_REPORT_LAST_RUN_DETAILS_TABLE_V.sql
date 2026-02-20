/********************************************************************************************
OBJECT NAME: XXOOD_XXI021_ESS_BICC_REPORT_LAST_RUN_DETAILS_TABLE_V
DESCRIPTION: This view is to store the last tun time of the integration
Version 	Name              			Date           		Version-Description
---------------------------------------------------------------------------
<1.0>	 Narasimha Ch  			10/21/2025 	    	1.0-Initial Draft
**********************************************************************************************/


CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXOOD"."XXOOD_XXI021_ESS_BICC_REPORT_LAST_RUN_DETAILS_TABLE_V" (
    "TYPE_OF_REPORT",
    "SUCCESSFULL_RUN_TIME",
    "IS_OLDER_THAN_3_MIN"
) DEFAULT COLLATION "USING_NLS_COMP" AS
    SELECT
        type_of_report,
        to_char(successfull_run_time, 'MM-DD-YYYY HH24:MI:SS') AS successfull_run_time,
        CASE
            WHEN current_timestamp - successfull_run_time > numtodsinterval(3, 'MINUTE') THEN
                'Y'
            ELSE
                'N'
        END                                                    AS is_older_than_3_min
    FROM
        xxood_xxi021_ess_bicc_report_last_run_details_table
    WHERE
        type_of_report = 'DETAIL';

GRANT DELETE ON "XXOOD"."XXOOD_XXI021_ESS_BICC_REPORT_LAST_RUN_DETAILS_TABLE_V" TO "KSY_ATP_ADMIN_USR";

GRANT INSERT ON "XXOOD"."XXOOD_XXI021_ESS_BICC_REPORT_LAST_RUN_DETAILS_TABLE_V" TO "KSY_ATP_ADMIN_USR";

GRANT SELECT ON "XXOOD"."XXOOD_XXI021_ESS_BICC_REPORT_LAST_RUN_DETAILS_TABLE_V" TO "KSY_ATP_ADMIN_USR";

GRANT UPDATE ON "XXOOD"."XXOOD_XXI021_ESS_BICC_REPORT_LAST_RUN_DETAILS_TABLE_V" TO "KSY_ATP_ADMIN_USR";

GRANT INSERT ON "XXOOD"."XXOOD_XXI021_ESS_BICC_REPORT_LAST_RUN_DETAILS_TABLE_V" TO "KSY_ATP_DEVELOPER_USR";

GRANT SELECT ON "XXOOD"."XXOOD_XXI021_ESS_BICC_REPORT_LAST_RUN_DETAILS_TABLE_V" TO "KSY_ATP_DEVELOPER_USR";

GRANT UPDATE ON "XXOOD"."XXOOD_XXI021_ESS_BICC_REPORT_LAST_RUN_DETAILS_TABLE_V" TO "KSY_ATP_DEVELOPER_USR";

GRANT SELECT ON "XXOOD"."XXOOD_XXI021_ESS_BICC_REPORT_LAST_RUN_DETAILS_TABLE_V" TO "KSY_ATP_VIEWER_USR";