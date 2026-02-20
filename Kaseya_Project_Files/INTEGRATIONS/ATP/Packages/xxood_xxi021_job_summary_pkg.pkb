create or replace PACKAGE BODY  xxood.xxood_xxi021_job_summary_pkg IS
		/**************************************************************
    NAME              :     xxood.xxood_xxi021_job_summary_pkg
    PURPOSE           :     Package body to returb summary count of ESS, BICC, OIC

	 Developer          Date         Version     Comments and changes made
	 -------------   ------       ----------  -------------------------------------------
	 Siva	 		 24-Sept-2025  	    1.0         	Initial Development
     Narshima        26-Sept-2025       1.1             Added Logic for OIC
	 Vaishnavi		 07-Oct - 2025		1.2				Added Logic for Last Day
	 Vaishnavi		 11-Nov-2025		1.3				Added Logic for User Time Zone Settings	
    **************************************************************/

    FUNCTION ess_summary_fnc (
        p_days_cnt NUMBER,
        p_job_type VARCHAR2,
		p_current_time VARCHAR2
    ) RETURN VARCHAR2 AS

        l_tcnt   NUMBER;
        l_scnt   NUMBER;
        l_ccnt   NUMBER;
        l_ecnt   NUMBER;
        l_wcnt   NUMBER;
        l_fcnt   NUMBER;
        l_wrcnt  NUMBER;
        l_inpcnt NUMBER;
        l_cc_msg VARCHAR2(100);
		l_start_time DATE;
		l_end_time DATE;
    BEGIN

	SELECT
    TO_DATE(to_char(utc_time, 'YYYY-MM-DD HH24:MI:SS'),
            'YYYY-MM-DD HH24:MI:SS') - p_days_cnt into l_start_time
	FROM
    (
        SELECT
            TO_TIMESTAMP_TZ(p_current_time, 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM') AT TIME ZONE 'UTC' AS utc_time
        FROM
            dual
    );

	SELECT
    TO_DATE(to_char(utc_time, 'YYYY-MM-DD HH24:MI:SS'),
            'YYYY-MM-DD HH24:MI:SS') into l_end_time
	FROM
    (
        SELECT
            TO_TIMESTAMP_TZ(p_current_time, 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM') AT TIME ZONE 'UTC' AS utc_time
        FROM
            dual
    );
        IF p_job_type = 'OIC' THEN
		 ---------------------------NEW OIC----------------------------------
		 If p_days_cnt = 1 THEN 
		 SELECT
                COUNT(1)
            INTO l_tcnt
            FROM
                xxood_oic_auditlogs_v
            WHERE
                    1 = 1
                AND last_update_date >= l_start_time  AND last_update_date < l_end_time;

            SELECT
                COUNT(1)
            INTO l_scnt
            FROM
                xxood_oic_auditlogs_v
            WHERE
                    status = 'SUCCESS'
                  AND last_update_date >= l_start_time  AND last_update_date < l_end_time;

            SELECT
                COUNT(1)
            INTO l_ecnt
            FROM
                xxood_oic_auditlogs_v
            WHERE
                    status = 'ERROR'
                  AND last_update_date >= l_start_time  AND last_update_date < l_end_time;

            SELECT
                COUNT(1)
            INTO l_inpcnt
            FROM
                xxood_oic_auditlogs_v
            WHERE
                    status = 'START'
                  AND last_update_date >= l_start_time  AND last_update_date < l_end_time;
		 ELSE
            SELECT
                COUNT(1)
            INTO l_tcnt
            FROM
                xxood_oic_auditlogs_v
            WHERE
                    1 = 1
                  AND last_update_date >= l_start_time ;

            SELECT
                COUNT(1)
            INTO l_scnt
            FROM
                xxood_oic_auditlogs_v
            WHERE
                    status = 'SUCCESS'
                 AND last_update_date >= l_start_time ;

            SELECT
                COUNT(1)
            INTO l_ecnt
            FROM
                xxood_oic_auditlogs_v
            WHERE
                    status = 'ERROR'
                 AND last_update_date >= l_start_time ;

            SELECT
                COUNT(1)
            INTO l_inpcnt
            FROM
                xxood_oic_auditlogs_v
            WHERE
                    status = 'START'
                AND last_update_date >= l_start_time ;
			END IF;
            RETURN 'tcnt-'
                   || ( l_scnt + l_ecnt + l_inpcnt )
                   || ':'
                   || 'scnt-'
                   || l_scnt
                   || ':'
                   || 'ecnt-'
                   || l_ecnt
                   || ':'
                   || 'inpcnt-'
                   || l_inpcnt;


		 ------------------------------------



        ELSE

		if p_days_cnt = 1 then 

		SELECT
                COUNT(1)
            INTO l_tcnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    1 = 1
                AND job_type = p_job_type
                AND nvl(process_end, process_start) >= l_start_time AND NVL(process_end,process_start) < l_end_time ;

            SELECT
                COUNT(1)
            INTO l_scnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'succeeded'
                AND job_type = p_job_type
                AND nvl(process_end, process_start) >= l_start_time AND NVL(process_end,process_start) < l_end_time ;

            SELECT
                COUNT(1)
            INTO l_ccnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'cancelled'
                AND job_type = p_job_type
               AND nvl(process_end, process_start) >= l_start_time AND NVL(process_end,process_start) < l_end_time ;

            SELECT
                COUNT(1)
            INTO l_ecnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'error'
                AND job_type = p_job_type
                AND nvl(process_end, process_start) >= l_start_time AND NVL(process_end,process_start) < l_end_time ;

            SELECT
                COUNT(1)
            INTO l_wcnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'wait'
                AND job_type = p_job_type
               AND nvl(process_end, process_start) >= l_start_time AND NVL(process_end,process_start) < l_end_time ;

            SELECT
                COUNT(1)
            INTO l_fcnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'finished'
                AND job_type = p_job_type
                AND nvl(process_end, process_start) >= l_start_time AND NVL(process_end,process_start) < l_end_time ;

            SELECT
                COUNT(1)
            INTO l_wrcnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'warning'
                AND job_type = p_job_type
                AND nvl(process_end, process_start) >= l_start_time AND NVL(process_end,process_start) < l_end_time ;

		else
            SELECT
                COUNT(1)
            INTO l_tcnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    1 = 1
                AND job_type = p_job_type
                AND nvl(process_end, process_start) >= l_start_time;

            SELECT
                COUNT(1)
            INTO l_scnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'succeeded'
                AND job_type = p_job_type
                AND nvl(process_end, process_start) >= l_start_time;

            SELECT
                COUNT(1)
            INTO l_ccnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'cancelled'
                AND job_type = p_job_type
               AND nvl(process_end, process_start) >= l_start_time;

            SELECT
                COUNT(1)
            INTO l_ecnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'error'
                AND job_type = p_job_type
                AND nvl(process_end, process_start) >= l_start_time;

            SELECT
                COUNT(1)
            INTO l_wcnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'wait'
                AND job_type = p_job_type
                AND nvl(process_end, process_start) >= l_start_time;

            SELECT
                COUNT(1)
            INTO l_fcnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'finished'
                AND job_type = p_job_type
                AND nvl(process_end, process_start) >= l_start_time;

            SELECT
                COUNT(1)
            INTO l_wrcnt
            FROM
                xxood_xxi021_ess_bicc_detail_table
            WHERE
                    lower(status) = 'warning'
                AND job_type = p_job_type
                AND nvl(process_end, process_start) >= l_start_time;
		end if;
            RETURN 'tcnt-'
                   || l_tcnt
                   || ':'
                   || 'scnt-'
                   || l_scnt
                   || ':'
                   || 'ccnt-'
                   || l_ccnt
                   || ':'
                   || 'ecnt-'
                   || l_ecnt
                   || ':'
                   || 'wcnt-'
                   || l_wcnt
                   || ':'
                   || 'fcnt-'
                   || l_fcnt
                   || ':'
                   || 'wrcnt-'
                   || l_wrcnt;

        END IF;
    END;

END xxood_xxi021_job_summary_pkg;