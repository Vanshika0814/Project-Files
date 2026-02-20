create or replace PACKAGE       xxood.xxood_xxi021_job_summary_pkg IS
    FUNCTION ess_summary_fnc (
        p_days_cnt IN NUMBER,
         p_job_type IN VARCHAR2,
		p_current_time IN VARCHAR2
       
    ) RETURN VARCHAR2;

END xxood_xxi021_job_summary_pkg;