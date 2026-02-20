create or replace PACKAGE BODY       XXINT.xxint_i130_emp_expense_acc_update_pkg IS
/********************************************************************************************
OBJECT NAME: XXINT_I130_EMP_EXPENSE_ACC_UPDATE_PKG
DESCRIPTION: Package specification for XXINT_I130_EMP_EXPENSE_ACC_UPDATE_PKG to update default expense account of an employee on change of location, department, comapny and division
Version 	Name              			Date           		Version-Description
---------------------------------------------------------------------------
<1.0>		Vaishnavi Kattula  			07/22/2025 	    	1.0-Initial Draft
**********************************************************************************************/
    PROCEDURE validate_emp_expense_account (
        p_current_instance_id  IN VARCHAR2,
        p_batch_limit          IN VARCHAR2,
        p_db_log               IN VARCHAR2,
        p_emp_records          IN xxint_hcm_i130_employee_default_expense_account_stg_typ,
        x_new_expense_record   OUT xxint_hcm_i130_employee_default_expense_account_stg_typ,
        x_return_records_count OUT NUMBER,
        x_status_code          OUT VARCHAR2,
        x_status_message       OUT VARCHAR2
    ) IS
    BEGIN
        DELETE FROM xxint_i130_employee_default_expense_account_stg;

        COMMIT;
        FORALL i IN p_emp_records.first..p_emp_records.last
            INSERT INTO xxint.xxint_i130_employee_default_expense_account_stg (
                assignment_id,
                person_id,
                person_number,
                last_name,
                first_name,
                assignment_number,
                assignment_name,
                assignement_status_type,
                work_terms_assignment_id,
                period_of_service_id,
                action_code,
                legal_employer_name,
                effective_start_date,
                primary_flag,
                primary_assignment_flag,
                worker_type,
                worker_number,
                manager_name,
                business_unit_name,
                department_name,
                job_code,
                job_name,
                location_code,
                location_name,
                grade_code,
                grade_name,
                division,
                effective_sequence,
                default_expense_account,
                new_deafault_expense_account,
                company,
                location_segment,
                division_segment,
                cost_center,
                created_by,
                last_updated_by,
                created_date,
                last_update_date
            ) VALUES ( p_emp_records(i).assignment_id,
                       p_emp_records(i).person_id,
                       p_emp_records(i).person_number,
                       p_emp_records(i).last_name,
                       p_emp_records(i).first_name,
                       p_emp_records(i).assignment_number,
                       p_emp_records(i).assignment_name,
                       p_emp_records(i).assignement_status_type,
                       p_emp_records(i).work_terms_assignment_id,
                       p_emp_records(i).period_of_service_id,
                       p_emp_records(i).action_code,
                       p_emp_records(i).legal_employer_name,
                       p_emp_records(i).effective_start_date,
                       p_emp_records(i).primary_flag,
                       p_emp_records(i).primary_assignment_flag,
                       p_emp_records(i).worker_type,
                       p_emp_records(i).worker_number,
                       p_emp_records(i).manager_name,
                       p_emp_records(i).business_unit_name,
                       p_emp_records(i).department_name,
                       p_emp_records(i).job_code,
                       p_emp_records(i).job_name,
                       p_emp_records(i).location_code,
                       p_emp_records(i).location_name,
                       p_emp_records(i).grade_code,
                       p_emp_records(i).grade_name,
                       p_emp_records(i).division,
                       p_emp_records(i).effective_sequence,
                       p_emp_records(i).default_expense_account,
                       p_emp_records(i).new_deafault_expense_account,
                       p_emp_records(i).company,
                       p_emp_records(i).location_segment,
                       p_emp_records(i).division_segment,
                       p_emp_records(i).cost_center,
                       'OIC',
                       'OIC',
                       sysdate,
                       sysdate );

        COMMIT;
        SELECT
            edea.*
        BULK COLLECT
        INTO x_new_expense_record
        FROM
            xxint_i130_employee_default_expense_account_stg edea;

        FORALL i IN x_new_expense_record.first..x_new_expense_record.last
            UPDATE xxint.xxint_i130_employee_default_expense_account_stg
            SET
                company = (
                    SELECT
                        value_code
                    FROM
                        xxmap.XXMAP_XX_I004_COA_SEGMENT_VALUES
                    WHERE
                            description = xxint_i130_employee_default_expense_account_stg.legal_employer_name
                        AND value_category = 'GLOBAL_COMPANY'
                ),
                location_segment = (
                    SELECT
                        value_code
                    FROM
                        xxmap.XXMAP_XX_I004_COA_SEGMENT_VALUES
                    WHERE
                            description = xxint_i130_employee_default_expense_account_stg.location_name
                        AND value_category = 'GLOBAL_LOCATION'
                ),
                division_segment = (
                    SELECT
                        value_code
                    FROM
                        xxmap.XXMAP_XX_I004_COA_SEGMENT_VALUES
                    WHERE
                            description = xxint_i130_employee_default_expense_account_stg.division
                        AND value_category = 'GLOBAL_DIVISION'
                ),
                cost_center = (
                    SELECT
                        value_code
                    FROM
                        xxmap.XXMAP_XX_I004_COA_SEGMENT_VALUES
                    WHERE
                            description = xxint_i130_employee_default_expense_account_stg.department_name
                        AND value_category = 'GLOBAL_COST_CENTER'
                )
            WHERE
                xxint_i130_employee_default_expense_account_stg.assignment_id = x_new_expense_record(i).assignment_id;

        COMMIT;
        UPDATE xxint_i130_employee_default_expense_account_stg
        SET
            new_deafault_expense_account = COALESCE(company,
                                               regexp_substr(default_expense_account, '[^-]+', 1, 1),'9999' )
                                           || '-'
                                           || COALESCE(division_segment,
                                                  regexp_substr(default_expense_account, '[^-]+', 1, 2),'999')
                                           || '-'
                                           || COALESCE(cost_center,
                                                  regexp_substr(default_expense_account, '[^-]+', 1, 3),'99999')
                                           || '-'
                                          -- || regexp_substr(default_expense_account, '[^-]+', 1, 4)
										   || '999999'
                                           || '-'
                                          -- || regexp_substr(default_expense_account, '[^-]+', 1, 5)
										   || '9999'
                                           || '-'
                                           || COALESCE(location_segment,
                                                  regexp_substr(default_expense_account, '[^-]+', 1, 6),'999999')
                                           || '-'
                                           || '9999'
                                           || '-'
                                           || '9999'
                                           || '-'
                                           || '9999'
                                           || '-'
                                           || '999999';

        COMMIT;
        SELECT
            *
        BULK COLLECT
        INTO x_new_expense_record
        FROM
            xxint_i130_employee_default_expense_account_stg
        WHERE
            new_deafault_expense_account IS NOT NULL
           -- AND default_expense_account IS NOT NULL
            AND new_deafault_expense_account <> nvl (default_expense_account, '9999');

        SELECT
            COUNT(*)
        INTO x_return_records_count
        FROM
            xxint_i130_employee_default_expense_account_stg
        WHERE
            new_deafault_expense_account IS NOT NULL
           -- AND default_expense_account IS NOT NULL
            AND new_deafault_expense_account <> nvl (default_expense_account, '9999');

    EXCEPTION
        WHEN OTHERS THEN
            x_status_code := sqlcode;
            x_status_message := sqlerrm;
    END;

END xxint_i130_emp_expense_acc_update_pkg;