--------------------------------------------------------
--  DDL for Package Body XXINT_GL_I008_CONCUR_ACC_JOURNALS_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "XXINT"."XXINT_GL_I008_CONCUR_ACC_JOURNALS_PKG" AS
/********************************************************************************************
OBJECT NAME: GL Concur Accruals Journals Package
DESCRIPTION: Package specification for GL_I008
Version 	Name              	Date           		Version-Description
---------------------------------------------------------------------------
<1.0>		Devishi   			23-May-2025 	    1.0-Initial Draft
**********************************************************************************************/

	/*****************************************************************
	OBJECT NAME: Validation Transformation Procedure
	DESCRIPTION: Procedure for Validation Transformation
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Devishi   			23-May-2025 	    1.0- Initial Draft
	******************************************************************/

    PROCEDURE validation_transformation_prc (
        p_current_instance_id IN VARCHAR2,
        p_source_file_name    IN VARCHAR2,
        x_status              OUT VARCHAR2,
        x_status_message      OUT VARCHAR2
    ) AS

---Cursor to check amount sum
        CURSOR cur_amount_check IS
        SELECT
            period_name,
            effective_date_of_transaction,
            reference1_batchname,
            reference4_jl_entryname,
            segment1,
            SUM(entered_credit_amount) sum_entered_cr,
            SUM(entered_debit_amount)  sum_entered_dr
        FROM
            xxint_gl_i008_concur_acc_journals_stg
        WHERE
            current_instance_id = p_current_instance_id
        GROUP BY
            period_name,
            effective_date_of_transaction,
            reference1_batchname,
            reference4_jl_entryname,
            segment1
        HAVING
            abs(sum_entered_cr - sum_entered_dr) > 1;

		---Cursor to get ledger currency
        CURSOR c_get_lcurrency IS
        SELECT
            glm.ledger_currency_code          AS ledger_currency,
            stg.effective_date_of_transaction accrual_date,
            stg.rowid                         row_id
        FROM
            ----<1.1> Code changes start
            (
                SELECT DISTINCT
                    ledger_currency_code,
                    ledger_name,
                    ledger_category_code
                FROM
                    xxmap.xxmap_gl_job_parameters_ref
            )                                     glm,
			----<1.1> Code changes start
            xxint_gl_i008_concur_acc_journals_stg stg
        WHERE
                stg.current_instance_id = p_current_instance_id
            AND glm.ledger_name = stg.ledger_name
            AND upper(glm.ledger_category_code) = upper('PRIMARY');

		---Cursor to get ledger details
        CURSOR c_get_ledger IS
        SELECT DISTINCT
            t1.ledger_id   ledger_id,
            t1.ledger_name ledger_name,
            stg.segment1 segment1
        FROM
            xxmap.xxmap_gl_job_parameters_ref     t1,
            xxint_gl_i008_concur_acc_journals_stg stg
        WHERE
                stg.current_instance_id = p_current_instance_id
			  ----<1.1> Code changes
            AND upper(t1.ledger_category_code) = upper('PRIMARY')
            AND stg.segment1 = t1.segment1;

		---Cursor to get vendor details
        CURSOR c_get_vendor IS
        SELECT
            t1.erp_coa_value,
            stg.rowid row_id
        FROM
            xxmap.xxmap_gl_concur_vendor_values_ref t1,
            xxint_gl_i008_concur_acc_journals_stg   stg
        WHERE
                1 = 1
            AND current_instance_id = p_current_instance_id
            AND stg.vendor LIKE t1.concur_company_attribute_1;

        TYPE lt_amountcheck IS
            TABLE OF cur_amount_check%rowtype;
        TYPE lt_lcurrency IS
            TABLE OF c_get_lcurrency%rowtype;
        TYPE lt_ledger IS
            TABLE OF c_get_ledger%rowtype;
        TYPE lt_vendor IS
            TABLE OF c_get_vendor%rowtype;
        l_amountcheck     lt_amountcheck;
        l_lcurrency       lt_lcurrency;
        l_ledger          lt_ledger;
        l_vendor          lt_vendor;
        l_status          VARCHAR2(100) := '';
        l_status_message  VARCHAR2(4000) := '';
        l_lcurrency_limit NUMBER := 10000;
        l_ledger_limit    NUMBER := 10000;
        l_vendor_limit    NUMBER := 10000;
        g_custom_exp EXCEPTION;
    BEGIN
        BEGIN
            UPDATE xxint_gl_i008_concur_acc_journals_stg
            SET
                status = 'INVALID',
                message = 'Invalid Date Format for Accrual Date. Please use YYYY/MM/DD date format'
            WHERE
                effective_date_of_transaction NOT LIKE '____/__/__'
                AND current_instance_id = p_current_instance_id;

        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'ERROR';
                l_status_message := 'Unxpected exception in checking Date format. Error Details - ' || sqlerrm;
        END;

        BEGIN
            UPDATE xxint_gl_i008_concur_acc_journals_stg
            SET
                status =
                    CASE
                        WHEN effective_date_of_transaction IS NULL
                             OR currency_code IS NULL
                             OR segment1 IS NULL
                             OR segment2 IS NULL
                             OR segment3 IS NULL
                             OR segment4 IS NULL
                             OR segment5 IS NULL
                             OR segment6 IS NULL THEN
                            'INVALID'
                        ELSE
                            status
                    END,
                message = message
                          ||
                          CASE
                              WHEN effective_date_of_transaction IS NULL THEN
                                  '|Effective_Date_of_transaction should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN currency_code IS NULL THEN
                                  '|Currency_Code should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN segment1 IS NULL THEN
                                  '|SEGMENT1 should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN segment2 IS NULL THEN
                                  '|SEGMENT2 should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN segment3 IS NULL THEN
                                  '|SEGMENT3 should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN segment4 IS NULL THEN
                                  '|SEGMENT4 should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN segment5 IS NULL THEN
                                  '|SEGMENT5 should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN segment6 IS NULL THEN
                                  '|SEGMENT6 should not be NULL'
                              ELSE
                                  ''
                          END
            WHERE
                    current_instance_id = p_current_instance_id
                AND ( effective_date_of_transaction IS NULL
                      OR currency_code IS NULL
                      OR segment1 IS NULL
                      OR segment2 IS NULL
                      OR segment3 IS NULL
                      OR segment4 IS NULL
                      OR segment5 IS NULL
                      OR segment6 IS NULL );

        EXCEPTION
            WHEN OTHERS THEN
                x_status := 'ERROR';
                x_status_message := 'Unexpected error in updating mandatory columnns null conditions. Error Details - ' || sqlerrm;
                RAISE g_custom_exp;
        END;

		----Amount Check----
        BEGIN
            OPEN cur_amount_check;
            LOOP
                FETCH cur_amount_check
                BULK COLLECT INTO l_amountcheck;
                EXIT WHEN l_amountcheck.count = 0;
                FORALL i IN l_amountcheck.first..l_amountcheck.last SAVE EXCEPTIONS
                    UPDATE xxint_gl_i008_concur_acc_journals_stg
                    SET
                    ---<1.1> Code Changes start
                        status = (
                            CASE
                                WHEN abs(l_amountcheck(i).sum_entered_cr - l_amountcheck(i).sum_entered_dr) > 1 THEN
                                    'INVALID'
                                ELSE
                                    status
                            END
                        ),
					---<1.1> Code Changes end
                        message = (
                            CASE
                                WHEN ( abs(l_amountcheck(i).sum_entered_cr - l_amountcheck(i).sum_entered_dr) ) > 1 THEN
                                    message
                                    || '|Entered_CR and Entered_DR are not equal and amount difference:'
                                    || ( l_amountcheck(i).sum_entered_cr - l_amountcheck(i).sum_entered_dr )
                                    || '.'
                                ELSE
                                    message
                            END
                        )
                    WHERE
                            period_name = l_amountcheck(i).period_name
                        AND effective_date_of_transaction = l_amountcheck(i).effective_date_of_transaction
                        AND reference1_batchname = l_amountcheck(i).reference1_batchname
                        AND reference4_jl_entryname = l_amountcheck(i).reference4_jl_entryname
                        AND segment1 = l_amountcheck(i).segment1
                        AND current_instance_id = p_current_instance_id;

            END LOOP;

            COMMIT;
            CLOSE cur_amount_check;
        EXCEPTION
            WHEN OTHERS THEN
                x_status := 'ERROR';
                x_status_message := 'Unexpected error in validating amount fields. Error Details - ' || sqlerrm;
                RAISE g_custom_exp;
        END;

      ----Update ledger fields----
        BEGIN
            OPEN c_get_ledger;
            LOOP
                FETCH c_get_ledger
                BULK COLLECT INTO l_ledger LIMIT l_ledger_limit;
                EXIT WHEN l_ledger.count() = 0;
                FORALL i IN l_ledger.first..l_ledger.last SAVE EXCEPTIONS
                    UPDATE xxint_gl_i008_concur_acc_journals_stg
                    SET
                        ledger_id = l_ledger(i).ledger_id,
                        ledger_name = l_ledger(i).ledger_name
                    WHERE
                        current_instance_id = p_current_instance_id
                        and segment1=l_ledger(i).segment1;

            END LOOP;

            COMMIT;
            CLOSE c_get_ledger;

			-----Few non-matching(segment1 missing or value not found in ledger sync table) update

            UPDATE xxint_gl_i008_concur_acc_journals_stg stg
            SET
                status = 'INVALID',
                message = 'Invalid Ledger Name.Either COA Segment1(Company) is null or sync table has no data for the provided segment value.'
            WHERE
                    current_instance_id = p_current_instance_id
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        xxmap.xxmap_gl_job_parameters_ref t1
                    WHERE
                            t1.segment1 = stg.segment1
						----<1.1> Code changes 
                        AND upper(t1.ledger_category_code) = upper('PRIMARY')
                );

        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'ERROR';
                l_status_message := 'Unexpected error in updating ledger fields. Error Details - ' || sqlerrm;
        END;


 ----Update vendor fields----
        BEGIN
            OPEN c_get_vendor;
            LOOP
                FETCH c_get_vendor
                BULK COLLECT INTO l_vendor LIMIT l_vendor_limit;
                IF l_vendor.count() = 0 THEN
                    UPDATE xxint_gl_i008_concur_acc_journals_stg
                    SET
                        journal_entry_line_dff5 = vendor
                    WHERE
                        current_instance_id = p_current_instance_id;

                ELSE
                    BEGIN
                        FORALL i IN l_vendor.first..l_vendor.last SAVE EXCEPTIONS
                            UPDATE xxint_gl_i008_concur_acc_journals_stg
                            SET
                                journal_entry_line_dff5 = l_vendor(i).erp_coa_value
                            WHERE
                                    current_instance_id = p_current_instance_id
                                AND ROWID = l_vendor(i).row_id;

                    EXCEPTION
                        WHEN OTHERS THEN
                            l_status := 'ERROR';
                            l_status_message := 'Unexpected error in updating vendor fields. Error Details - ' || sqlerrm;
                    END;
                END IF;

                EXIT WHEN c_get_vendor%notfound;
            END LOOP;

            COMMIT;
            CLOSE c_get_vendor;

		-----Few non-matching(from vendor sync table) vendor update
            UPDATE xxint_gl_i008_concur_acc_journals_stg
            SET
                journal_entry_line_dff5 = vendor
            WHERE
                    current_instance_id = p_current_instance_id
                AND journal_entry_line_dff5 IS NULL
                AND EXISTS (
                    SELECT
                        t1.erp_coa_value,
                        stg.rowid row_id
                    FROM
                        xxmap.xxmap_gl_concur_vendor_values_ref t1,
                        xxint_gl_i008_concur_acc_journals_stg   stg
                    WHERE
                            1 = 1
                        AND current_instance_id = p_current_instance_id
                        AND stg.vendor LIKE t1.concur_company_attribute_1
                );

        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'ERROR';
                l_status_message := 'Unexpected error in updating vendor fields. Error Details - ' || sqlerrm;
        END;



----Update Amount fields----
        BEGIN
            OPEN c_get_lcurrency;
            LOOP
                FETCH c_get_lcurrency
                BULK COLLECT INTO l_lcurrency LIMIT l_lcurrency_limit;
                EXIT WHEN l_lcurrency.count() = 0;
                FORALL i IN l_lcurrency.first..l_lcurrency.last SAVE EXCEPTIONS
                    UPDATE xxint_gl_i008_concur_acc_journals_stg
                    SET
                        currency_conversion_type = decode(currency_code,
                                                          l_lcurrency(i).ledger_currency,
                                                          NULL,
                                                          'Corporate'),
                        currency_conversion_date = decode(currency_code,
                                                          l_lcurrency(i).ledger_currency,
                                                          NULL,
                                                          l_lcurrency(i).accrual_date)
                    WHERE
                            current_instance_id = p_current_instance_id
                        AND ROWID = l_lcurrency(i).row_id;

            END LOOP;

            COMMIT;
            CLOSE c_get_lcurrency;
            UPDATE xxint_gl_i008_concur_acc_journals_stg stg
            SET
                status = 'INVALID',
                message = message || 'Invalid Ledger Currency.Either currency_code is null or sync table(xxmap_gl_job_parameters_ref) has no data for the provided currency value.'
            WHERE
                    current_instance_id = p_current_instance_id
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        xxmap.xxmap_gl_job_parameters_ref t1
                    WHERE
                            t1.segment1 = stg.segment1
                        AND upper(t1.ledger_category_code) = upper('PRIMARY')
                );

        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'ERROR';
                l_status_message := 'Unexpected error in updating amount fields. Error Details - ' || sqlerrm;
        END;


	---Update PERIOD_NAME---------

        BEGIN
            UPDATE xxint_gl_i008_concur_acc_journals_stg
            SET
                period_name = to_char(TO_DATE(period_name, 'YYYY/MM/DD'), 'MON-YY')
            WHERE
                    current_instance_id = p_current_instance_id
                AND period_name IS NOT NULL
                AND REGEXP_LIKE ( period_name,
                                  '^\d{4}/\d{2}/\d{2}$' );

            COMMIT;
            l_status := 'SUCCESS';
            l_status_message := 'SUCCESS';
        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'ERROR';
                l_status_message := 'Unexpected error in updating Period. Error Details - ' || sqlerrm;
        END;

       -- Update remaining records as Valid Records
        UPDATE xxint_gl_i008_concur_acc_journals_stg
        SET
            status = 'VALID'
        WHERE
                current_instance_id = p_current_instance_id
            AND status = 'NEW';

        l_status := 'SUCCESS';
        l_status_message := 'SUCCESS';
        x_status := l_status;
        x_status_message := l_status_message;
    EXCEPTION
        WHEN g_custom_exp THEN
            x_status := l_status;
            x_status_message := l_status_message;
        WHEN OTHERS THEN
            x_status := 'ERROR';
            x_status_message := 'Unexpected error in validation_transformation_prc procedure. Error Details - ' || sqlerrm;
    END validation_transformation_prc;

END xxint_gl_i008_concur_acc_journals_pkg;

/
