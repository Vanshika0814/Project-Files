--------------------------------------------------------
--  DDL for Package Body XXINT_GL_I001_JOURNALS_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "XXINT"."XXINT_GL_I001_JOURNALS_PKG" IS
/********************************************************************************************
OBJECT NAME: GL Journals Package
DESCRIPTION: Package specification for GL_I001
Version 	Name              	Date           		Version-Description
---------------------------------------------------------------------------
<1.0>		Devishi   			02-MAR-2025 	    1.0-Initial Draft
<1.1>       Devishi   			30-JULY-2025 	    1.1-Removed 'batch_details_prc' procedure
<1.2>		Devishi				02-SEPT-2025		1.2-LTCI-8034 Code optimization changes
<1.3>		Devishi				11-SEPT-2025		1.3-LTCI-8919 Code removal changes
<1.4>		Devishi				16-SEPT-2025		1.4-LTCI-7468 Performance optimization changes
<1.5>		Devishi				08-OCT-2025			1.5-BO-78 Code changes related to deadlock
**********************************************************************************************/

/*****************************************************************
	OBJECT NAME: Get ledger Name Procedure
	DESCRIPTION: Procedure to fetch Ledger Name 
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Devishi   			02-MAR-2025 	    1.0- Initial Draft
******************************************************************/
    PROCEDURE validation_transformation_prc (
        p_current_instance_id IN VARCHAR2,
        p_batch_limit         IN VARCHAR2,
        x_status              OUT VARCHAR2,
        x_status_message      OUT VARCHAR2
    ) AS 

	---Cursor to get ledger currency
        CURSOR c_get_lcurrency IS
        SELECT
            glm.ledger_currency_code AS ledger_currency,
            stg.transaction_date     transaction_date,
            stg.rowid                row_id ---<1.2> Code Changes
        FROM
		----<1.4> Code changes start
            (
                SELECT DISTINCT
                    ledger_currency_code,
                    ledger_name,
                    ledger_category_code
                FROM
                    xxmap.xxmap_gl_job_parameters_ref
            )                          glm,
			----<1.4> Code changes start
            xxint_gl_i001_journals_stg stg
        WHERE
                stg.current_instance_id = p_current_instance_id
            AND glm.ledger_name = stg.ledger_name
			----<1.4> Code changes start
            AND upper(glm.ledger_category_code) = upper('PRIMARY');

	---Cursor to get ledger details
        CURSOR c_get_ledger IS
        SELECT DISTINCT
            t1.ledger_id    ledger_id,
            t1.ledger_name  ledger_name,
            stg.ns_segment1 AS ns_segment1
        FROM
            xxmap.xxmap_gl_job_parameters_ref     t1,
            xxmap.xxmap_gl_e001_kaseya_ns_company t2,
            xxint_gl_i001_journals_stg            stg
        WHERE
                stg.ns_segment1 = t2.ns_company_attribute_1
            AND t1.segment1 = t2.erp_coa_value
			----<1.4> Code changes
            AND upper(t1.ledger_category_code) = upper('PRIMARY')
            AND stg.current_instance_id = p_current_instance_id;



	---Cursor to check amount sum
        CURSOR cur_amount_check IS
        SELECT
            ledger_name,
            period_name,
            transaction_date,
            reference1,
            reference4,
            ns_segment1,
            SUM(entered_credit_amount) sum_entered_cr,
            SUM(entered_debit_amount)  sum_entered_dr
        FROM
            xxint.xxint_gl_i001_journals_stg
        WHERE
            current_instance_id = p_current_instance_id
        GROUP BY
            ledger_name,
            period_name,
            transaction_date,
            reference1,
            reference4,
            ns_segment1
        HAVING
            abs(sum_entered_cr - sum_entered_dr) > 1;

        TYPE lt_lcurrency IS
            TABLE OF c_get_lcurrency%rowtype;
        TYPE lt_amountcheck IS
            TABLE OF cur_amount_check%rowtype;
        TYPE lt_ledger IS
            TABLE OF c_get_ledger%rowtype;

		 -- Declaring Local Variables for validation
        l_lcurrency       lt_lcurrency;
        l_amountcheck     lt_amountcheck;
        l_ledger          lt_ledger;
        l_status          VARCHAR2(100) := '';
        l_status_message  VARCHAR2(4000) := '';
        l_ledger_currency VARCHAR2(10);
        l_lcurrency_limit NUMBER := 10000;
        l_ledger_limit    NUMBER := 10000;
        g_custom_exp EXCEPTION;
        counter           NUMBER := 0;
    BEGIN
        BEGIN
            UPDATE xxint.xxint_gl_i001_journals_stg
            SET
                status =
                    CASE
                        WHEN transaction_date IS NULL
                             OR journal_category IS NULL
                             OR currency_code IS NULL
                             OR je_creation_date IS NULL
                             OR reference1 IS NULL
                             OR reference4 IS NULL
                             OR reference7 IS NULL
                             OR reference10 IS NULL
                             OR period_name IS NULL THEN
                            'INVALID'
                        ELSE
                            status
                    END,
                message = message
                          ||
                          CASE
                              WHEN transaction_date IS NULL THEN
                                  '|Effective_Date_of_transaction should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN journal_category IS NULL THEN
                                  '|Journal_Category should not be NULL'
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
                              WHEN je_creation_date IS NULL THEN
                                  '|Je_Creation_Date should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN reference1 IS NULL THEN
                                  '|REFERENCE1 should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN reference4 IS NULL THEN
                                  '|REFERENCE4 should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN reference7 IS NULL THEN
                                  '|REFERENCE7 should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN reference10 IS NULL THEN
                                  '|REFERENCE10 should not be NULL'
                              ELSE
                                  ''
                          END
                          ||
                          CASE
                              WHEN period_name IS NULL THEN
                                  '|PERIOD_NAME should not be NULL'
                              ELSE
                                  ''
                          END
            WHERE
                    current_instance_id = p_current_instance_id
                AND ( transaction_date IS NULL
                      OR journal_category IS NULL
                      OR currency_code IS NULL
                      OR je_creation_date IS NULL
                      OR reference1 IS NULL
                      OR reference4 IS NULL
                      OR reference7 IS NULL
                      OR reference10 IS NULL
                      OR period_name IS NULL );

        EXCEPTION
            WHEN OTHERS THEN
                x_status := 'ERROR';
                x_status_message := 'Unexpected error in updating mandatory columnns null conditions. Error Details - ' || sqlerrm;
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
                    UPDATE xxint_gl_i001_journals_stg
                    SET
                        ledger_id = l_ledger(i).ledger_id,
                        ledger_name = l_ledger(i).ledger_name
                    WHERE
                            ns_segment1 = l_ledger(i).ns_segment1
                        AND current_instance_id = p_current_instance_id; ---<1.2> Code Changes


            END LOOP;

            COMMIT;
            CLOSE c_get_ledger;

			-----Few non-matching(segment1 missing or value not found in ledger sync table) update

            UPDATE xxint_gl_i001_journals_stg stg
            SET
                status = 'INVALID',
                message = 'Invalid Ledger Name.Either COA Segment1(Company) is null or sync table has no data for the provided segment value.'
            WHERE
                    current_instance_id = p_current_instance_id
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        xxmap.xxmap_gl_job_parameters_ref     t1,
                        xxmap.xxmap_gl_e001_kaseya_ns_company t2
                    WHERE
                            stg.ns_segment1 = t2.ns_company_attribute_1
                        AND t1.segment1 = t2.erp_coa_value
						----<1.4> Code changes
                        AND upper(t1.ledger_category_code) = upper('PRIMARY')
                );

        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'ERROR';
                l_status_message := 'Unexpected error in updating ledger fields. Error Details - ' || sqlerrm;
        END;

        BEGIN
            BEGIN
                UPDATE xxint_gl_i001_journals_stg
                SET
                    ns_segment6 = replace(ns_segment6, '|', '')
                WHERE
                    current_instance_id = p_current_instance_id;

                COMMIT;
            END;

            BEGIN
                UPDATE xxint_gl_i001_journals_stg
                SET
                    attribute7 = substr(attribute7, 1, 148),
                    reference10 = substr(reference10, 1, 238)
                WHERE
                    current_instance_id = p_current_instance_id;

                COMMIT;
            END;

        EXCEPTION
            WHEN OTHERS THEN
                x_status := 'ERROR';
                x_status_message := 'Unexpected error in removing special chaarcters from columns. Error Details - ' || sqlerrm;
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
                    UPDATE xxint_gl_i001_journals_stg
                    SET
					---<1.2> Code Changes start
                        status = (
                            CASE
                                WHEN abs(l_amountcheck(i).sum_entered_cr - l_amountcheck(i).sum_entered_dr) > 1 THEN
                                    'INVALID'
                                ELSE
                                    status
                            END
                        ),
					---<1.2> Code Changes end
                        message = (
                            CASE
                                WHEN ( abs(l_amountcheck(i).sum_entered_cr - l_amountcheck(i).sum_entered_dr) ) > 1 THEN
                                    message
                                    || '|Entered_CR and Entered_DR are not equal and amount difference:'
                                    || ( l_amountcheck(i).sum_entered_cr - l_amountcheck(i).sum_entered_dr )
                                ELSE
                                    message
                            END
                        )
                    WHERE
                            ledger_name = l_amountcheck(i).ledger_name
                        AND period_name = l_amountcheck(i).period_name
                        AND transaction_date = l_amountcheck(i).transaction_date
                        AND reference1 = l_amountcheck(i).reference1
                        AND reference4 = l_amountcheck(i).reference4
                        AND ns_segment1 = l_amountcheck(i).ns_segment1
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


----Update Amount fields----
        BEGIN
            OPEN c_get_lcurrency;
            LOOP
                FETCH c_get_lcurrency
                BULK COLLECT INTO l_lcurrency LIMIT l_lcurrency_limit;
                EXIT WHEN l_lcurrency.count() = 0;
                FORALL i IN l_lcurrency.first..l_lcurrency.last SAVE EXCEPTIONS
                    UPDATE xxint_gl_i001_journals_stg
                    SET
                        entered_credit_amount =
                            CASE
                                WHEN currency_code = l_lcurrency(i).ledger_currency
                                     AND converted_credit_amount IS NOT NULL THEN
                                    converted_credit_amount
                                ELSE
                                    entered_credit_amount
                            END,
                        entered_debit_amount =
                            CASE
                                WHEN currency_code = l_lcurrency(i).ledger_currency
                                     AND converted_debit_amount IS NOT NULL THEN
                                    converted_debit_amount
                                ELSE
                                    entered_debit_amount
                            END,
                        converted_credit_amount =
                            CASE
                                WHEN currency_code = l_lcurrency(i).ledger_currency THEN
                                    NULL
                                ELSE
                                    converted_credit_amount
                            END,
                        converted_debit_amount =
                            CASE
                                WHEN currency_code = l_lcurrency(i).ledger_currency THEN
                                    NULL
                                ELSE
                                    converted_debit_amount
                            END,
							---Added as per latest mapping
                        currency_conversion_type =
                            CASE
                                WHEN currency_code = l_lcurrency(i).ledger_currency THEN
                                    NULL
                                ELSE
                                    'Corporate'
                            END,
                        currency_conversion_date =
                            CASE
                                WHEN currency_code = l_lcurrency(i).ledger_currency THEN
                                    NULL
                                ELSE
                                    l_lcurrency(i).transaction_date
                            END
                    WHERE
                            current_instance_id = p_current_instance_id ---<1.2> Code Changes
                        AND ROWID = l_lcurrency(i).row_id;

            END LOOP;

            COMMIT;
            CLOSE c_get_lcurrency;


--Update in case ledger not populated
            UPDATE xxint_gl_i001_journals_stg stg
            SET
                status = 'INVALID',
                message = message || 'Invalid Ledger Currency.Either currency_code is null or sync table has no data for the provided currency value.'
            WHERE
                    current_instance_id = p_current_instance_id
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        xxmap.xxmap_gl_job_parameters_ref t1
                    WHERE
                            t1.ledger_name = stg.ledger_name
                        AND upper(t1.ledger_category_code) = upper('PRIMARY')
                );

        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'ERROR';
                l_status_message := 'Unexpected error in updating amount fields. Error Details - ' || sqlerrm;
        END;

		       -- Update remaining records as Valid Records
        UPDATE xxint_gl_i001_journals_stg
        SET
            status = 'VALID'
        WHERE
                current_instance_id = p_current_instance_id
            AND status = 'NEW';


        -- Update Process Batch ID
        BEGIN
		----<1.5> Code Start---
            MERGE INTO xxint_gl_i001_journals_stg tgt
            USING (
                SELECT
                    ROWID              AS r_id,
                    ceil(ROW_NUMBER()
                         OVER(
                        ORDER BY
                            current_instance_id
                         ) / p_batch_limit) AS new_batch_id
                FROM
                    xxint_gl_i001_journals_stg
                WHERE
                    current_instance_id = p_current_instance_id
            ) src ON ( tgt.rowid = src.r_id )
            WHEN MATCHED THEN UPDATE
            SET process_batch_id = src.new_batch_id;
		----<1.5> Code End---

            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'ERROR';
                l_status_message := 'Unexpected error in updating Process Batch ID. Error Details - ' || sqlerrm;
                RAISE g_custom_exp;
        END;

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


  /*****************************************************************
	OBJECT NAME: Split COA Values Procedure
	DESCRIPTION: Procedure to split COA Values 
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Devishi   			16-APR-2025 	    1.0- Initial Draft
******************************************************************/
    PROCEDURE coa_split_prc (
        p_current_instance_id IN VARCHAR2,
        p_coa_batch_limit     IN VARCHAR2,
        x_status              OUT VARCHAR2,
        x_status_message      OUT VARCHAR2
    ) AS

        CURSOR coa_split_records IS
        SELECT
            row_id,
            ledger_name,
            ns_segment1,
            ns_segment2,
            ns_segment3,
            ns_segment4,
            ns_segment5,
            ns_segment6,
            ns_segment7,
            ns_segment8,
            ns_segment9,
            ns_segment10,
            ceil(ROWNUM / p_coa_batch_limit) AS batch_num
        FROM
            (
                SELECT
                    ROWID AS row_id,
                    ledger_name,
                    ns_segment1,
                    ns_segment2,
                    ns_segment3,
                    ns_segment4,
                    ns_segment5,
                    ns_segment6,
                    ns_segment7,
                    ns_segment8,
                    ns_segment9,
                    ns_segment10,
                    ROW_NUMBER()
                    OVER(PARTITION BY ledger_name, ns_segment1, ns_segment2, ns_segment3, ns_segment4,
                                      ns_segment5, ns_segment6, ns_segment7, ns_segment8, ns_segment9,
                                      ns_segment10
                         ORDER BY
                             ROWID
                    )     AS row_num
                FROM
                    xxint.xxint_gl_i001_journals_stg
                WHERE
                        status <> 'COA_VALID'
                    AND current_instance_id = p_current_instance_id
            )
        WHERE
            row_num = 1;

        TYPE lt_coasplit IS
            TABLE OF coa_split_records%rowtype;
        l_coasplit       lt_coasplit;
        l_status         VARCHAR2(100) := '';
        l_status_message VARCHAR2(4000) := '';
        l_lcoa_limit     NUMBER := 200; ---<1.4> Code Changes
        g_custom_exp EXCEPTION;
    BEGIN
        BEGIN
            OPEN coa_split_records;
            LOOP
                FETCH coa_split_records
                BULK COLLECT INTO l_coasplit LIMIT l_lcoa_limit;
                EXIT WHEN l_coasplit.count = 0;
                FORALL i IN l_coasplit.first..l_coasplit.last SAVE EXCEPTIONS
                    UPDATE xxint_gl_i001_journals_stg
                    SET
                        coa_batch_id = l_coasplit(i).batch_num
                    WHERE
                            current_instance_id = p_current_instance_id
                        AND ROWID = l_coasplit(i).row_id;

            END LOOP;

            COMMIT;
            CLOSE coa_split_records;
        EXCEPTION
            WHEN OTHERS THEN
                x_status := 'ERROR';
                x_status_message := 'Unexpected error in COA Batch Split. Error Details - ' || sqlerrm;
                RAISE g_custom_exp;
        END;

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
            x_status_message := 'Unexpected error in coa_split_prc procedure. Error Details - ' || sqlerrm;
    END coa_split_prc;


  /*****************************************************************
	OBJECT NAME: Update COA Values Procedure
	DESCRIPTION: Procedure to update COA Values 
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Devishi   			16-APR-2025 	    1.0- Initial Draft
******************************************************************/
    PROCEDURE coa_values_prc (
        p_current_instance_id IN VARCHAR2,
        p_batch_limit         IN VARCHAR2,
        x_status              OUT VARCHAR2,
        x_status_message      OUT VARCHAR2
    ) AS

        l_status         VARCHAR2(100) := '';
        l_status_message VARCHAR2(4000) := '';
        g_custom_exp EXCEPTION;
    BEGIN
        BEGIN
            UPDATE xxint.xxint_gl_i001_journals_stg gl
            SET
                ( gl.erp_segment1,
                  gl.erp_segment2,
                  gl.erp_segment3,
                  gl.erp_segment4,
                  gl.erp_segment5,
                  gl.erp_segment6,
                  gl.erp_segment7,
                  gl.erp_segment8,
                  gl.erp_segment9,
                  gl.erp_segment10,
                  status ) = (
                    SELECT
                        coa.erp_segment1,
                        coa.erp_segment2,
                        coa.erp_segment3,
                        coa.erp_segment4,
                        coa.erp_segment5,
                        coa.erp_segment6,
                        coa.erp_segment7,
                        coa.erp_segment8,
                        coa.erp_segment9,
                        coa.erp_segment10,
                        'COA_VALID'
                    FROM
                        xxmap.xxmap_gl_e001_coa_nserp_data coa
                    WHERE
					----<1.2> Code changes start
                            nvl(gl.ns_segment1, '~NULL~') = nvl(coa.ns_segment1, '~NULL~')
                        AND nvl(gl.ns_segment2, '~NULL~') = nvl(coa.ns_segment2, '~NULL~')
                        AND nvl(gl.ns_segment3, '~NULL~') = nvl(coa.ns_segment3, '~NULL~')
                        AND nvl(gl.ns_segment4, '~NULL~') = nvl(coa.ns_segment4, '~NULL~')
                        AND nvl(gl.ns_segment5, '~NULL~') = nvl(coa.ns_segment5, '~NULL~')
                        AND nvl(gl.ns_segment6, '~NULL~') = nvl(coa.ns_segment6, '~NULL~')
                        AND nvl(gl.ns_segment7, '~NULL~') = nvl(coa.ns_segment7, '~NULL~')
                        AND nvl(gl.ns_segment8, '~NULL~') = nvl(coa.ns_segment8, '~NULL~')
                        AND nvl(gl.ns_segment9, '~NULL~') = nvl(coa.ns_segment9, '~NULL~')
                        AND nvl(gl.ns_segment10, '~NULL~') = nvl(coa.ns_segment10, '~NULL~')
						---<1.4> Code Changes
                        AND ROWNUM = 1			  
					----<1.2> Code changes end
                )
            WHERE
                    gl.current_instance_id = p_current_instance_id
                AND status <> 'COA_VALID'
                AND EXISTS (
                    SELECT
                        1
                    FROM
                        xxmap.xxmap_gl_e001_coa_nserp_data coa
                    WHERE
					----<1.2> Code changes start
                            nvl(gl.ns_segment1, '~NULL~') = nvl(coa.ns_segment1, '~NULL~')
                        AND nvl(gl.ns_segment2, '~NULL~') = nvl(coa.ns_segment2, '~NULL~')
                        AND nvl(gl.ns_segment3, '~NULL~') = nvl(coa.ns_segment3, '~NULL~')
                        AND nvl(gl.ns_segment4, '~NULL~') = nvl(coa.ns_segment4, '~NULL~')
                        AND nvl(gl.ns_segment5, '~NULL~') = nvl(coa.ns_segment5, '~NULL~')
                        AND nvl(gl.ns_segment6, '~NULL~') = nvl(coa.ns_segment6, '~NULL~')
                        AND nvl(gl.ns_segment7, '~NULL~') = nvl(coa.ns_segment7, '~NULL~')
                        AND nvl(gl.ns_segment8, '~NULL~') = nvl(coa.ns_segment8, '~NULL~')
                        AND nvl(gl.ns_segment9, '~NULL~') = nvl(coa.ns_segment9, '~NULL~')
                        AND nvl(gl.ns_segment10, '~NULL~') = nvl(coa.ns_segment10, '~NULL~')
					----<1.2> Code changes end
                );

            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN
                x_status := 'ERROR';
                x_status_message := 'Unexpected error in updating COA Values. Error Details - ' || sqlerrm;
                RAISE g_custom_exp;
        END;

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
            x_status_message := 'Unexpected error in coa_values_prc procedure. Error Details - ' || sqlerrm;
    END coa_values_prc;

END xxint_gl_i001_journals_pkg;

/
