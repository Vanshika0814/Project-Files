--------------------------------------------------------
--  File created - Thursday-July-24-2025   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package Body XXINT_GL_I003_ADP_JOURNALS_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "XXINT"."XXINT_GL_I003_ADP_JOURNALS_PKG" IS
/********************************************************************************************
OBJECT NAME: GL Journals ADP Package
DESCRIPTION: Package specification for GL_I003
Version 	Name              	Date           		Version-Description
---------------------------------------------------------------------------
<1.0>		Priyanka Gupta A   			15-May-2025 	    1.0-Initial Draft
**********************************************************************************************/

    /*****************************************************************
	OBJECT NAME: Journals Data Validation Procedure
	DESCRIPTION: Procedure to validate the ADP journals source data XXINT_GL_I003_ADP_JOURNALS_STG Records 
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Priyanka Gupta A   			15-May-2025  	    1.0- Initial Draft
******************************************************************/ 

PROCEDURE data_validations_prc (
    p_parent_instance_id  IN VARCHAR2,
    p_current_instance_id IN VARCHAR2,
    p_interface_rice_id   IN VARCHAR2,
    p_integration_name    IN VARCHAR2,
    p_log_flag            IN VARCHAR2,
    x_status              OUT VARCHAR2,
    x_status_message      OUT VARCHAR2
) AS 

	 -- Declaring Local Variables for validation
        l_status         VARCHAR2(100) := '';
        l_status_message VARCHAR2(4000) := '';
        lv_total_credit NUMBER;
        lv_total_debit NUMBER;
        g_custom_exp exception;
    BEGIN
        dbms_output.put_line('start proc : ' || to_char(sysdate, 'DD-MON-YYYY HH24:MI:SS'));
		 
       --DB logging started  
       xxint.XXINT_XX_I010_COMMON_LOGGING_PKG.db_logging_details_prc(p_log_flag,p_interface_rice_id,p_integration_name,p_parent_instance_id,p_current_instance_id
                ,'XXINT_XX_I010_COMMON_LOGGING_PKG.data_validations_prc', 'Procedure Execution Started at '||systimestamp, NULL, NULL, NULL,NULL, NULL);
               
        BEGIN
        UPDATE xxint.xxint_gl_i003_adp_journals_stg
        SET
            status =
                CASE
                    WHEN effective_date_of_transaction IS NULL
                         OR segment1 IS NULL
                            OR segment2 IS NULL
                               OR segment3 IS NULL
                                  OR segment4 IS NULL
                                     OR segment5 IS NULL
                                        OR segment6 IS NULL
                                           OR period_name IS NULL THEN
                        'INVALID'
                    ELSE
                        status
                END,
            message = message
                      ||
                      CASE
                          WHEN effective_date_of_transaction IS NULL THEN
                              '|Effective_Date_of_transaction should not be NULL.'
                          ELSE
                              ''
                      END
                      ||
                      CASE
                          WHEN segment1 IS NULL THEN
                              '|Segment1-Company should not be NULL.'
                          ELSE
                              ''
                      END
                      ||
                      CASE
                          WHEN segment2 IS NULL THEN
                              '|Segment2-Division should not be NULL.'
                          ELSE
                              ''
                      END
                      ||
                      CASE
                          WHEN segment3 IS NULL THEN
                              '|Segment3-Cost Center should not be NULL.'
                          ELSE
                              ''
                      END
                      ||
                      CASE
                          WHEN segment4 IS NULL THEN
                              '|Segment4-Account should not be NULL.'
                          ELSE
                              ''
                      END
                      ||
                      CASE
                          WHEN segment5 IS NULL THEN
                              '|Segment5-Product should not be NULL.'
                          ELSE
                              ''
                      END
                      ||
                      CASE
                          WHEN segment6 IS NULL THEN
                              '|Segment6-Location should not be NULL.'
                          ELSE
                              ''
                      END
                      ||
                      CASE
                          WHEN period_name IS NULL THEN
                              '|PERIOD_NAME should not be NULL.'
                          ELSE
                              ''
                      END
        WHERE
                current_instance_id = p_current_instance_id
            AND ( effective_date_of_transaction IS NULL
                  OR segment1 IS NULL
                     OR segment2 IS NULL
                        OR segment3 IS NULL
                           OR segment4 IS NULL
                              OR segment5 IS NULL
                                 OR segment6 IS NULL
                                    OR period_name IS NULL );

        dbms_output.put_line('mandatory columnns null conditions check done');
           
                    xxint.XXINT_XX_I010_COMMON_LOGGING_PKG.db_logging_details_prc(p_log_flag, p_interface_rice_id, p_integration_name, p_parent_instance_id, p_current_instance_id
                    ,
                                          'XXINT_XX_I010_COMMON_LOGGING_PKG.data_validations_prc', 'mandatory columnns null conditions check done', NULL, NULL, NULL,
                                          NULL, NULL);


    EXCEPTION
        WHEN OTHERS THEN
            x_status := 'ERROR';
            x_status_message := 'Unexpected error in updating mandatory columnns null conditions. Error Details - ' || sqlerrm;
            RAISE g_custom_exp;
    END;
       

   -- Calculate the sum of Entered Credit and Debit(Journals are Balanced)

    BEGIN
     lv_total_credit := 0;
            lv_total_debit := 0;
        SELECT
            SUM(entered_credit_amount)
        INTO lv_total_credit
        FROM
            xxint_gl_i003_adp_journals_stg
        WHERE
            current_instance_id = p_current_instance_id;

        SELECT
            SUM(entered_debit_amount)
        INTO lv_total_debit
        FROM
            xxint_gl_i003_adp_journals_stg
        WHERE
            current_instance_id = p_current_instance_id;

    -- Check if the sums are equal 
        IF ( abs(lv_total_debit - lv_total_credit) > 0 ) THEN
            dbms_output.put_line('The Sum of Entered Debit is not equal to Sum of Entered Credit' ||(lv_total_debit - lv_total_credit
            ));
            UPDATE xxint_gl_i003_adp_journals_stg
            SET STATUS= 'INVALID',
                message = message
                          || 'Unbalanced Journal Entries. Entered_CR and Entered_DR should be equal.'
                             || ( lv_total_debit - lv_total_credit )
            WHERE
                current_instance_id = p_current_instance_id;

        ELSE
            dbms_output.put_line('The Sum of Entered Debit is equal to Sum of Entered Credit');
        END IF;
			xxint.XXINT_XX_I010_COMMON_LOGGING_PKG.db_logging_details_prc(p_log_flag, p_interface_rice_id, p_integration_name, p_parent_instance_id, p_current_instance_id
                    ,'XXINT_XX_I010_COMMON_LOGGING_PKG.data_validations_prc', 'Debit and Credit amount balance validation completed', NULL, NULL, NULL,
                                          NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('An error occurred while calculating sums: '
                                 || '->'
                                 || substr(sqlerrm, 1, 3000)
                                 || '->'
                                 || dbms_utility.format_error_backtrace);
    --Logging Error
    xxint.XXINT_XX_I010_COMMON_LOGGING_PKG.db_logging_details_prc(p_log_flag, p_interface_rice_id, p_integration_name, p_parent_instance_id, p_current_instance_id
                    ,'XXINT_XX_I010_COMMON_LOGGING_PKG.data_validations_prc', 'An error occurred while calculating debit and credit total', NULL, NULL, NULL,
                                          NULL, NULL);
    END;
 
		       -- Update remaining records as Valid Records
       UPDATE xxint_gl_i003_adp_journals_stg
        SET
            status = 'VALID'
        WHERE
                current_instance_id = p_current_instance_id
            AND status = 'NEW';
      
      xxint.XXINT_XX_I010_COMMON_LOGGING_PKG.db_logging_details_prc(p_log_flag, p_interface_rice_id, p_integration_name, p_parent_instance_id, p_current_instance_id
                    ,'XXINT_XX_I010_COMMON_LOGGING_PKG.data_validations_prc', 'Procedure Execution Successfully completed at '||systimestamp, NULL, NULL, NULL,
                                          NULL, NULL);
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
            x_status_message := 'Unexpected error in data_validations_prc procedure. Error Details - ' || sqlerrm;
         
         --Logging Error
    xxint.XXINT_XX_I010_COMMON_LOGGING_PKG.db_logging_details_prc(p_log_flag, p_interface_rice_id, p_integration_name, p_parent_instance_id, p_current_instance_id
                    ,'XXINT_XX_I010_COMMON_LOGGING_PKG.data_validations_prc', x_status_message, NULL, NULL, NULL,
                                          NULL, NULL);    

END data_validations_prc;
/******************************************************************************************** 
Version 	Name              	Date           		Version-Description 
---------------------------------------------------------------------------
<1.0>		Priyanka Gupta A   			15-May-2025 	    1.0-Initial Draft
**********************************************************************************************/

    /*****************************************************************
	OBJECT NAME: DB Pagination Procedure
	DESCRIPTION: Procedure to paginate XXINT_GL_I003_ADP_JOURNALS_STG Records 
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Priyanka Gupta A   			15-May-2025  	    1.0- Initial Draft
******************************************************************/

    PROCEDURE create_chunk_prc (
        p_current_instance_id IN VARCHAR2,
        p_batch_limit         IN VARCHAR2,
        p_status              OUT VARCHAR2,
        p_message             OUT VARCHAR2
    ) IS
        ln_group_index NUMBER;
        ln_batch_count NUMBER;
    BEGIN
        SELECT
            COUNT(1)
        INTO ln_batch_count
        FROM
            xxint_gl_i003_adp_journals_stg
        WHERE
            current_instance_id = p_current_instance_id;

        ln_group_index := ceil(ln_batch_count / p_batch_limit);
        IF ln_group_index IS NOT NULL THEN
            FOR i IN 1..ln_group_index LOOP
                UPDATE xxint_gl_i003_adp_journals_stg
                SET
                    chunk_id = i
                WHERE
                        current_instance_id = p_current_instance_id
                    AND chunk_id IS NULL
                    AND ROWNUM <= p_batch_limit;

            END LOOP;
        END IF;

        COMMIT;
        p_status := 'SUCCESS';
        p_message := NULL;
    EXCEPTION
        WHEN OTHERS THEN
            p_status := 'ERROR';
            p_message := 'Error in Creating Chunks'
                         || sqlerrm
                         || ' Code '
                         || sqlcode
                         || ' '
                         || dbms_utility.format_error_backtrace;

    END create_chunk_prc;

END XXINT_GL_I003_ADP_JOURNALS_PKG;

/

  GRANT EXECUTE ON "XXINT"."XXINT_GL_I003_ADP_JOURNALS_PKG" TO "XXINT_RO";
