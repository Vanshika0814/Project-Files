create or replace PACKAGE BODY             XXINT.XXINT_HCM_110_MOOREPAY_PKG AS
    PROCEDURE Get_Error_Person_Numbers (
        p_days           IN  NUMBER,
        p_person_list    OUT VARCHAR2,
        p_status         OUT VARCHAR2,
        p_error_message  OUT VARCHAR2
    )
    IS
        v_list VARCHAR2(32767);
    BEGIN
        v_list := NULL;

        BEGIN
            FOR rec IN (
                SELECT PERSON_NUMBER
                FROM (
                    SELECT PERSON_NUMBER,
                           STATUS,
                           ROW_NUMBER() OVER (
                               PARTITION BY PERSON_NUMBER
                               ORDER BY CREATION_DATE DESC,
                                        LAST_UPDATE_DATE DESC
                           ) AS rn
                    FROM XXINT.XXINT_HCM_110_MOOREPAY_DATA_STATUS
                    WHERE CREATION_DATE BETWEEN SYSTIMESTAMP - NUMTODSINTERVAL(p_days, 'DAY')
                                            AND SYSTIMESTAMP
                )
                WHERE rn = 1
                  AND STATUS = 'ERROR'
                ORDER BY PERSON_NUMBER
            )
            LOOP
                IF v_list IS NOT NULL THEN
                    v_list := v_list || ',';
                END IF;
                v_list := v_list || rec.PERSON_NUMBER;
            END LOOP;


            IF v_list IS NULL THEN
                p_person_list := '';
                p_status := 'WARNING';
                p_error_message := 'No persons found with ERROR status in the given date range.';
            ELSE
                p_person_list := v_list;
                p_status := 'SUCCESS';
                p_error_message := 'Person numbers retrieved successfully.';
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                p_person_list := NULL;
                p_status := 'ERROR';
                p_error_message := 'Error while retrieving person numbers: ' || SQLERRM;
        END;
    END Get_Error_Person_Numbers;

END XXINT_HCM_110_MOOREPAY_PKG;