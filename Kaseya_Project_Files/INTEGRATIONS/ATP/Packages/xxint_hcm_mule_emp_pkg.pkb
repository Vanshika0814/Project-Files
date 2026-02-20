create or replace PACKAGE BODY       XXINT.XXINT_HCM_MULE_EMP_PKG IS

    PROCEDURE merge_emp_data (
        p_data    IN emp_data_tab,
        p_status  OUT VARCHAR2,
        p_message OUT VARCHAR2
    ) IS
        v_count NUMBER;
    BEGIN
        FOR i IN 1..p_data.count LOOP
        -- Check if record exists
            SELECT
                COUNT(1)
            INTO v_count
            FROM
                xxint.xxint_hcm_mule_emp_data
            WHERE
                person_id = p_data(i).person_id;

            IF v_count > 0 THEN
            -- Update existing record
                UPDATE xxint.xxint_hcm_mule_emp_data
                SET
                    person_image_id = p_data(i).person_image_id,
                    last_update_date = sysdate,
                    last_updated_by = p_data(i).invoked_by
                WHERE
                    person_id = p_data(i).person_id;

            ELSE
            -- Insert new record
                INSERT INTO xxint.xxint_hcm_mule_emp_data (
                    person_id,
                    person_number,
                    person_image_id,
                    creation_date,
                    created_by,
                    last_update_date,
                    last_updated_by
                ) VALUES (
                    p_data(i).person_id,
                    p_data(i).person_number,
                    p_data(i).person_image_id,
                    sysdate,
                    p_data(i).invoked_by,
                    sysdate,
                    p_data(i).invoked_by
                );

            END IF;

        END LOOP;

        COMMIT;
        p_status := 'SUCCESS';
        p_message := 'Merge completed successfully.';
    EXCEPTION
        WHEN OTHERS THEN
            p_status := 'ERROR';
            p_message := 'Error during merge: ' || sqlerrm;
    END merge_emp_data;

    PROCEDURE split_person_data_by_date (
        p_from_date     IN DATE,
        p_to_date       IN DATE,
        p_group_size    IN NUMBER,
        p_result_cursor OUT SYS_REFCURSOR
    ) IS
    BEGIN
        OPEN p_result_cursor FOR WITH filtered_data AS (
                                     SELECT
                                         person_number,
                                         ROW_NUMBER()
                                         OVER(
                                             ORDER BY
                                                 person_number
                                         ) AS rn
                                     FROM
                                         xxint_hcm_mule_emp_data
                                     WHERE
                                         trunc(last_update_date) BETWEEN trunc(p_from_date) AND trunc(p_to_date)
                                 ), grouped AS (
                                     SELECT
                                         ceil(rn / p_group_size) AS grp,
                                         person_number,
                                         rn
                                     FROM
                                         filtered_data
                                 )
                                 SELECT
                                     LISTAGG(person_number, ',') WITHIN GROUP(
                                     ORDER BY
                                         rn
                                     ) AS group_values
                                 FROM
                                     grouped
                                 GROUP BY
                                     grp
                                 ORDER BY
                                     grp;

    END split_person_data_by_date;

    PROCEDURE split_person_number (
        p_person_number IN VARCHAR2,
        p_group_size    IN NUMBER,
        p_result_cursor OUT SYS_REFCURSOR
    ) IS
    BEGIN
        OPEN p_result_cursor FOR WITH split_values AS (
                                     SELECT
                                         regexp_substr(
                                             p_person_number, '[^,]+', 1, level
                                         )     AS val,
                                         level AS rn
                                     FROM
                                         dual
                                     CONNECT BY
                                         level <= regexp_count(
                                             p_person_number, ','
                                         ) + 1
                                 ), grouped AS (
                                     SELECT
                                         ceil(rn / p_group_size) AS grp,
                                         val,
                                         rn
                                     FROM
                                         split_values
                                 )
                                 SELECT
                                     LISTAGG(val, ',') WITHIN GROUP(
                                     ORDER BY
                                         rn
                                     ) AS group_values
                                 FROM
                                     grouped
                                 GROUP BY
                                     grp
                                 ORDER BY
                                     grp;

    END split_person_number;

END XXINT_HCM_MULE_EMP_PKG;