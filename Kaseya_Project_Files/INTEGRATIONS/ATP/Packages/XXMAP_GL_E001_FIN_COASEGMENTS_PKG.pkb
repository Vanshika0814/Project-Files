CREATE OR REPLACE PACKAGE BODY xxmap.xxmap_gl_e001_fin_coasegments_pkg AS

  /*************************************************************************************************
    NAME              :     XXMAP.XXMAP_GL_E001_FIN_COASEGMENTS_PKG body				 	
    PURPOSE           :     This package is the detailed body of all the procedures.      
    Change History																	    
    Developer        Date         Version     Comments and changes made				     
    -------------   ------       ----------  ----------------------------
    Harish.V        21-05-2025      1.0         Initial Development 	
    Harish.V        05-06-2025      1.1         CR #39. Balance Accounts
    Harish.V        30-07-2025      1.2         LTCI-6261 CR #39 exclude accounts for cost center default values.
    Harish.V        29-08-2025      1.3         LTCI-8034
	Harish.V        17-09-2025      1.4         LTCI-7468
    ***********************************************************************************************/

    PROCEDURE update_ns2oracle_coa_data_prc (
        p_instnaceid    IN VARCHAR2,
        p_status        OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS

        CURSOR c_data IS
        SELECT
            *
        FROM
            xxmap.xxmap_gl_e001_temp_coa_comb_data
        WHERE
            parent_instance_id = p_instnaceid AND STATUS='NEW';  --Added Status criteria #LTCI-7468

        TYPE status_table_type IS
            TABLE OF xxmap.xxmap_gl_e001_temp_coa_comb_data%rowtype;
        v_status_table   status_table_type;
        v_count          NUMBER;
        v_ns_attribute_1 VARCHAR2(100);
        v_ns_attribute_2 VARCHAR2(100);
        v_status         VARCHAR2(100);
        v_msg            VARCHAR2(1000);
        v_found          BOOLEAN := FALSE;
        v_rec_cnt        NUMBER;
    BEGIN

     -- Fetch all rows into the table type
        OPEN c_data;
        FETCH c_data
        BULK COLLECT INTO v_status_table;
        CLOSE c_data;
        FOR i IN v_status_table.first..v_status_table.last LOOP
            v_found := FALSE;  -- Reset for each record
            v_status := '';
            v_msg := '';
            BEGIN
            -- Check if ns_segment4 is NULL and assign default value
                IF
                    v_status_table(i).ns_segment4 IS NOT NULL
                    AND regexp_like(v_status_table(i).ns_segment4,
                                    '^[0-9]+$')
                THEN
                -- Check if ns_segment4 is a number 
                    SELECT
                        COUNT(*),
                        MAX(t4.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_1
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_account t4
                    WHERE
                        v_status_table(i).ns_segment4 = t4.ns_account_attribute_1;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment4 := v_ns_attribute_1;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS4:'
                                   || v_status_table(i).ns_segment4
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS4:'
                                   || v_status_table(i).ns_segment4
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                ELSIF
                    v_status_table(i).ns_segment4 IS NOT NULL
                    AND NOT regexp_like(v_status_table(i).ns_segment4,
                                        '^[0-9]+$')
                THEN
                -- Check if ns_segment4 is not a number 
                    SELECT
                        COUNT(*),
                        MAX(t4.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_2
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_account t4
                    WHERE
                        v_status_table(i).ns_segment4 = t4.ns_account_attribute_2;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment4 := v_ns_attribute_2;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS4:'
                                   || v_status_table(i).ns_segment4
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS4:'
                                   || v_status_table(i).ns_segment4
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred: ' || sqlerrm);
            END;

            BEGIN
            -- Check if ns_segment1 is NULL and assign default value
            -- dbms_output.put_line('VALUE: ' || v_status_table(i).ns_segment1);
                IF
                    v_status_table(i).ns_segment1 IS NOT NULL
                    AND regexp_like(v_status_table(i).ns_segment1,
                                    '^[0-9]+$')
                THEN
                -- Check if ns_segment1 is a number 
                    SELECT
                        COUNT(*),
                        MAX(t1.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_1
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_company t1
                    WHERE
                        v_status_table(i).ns_segment1 = t1.ns_company_attribute_1;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment1 := v_ns_attribute_1;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS1:'
                                   || v_status_table(i).ns_segment1
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS1:'
                                   || v_status_table(i).ns_segment1
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                ELSIF
                    v_status_table(i).ns_segment1 IS NOT NULL
                    AND NOT regexp_like(v_status_table(i).ns_segment1,
                                        '^[0-9]+$')
                THEN
                -- Check if ns_segment1 is not a number 
                    SELECT
                        COUNT(*),
                        MAX(t1.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_2
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_company t1
                    WHERE
                        v_status_table(i).ns_segment1 = t1.ns_company_attribute_2;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment1 := v_ns_attribute_2;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS1:'
                                   || v_status_table(i).ns_segment1
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS1:'
                                   || v_status_table(i).ns_segment1
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred: ' || sqlerrm);
            END;

            BEGIN
            -- Check if ns_segment2 is NULL and assign default value

                IF
                    v_status_table(i).oc_segment4 IS NOT NULL
                    AND v_status_table(i).oc_segment4 < 400000
                OR v_status_table(i).ns_segment2 IS NULL THEN
            --dbms_output.put_line('EMPTYVALUE: ' || v_status_table(i).xxmap_coa_seq_id);
                    v_status_table(i).oc_segment2 := '999';
                ELSIF
                    v_status_table(i).ns_segment2 IS NOT NULL
                    AND regexp_like(v_status_table(i).ns_segment2,
                                    '^[0-9]+$')
                THEN
                -- Check if ns_segment2 is a number 
                    SELECT
                        COUNT(*),
                        MAX(t2.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_1
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_divison t2
                    WHERE
                        v_status_table(i).ns_segment2 = t2.ns_divison_attribute_1;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment2 := v_ns_attribute_1;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS2:'
                                   || v_status_table(i).ns_segment2
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS2:'
                                   || v_status_table(i).ns_segment2
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                ELSIF
                    v_status_table(i).ns_segment2 IS NOT NULL
                    AND NOT regexp_like(v_status_table(i).ns_segment2,
                                        '^[0-9]+$')
                THEN
                -- Check if ns_segment2 is not a number and exists in costcenter_attribute_2
                    SELECT
                        COUNT(*),
                        MAX(t2.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_2
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_divison t2
                    WHERE
                        v_status_table(i).ns_segment2 = t2.ns_divison_attribute_2;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment2 := v_ns_attribute_2;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS2:'
                                   || v_status_table(i).ns_segment2
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS2:'
                                   || v_status_table(i).ns_segment2
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred: ' || sqlerrm);
            END;

       --code modified here to handle the Balance sheet chagnes CR #39--
            BEGIN
             -- Check Case3  Cost_CENTER
                v_rec_cnt := 0;
                SELECT
                    COUNT(1)
                INTO v_rec_cnt
                FROM
                    xxmap.xxmap_gl_e001_exclude_accounts
                WHERE
                    account_no = v_status_table(i).oc_segment4;

                IF (
                    v_status_table(i).oc_segment4 IS NOT NULL
                    AND v_status_table(i).oc_segment4 < 400000
                    AND v_rec_cnt = 0
                )
                OR v_status_table(i).ns_segment3 IS NULL THEN
                    v_status_table(i).oc_segment3 := '99999';
                    v_found := TRUE;
                ELSE
        -- CASE 1
                    IF
                        regexp_like(v_status_table(i).ns_segment3,
                                    '^[0-9]+$')
                        AND regexp_like(v_status_table(i).ns_segment4,
                                        '^[0-9]+$')
                    THEN
                        SELECT
                            COUNT(*),
                            MAX(tc.erp_coa_value)
                        INTO
                            v_count,
                            v_ns_attribute_1
                        FROM
                            xxmap.xxmap_gl_e001_kaseya_ns_acctcc tc
                        WHERE
                                v_status_table(i).ns_segment3 = tc.ns_costcenter_attribute_1
                            AND v_status_table(i).ns_segment4 = tc.ns_account_attribute_1;

                        IF v_count = 1 THEN
                            v_status_table(i).oc_segment3 := v_ns_attribute_1;
                            v_found := TRUE;
                        END IF;

                    END IF;
        -- CASE 2
                    IF
                        NOT v_found
                        AND NOT regexp_like(v_status_table(i).ns_segment3,
                                            '^[0-9]+$')
                        AND NOT regexp_like(v_status_table(i).ns_segment4,
                                            '^[0-9]+$')
                    THEN
                        SELECT
                            COUNT(*),
                            MAX(tc.erp_coa_value)
                        INTO
                            v_count,
                            v_ns_attribute_1
                        FROM
                            xxmap.xxmap_gl_e001_kaseya_ns_acctcc tc
                        WHERE
                                v_status_table(i).ns_segment3 = tc.ns_costcenter_attribute_2
                            AND v_status_table(i).ns_segment4 = tc.ns_account_attribute_2;

                        IF v_count = 1 THEN
                            v_status_table(i).oc_segment3 := v_ns_attribute_1;
                            v_found := TRUE;
                        END IF;

                    END IF;
        -- CASE 3
                    IF
                        NOT v_found
                        AND regexp_like(v_status_table(i).ns_segment3,
                                        '^[0-9]+$')
                        AND NOT regexp_like(v_status_table(i).ns_segment4,
                                            '^[0-9]+$')
                    THEN
                        SELECT
                            COUNT(*),
                            MAX(tc.erp_coa_value)
                        INTO
                            v_count,
                            v_ns_attribute_1
                        FROM
                            xxmap.xxmap_gl_e001_kaseya_ns_acctcc tc
                        WHERE
                                v_status_table(i).ns_segment3 = tc.ns_costcenter_attribute_1
                            AND v_status_table(i).ns_segment4 = tc.ns_account_attribute_2;

                        IF v_count = 1 THEN
                            v_status_table(i).oc_segment3 := v_ns_attribute_1;
                            v_found := TRUE;
                        END IF;

                    END IF;
        -- CASE 4
                    IF
                        NOT v_found
                        AND NOT regexp_like(v_status_table(i).ns_segment3,
                                            '^[0-9]+$')
                        AND regexp_like(v_status_table(i).ns_segment4,
                                        '^[0-9]+$')
                    THEN
                        SELECT
                            COUNT(*),
                            MAX(tc.erp_coa_value)
                        INTO
                            v_count,
                            v_ns_attribute_1
                        FROM
                            xxmap.xxmap_gl_e001_kaseya_ns_acctcc tc
                        WHERE
                                v_status_table(i).ns_segment3 = tc.ns_costcenter_attribute_2
                            AND v_status_table(i).ns_segment4 = tc.ns_account_attribute_1;

                        IF v_count = 1 THEN
                            v_status_table(i).oc_segment3 := v_ns_attribute_1;
                            v_found := TRUE;
                        END IF;

                    END IF;
        -- CASE 5
                    IF
                        NOT v_found
                        AND regexp_like(v_status_table(i).ns_segment3,
                                        '^[0-9]+$')
                    THEN
                        SELECT
                            COUNT(*),
                            MAX(t3.erp_coa_value)
                        INTO
                            v_count,
                            v_ns_attribute_1
                        FROM
                            xxmap.xxmap_gl_e001_kaseya_ns_costcenter t3
                        WHERE
                            v_status_table(i).ns_segment3 = t3.ns_costcenter_attribute_1;

                        IF v_count = 1 THEN
                            v_status_table(i).oc_segment3 := v_ns_attribute_1;
                            v_found := TRUE;
                        END IF;

                    END IF;
        -- CASE 6
                    IF
                        NOT v_found
                        AND NOT regexp_like(v_status_table(i).ns_segment3,
                                            '^[0-9]+$')
                    THEN
                        SELECT
                            COUNT(*),
                            MAX(t3.erp_coa_value)
                        INTO
                            v_count,
                            v_ns_attribute_2
                        FROM
                            xxmap.xxmap_gl_e001_kaseya_ns_costcenter t3
                        WHERE
                            v_status_table(i).ns_segment3 = t3.ns_costcenter_attribute_2;

                        IF v_count = 1 THEN
                            v_status_table(i).oc_segment3 := v_ns_attribute_2;
                            v_found := TRUE;
                        END IF;

                    END IF;

                END IF; -- End of all CASE IF/ELSIFs

    -- If none of the cases matched, set error status/message
                IF NOT v_found THEN
                    v_status := 'ERROR';
                    v_msg := 'No valid mapping found for NS_Segment3: ' || v_status_table(i).ns_segment3;
                    v_status_table(i).status := v_status;
                    v_status_table(i).message := v_msg;
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred: ' || sqlerrm);
            END;

            BEGIN
            -- Check if ns_segment5 is NULL and assign default value
                IF v_status_table(i).ns_segment5 IS NULL THEN
                    v_status_table(i).oc_segment5 := '9999';
                ELSIF
                    v_status_table(i).ns_segment5 IS NOT NULL
                    AND regexp_like(v_status_table(i).ns_segment5,
                                    '^[0-9]+$')
                THEN
                -- Check if ns_segment5 is a number and exists in costcenter_attribute_1
                    SELECT
                        COUNT(*),
                        MAX(t5.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_1
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_productline t5
                    WHERE
                        v_status_table(i).ns_segment5 = t5.ns_productline_attribute_1;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment5 := v_ns_attribute_1;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS5:'
                                   || v_status_table(i).ns_segment5
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS5:'
                                   || v_status_table(i).ns_segment5
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                ELSIF
                    v_status_table(i).ns_segment5 IS NOT NULL
                    AND NOT regexp_like(v_status_table(i).ns_segment5,
                                        '^[0-9]+$')
                THEN
                -- Check if ns_segment5 is not a number and exists in costcenter_attribute_2
                    SELECT
                        COUNT(*),
                        MAX(t5.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_2
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_productline t5
                    WHERE
                        v_status_table(i).ns_segment5 = t5.ns_productline_attribute_2;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment5 := v_ns_attribute_2;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS5:'
                                   || v_status_table(i).ns_segment5
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS5:'
                                   || v_status_table(i).ns_segment5
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred: ' || sqlerrm);
            END;

            BEGIN
            -- Check if ns_segment6 is NULL and assign default value
                IF v_status_table(i).ns_segment6 IS NULL THEN
                    v_status_table(i).oc_segment6 := '999999';
                ELSIF
                    v_status_table(i).ns_segment6 IS NOT NULL
                    AND regexp_like(v_status_table(i).ns_segment6,
                                    '^[0-9]+$')
                THEN
                -- Check if ns_segment6 is a number and exists in costcenter_attribute_1
                    SELECT
                        COUNT(*),
                        MAX(t6.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_1
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_location t6
                    WHERE
                        v_status_table(i).ns_segment6 = t6.ns_location_attribute_1;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment6 := v_ns_attribute_1;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS6:'
                                   || v_status_table(i).ns_segment6
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS6:'
                                   || v_status_table(i).ns_segment6
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                ELSIF
                    v_status_table(i).ns_segment6 IS NOT NULL
                    AND NOT regexp_like(v_status_table(i).ns_segment6,
                                        '^[0-9]+$')
                THEN
                -- Check if ns_segment6 is not a number and exists in costcenter_attribute_2
                    SELECT
                        COUNT(*),
                        MAX(t6.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_2
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_location t6
                    WHERE
                        v_status_table(i).ns_segment6 = t6.ns_location_attribute_2;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment6 := v_ns_attribute_2;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS6:'
                                   || v_status_table(i).ns_segment6
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS6:'
                                   || v_status_table(i).ns_segment6
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred: ' || sqlerrm);
            END;

            BEGIN
            -- Check if ns_segment7 is NULL and assign default value
                IF v_status_table(i).ns_segment7 IS NULL THEN
                    v_status_table(i).oc_segment7 := '9999';
                ELSIF
                    v_status_table(i).ns_segment7 IS NOT NULL
                    AND regexp_like(v_status_table(i).ns_segment7,
                                    '^[0-9]+$')
                THEN
                -- Check if ns_segment7 is a number and exists in costcenter_attribute_1
                    SELECT
                        COUNT(*),
                        MAX(t7.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_1
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_intercompany t7
                    WHERE
                        v_status_table(i).ns_segment7 = t7.ns_intercompany_attribute_1;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment7 := v_ns_attribute_1;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS7:'
                                   || v_status_table(i).ns_segment7
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS7:'
                                   || v_status_table(i).ns_segment7
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                ELSIF
                    v_status_table(i).ns_segment7 IS NOT NULL
                    AND NOT regexp_like(v_status_table(i).ns_segment7,
                                        '^[0-9]+$')
                THEN
                -- Check if ns_segment7 is not a number and exists in costcenter_attribute_2
                    SELECT
                        COUNT(*),
                        MAX(t7.erp_coa_value)
                    INTO
                        v_count,
                        v_ns_attribute_2
                    FROM
                        xxmap.xxmap_gl_e001_kaseya_ns_intercompany t7
                    WHERE
                        v_status_table(i).ns_segment7 = t7.ns_intercompany_attribute_2;

                    IF v_count = 1 THEN
                        v_status_table(i).oc_segment7 := v_ns_attribute_2;
                    ELSIF v_count > 1 THEN
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Duplicate entries found for the NS7:'
                                   || v_status_table(i).ns_segment7
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    ELSE
                        v_status := 'ERROR';
                        v_msg := ( v_msg
                                   || 'Mapping set value not present for the NS7:'
                                   || v_status_table(i).ns_segment7
                                   || '.' );

                        v_status_table(i).status := v_status;
                        v_status_table(i).message := v_msg;
                    END IF;

                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    dbms_output.put_line('An error occurred: ' || sqlerrm);
            END;

        END LOOP;

        UPDATE xxmap.xxmap_gl_e001_temp_coa_comb_data
        SET
            oc_segment8 = '9999';

        COMMIT;
        UPDATE xxmap.xxmap_gl_e001_temp_coa_comb_data
        SET
            oc_segment9 = '9999';

        COMMIT;
        UPDATE xxmap.xxmap_gl_e001_temp_coa_comb_data
        SET
            oc_segment10 = '999999';

        COMMIT;

        -- Perform the update in bulk
        FORALL i IN v_status_table.first..v_status_table.last
            UPDATE xxmap.xxmap_gl_e001_temp_coa_comb_data xgl1
            SET
                oc_segment1 = v_status_table(i).oc_segment1,
                oc_segment2 = v_status_table(i).oc_segment2,
                oc_segment3 = v_status_table(i).oc_segment3,
                oc_segment4 = v_status_table(i).oc_segment4,
                oc_segment5 = v_status_table(i).oc_segment5,
                oc_segment6 = v_status_table(i).oc_segment6,
                oc_segment7 = v_status_table(i).oc_segment7,
            -- Repeat for other segments

                status =
                    CASE
                        WHEN v_status_table(i).status = 'ERROR' THEN
                            v_status_table(i).status
                        ELSE
                            'NEW'
                    END,
                message = v_status_table(i).message
            WHERE
           -- xgl1.ns_segment1 = v_status_table(i).ns_segment1
            --AND 
                xgl1.xxmap_coa_seq_id = v_status_table(i).xxmap_coa_seq_id;

        p_status := 'SUCCESS';
        p_error_message := NULL;
    EXCEPTION
        WHEN OTHERS THEN
            p_status := 'ERROR';
            p_error_message := sqlerrm();
    END update_ns2oracle_coa_data_prc;

 --code modified for chagnes LTCI-8034

    PROCEDURE insert_processed_coa_values_prc (
        p_instnaceid    IN VARCHAR2,
        p_status        OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS

        p_flag          VARCHAR2(10) := 'Y';
        p_created_by    VARCHAR2(100) := user;
        p_creation_date DATE := sysdate;
        p_updated_by    VARCHAR2(100) := user;
        p_update_date   DATE := sysdate;

    -- Cursor for temp table rows
        CURSOR c_temp IS
        SELECT
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
            oc_segment1,
            oc_segment2,
            oc_segment3,
            oc_segment4,
            oc_segment5,
            oc_segment6,
            oc_segment7,
            oc_segment8,
            oc_segment9,
            oc_segment10
        FROM
            xxmap.xxmap_gl_e001_temp_coa_comb_data
        WHERE
                parent_instance_id = p_instnaceid
            AND status = 'PROCESSED';

        v_exists        NUMBER;
    BEGIN
        FOR rec IN c_temp LOOP
        -- Check if target row exists
            SELECT
                COUNT(*)
            INTO v_exists
            FROM
                xxmap.xxmap_gl_e001_coa_nserp_data
            WHERE
                    ns_segment1 = rec.ns_segment1
                AND ns_segment2 = rec.ns_segment2
                AND ns_segment3 = rec.ns_segment3
                AND ns_segment4 = rec.ns_segment4
                AND ns_segment5 = rec.ns_segment5
                AND ns_segment6 = rec.ns_segment6
                AND ns_segment7 = rec.ns_segment7
                AND ns_segment8 = rec.ns_segment8
                AND ns_segment9 = rec.ns_segment9
                AND ns_segment10 = rec.ns_segment10;

            IF v_exists > 0 THEN
            -- Update existing row
                UPDATE xxmap.xxmap_gl_e001_coa_nserp_data
                SET
                    enabled_flag = p_flag,
                    erp_segment1 = rec.oc_segment1,
                    erp_segment2 = rec.oc_segment2,
                    erp_segment3 = rec.oc_segment3,
                    erp_segment4 = rec.oc_segment4,
                    erp_segment5 = rec.oc_segment5,
                    erp_segment6 = rec.oc_segment6,
                    erp_segment7 = rec.oc_segment7,
                    erp_segment8 = rec.oc_segment8,
                    erp_segment9 = rec.oc_segment9,
                    erp_segment10 = rec.oc_segment10,
                    last_updated_by = p_updated_by,
                    last_update_date = p_update_date
                WHERE
                        ns_segment1 = rec.ns_segment1
                    AND ns_segment2 = rec.ns_segment2
                    AND ns_segment3 = rec.ns_segment3
                    AND ns_segment4 = rec.ns_segment4
                    AND ns_segment5 = rec.ns_segment5
                    AND ns_segment6 = rec.ns_segment6
                    AND ns_segment7 = rec.ns_segment7
                    AND ns_segment8 = rec.ns_segment8
                    AND ns_segment9 = rec.ns_segment9
                    AND ns_segment10 = rec.ns_segment10;

            ELSE
            -- Insert new row
                INSERT INTO xxmap.xxmap_gl_e001_coa_nserp_data (
                    enabled_flag,
                    erp_segment1,
                    erp_segment2,
                    erp_segment3,
                    erp_segment4,
                    erp_segment5,
                    erp_segment6,
                    erp_segment7,
                    erp_segment8,
                    erp_segment9,
                    erp_segment10,
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
                    created_by,
                    creation_date,
                    last_updated_by,
                    last_update_date
                ) VALUES ( p_flag,
                           rec.oc_segment1,
                           rec.oc_segment2,
                           rec.oc_segment3,
                           rec.oc_segment4,
                           rec.oc_segment5,
                           rec.oc_segment6,
                           rec.oc_segment7,
                           rec.oc_segment8,
                           rec.oc_segment9,
                           rec.oc_segment10,
                           rec.ns_segment1,
                           rec.ns_segment2,
                           rec.ns_segment3,
                           rec.ns_segment4,
                           rec.ns_segment5,
                           rec.ns_segment6,
                           rec.ns_segment7,
                           rec.ns_segment8,
                           rec.ns_segment9,
                           rec.ns_segment10,
                           p_created_by,
                           p_creation_date,
                           p_updated_by,
                           p_update_date );

            END IF;

        END LOOP;

        COMMIT;
        p_status := 'SUCCESS';
        p_error_message := NULL;
    EXCEPTION
        WHEN OTHERS THEN
            p_status := 'ERROR';
            p_error_message := sqlerrm;
            UPDATE xxmap.xxmap_gl_e001_temp_coa_comb_data
            SET
                status = p_status,
                message = p_error_message
            WHERE
                parent_instance_id = p_instnaceid;

    END insert_processed_coa_values_prc;

    PROCEDURE purgedata_coa_tblmapper_prc (
        p_table_name  IN VARCHAR2,
        status        OUT VARCHAR2,
        error_message OUT VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
    BEGIN
        v_sql := 'TRUNCATE TABLE ' || p_table_name;
        EXECUTE IMMEDIATE v_sql;
        status := 'SUCCESS';
        error_message := NULL;
    EXCEPTION
        WHEN OTHERS THEN
            status := 'ERROR';
            error_message := sqlerrm;
    END purgedata_coa_tblmapper_prc;

 --code modified for chagnes LTCI-8034

    PROCEDURE insert_tmp_coa_data_prc (
        p_coa_tmp_rec_tbl IN coa_tmp_rec_tbl,
        p_status          OUT VARCHAR2,
        p_inserted_recs   OUT NUMBER
    ) IS
    BEGIN
        FOR i IN 1..p_coa_tmp_rec_tbl.count LOOP
        -- Check if NS_SEGMENT1-10 combination exists in the target table
            DECLARE
                v_exists NUMBER;
            BEGIN
                SELECT
                    COUNT(*)
                INTO v_exists
                FROM
                    xxmap.xxmap_gl_e001_coa_nserp_data
                WHERE
                        ns_segment1 = p_coa_tmp_rec_tbl(i).ns_segment1
                    AND ns_segment2 = p_coa_tmp_rec_tbl(i).ns_segment2
                    AND ns_segment3 = p_coa_tmp_rec_tbl(i).ns_segment3
                    AND ns_segment4 = p_coa_tmp_rec_tbl(i).ns_segment4
                    AND ns_segment5 = p_coa_tmp_rec_tbl(i).ns_segment5
                    AND ns_segment6 = p_coa_tmp_rec_tbl(i).ns_segment6
                    AND ns_segment7 = p_coa_tmp_rec_tbl(i).ns_segment7
                    AND ns_segment8 = p_coa_tmp_rec_tbl(i).ns_segment8
                    AND ns_segment9 = p_coa_tmp_rec_tbl(i).ns_segment9
                    AND ns_segment10 = p_coa_tmp_rec_tbl(i).ns_segment10;

                IF v_exists = 0 THEN
                    INSERT INTO xxmap.xxmap_gl_e001_temp_coa_comb_data (
                        parent_instance_id,
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
                        status,
                        creation_date,
                        created_by,
                        last_update_date,
                        last_updated_by,
                        ledger
                    ) VALUES ( p_coa_tmp_rec_tbl(i).parent_instance_id,
                               p_coa_tmp_rec_tbl(i).ns_segment1,
                               p_coa_tmp_rec_tbl(i).ns_segment2,
                               p_coa_tmp_rec_tbl(i).ns_segment3,
                               p_coa_tmp_rec_tbl(i).ns_segment4,
                               p_coa_tmp_rec_tbl(i).ns_segment5,
                               p_coa_tmp_rec_tbl(i).ns_segment6,
                               p_coa_tmp_rec_tbl(i).ns_segment7,
                               p_coa_tmp_rec_tbl(i).ns_segment8,
                               p_coa_tmp_rec_tbl(i).ns_segment9,
                               p_coa_tmp_rec_tbl(i).ns_segment10,
                               p_coa_tmp_rec_tbl(i).status,
                               p_coa_tmp_rec_tbl(i).creation_date,
                               p_coa_tmp_rec_tbl(i).created_by,
                               p_coa_tmp_rec_tbl(i).last_update_date,
                               p_coa_tmp_rec_tbl(i).last_updated_by,
                               p_coa_tmp_rec_tbl(i).ledger );

                END IF;

            END;
        END LOOP;

        SELECT
            COUNT(*)
        INTO p_inserted_recs
        FROM
            xxmap.xxmap_gl_e001_temp_coa_comb_data
        WHERE
            parent_instance_id = p_coa_tmp_rec_tbl(1).parent_instance_id;

        p_status := 'Completed';
    EXCEPTION
        WHEN OTHERS THEN
            p_status := 'Error: ' || sqlerrm;
    END;

END xxmap_gl_e001_fin_coasegments_pkg;