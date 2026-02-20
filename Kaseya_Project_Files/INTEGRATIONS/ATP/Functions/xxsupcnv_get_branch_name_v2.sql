
--------------------------------------------------------
--  DDL for Function XXSUPCNV_GET_BRANCH_NAME_V2
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "XXSUPCNV"."XXSUPCNV_GET_BRANCH_NAME_V2" (
    p_country                  IN VARCHAR2,
    p_bank_name               IN VARCHAR2,
    p_bic_code                IN VARCHAR2,
    p_routing_number          IN VARCHAR2,
    p_iban                    IN VARCHAR2,
    p_account_suffix          IN VARCHAR2,
    p_payment_reason_comments IN VARCHAR2,
    p_payee_identifier        IN VARCHAR2
) RETURN VARCHAR2 IS
    v_branch_name   VARCHAR2(4000);
    v_sql           VARCHAR2(4000);
    v_where_clause  VARCHAR2(4000);
    v_count         NUMBER;
    v_error_msg     VARCHAR2(4000) := '';
BEGIN
    -- Country is required
    IF p_country IS NULL THEN
        RETURN 'ERROR: COUNTRY IS REQUIRED';
    END IF;

    -- IBAN presence check
    IF p_country IN ('IE','PL','DE','NL','LB','CH','RO','SK','LT','FI','BE') THEN
        IF p_iban IS NULL THEN
            v_error_msg := v_error_msg || 'ERROR: IBAN REQUIRED|';
        END IF;
    END IF;

    -- Account Suffix check for NZ
    IF p_country = 'NZ' AND p_account_suffix IS NULL THEN
        v_error_msg := v_error_msg || 'ERROR: ACCOUNT_SUFFIX REQUIRED|';
    END IF;

    -- Payment Reason Comments check
    IF p_country IN ('AE','BH','CN','IN','MY','OM','PH','QA','RU','TH') THEN
        IF p_payment_reason_comments IS NULL THEN
            v_error_msg := v_error_msg || 'ERROR: PAYMENT_REASON_COMMENTS REQUIRED|';
        END IF;
    END IF;

    -- Payee Identifier check
    IF p_payee_identifier IS NULL THEN
        v_error_msg := v_error_msg || 'ERROR: PAYEE_IDENTIFIER REQUIRED|';
    ELSE
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM ibytempextpayees_cleaned
            WHERE payee_identifier = p_payee_identifier;

            IF v_count = 0 THEN
                v_error_msg := v_error_msg || 'ERROR: INVALID PAYEE_IDENTIFIER|';
            ELSIF v_count > 1 THEN
                v_error_msg := v_error_msg || 'ERROR: DUPLICATE PAYEE_IDENTIFIER|';
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN 'ERROR: PAYEE_IDENTIFIER CHECK FAILED - ' || SQLERRM;
        END;
    END IF;

    -- Validate each input independently
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM lexis_nexis_data
        WHERE country = p_country;
        IF v_count = 0 THEN
            v_error_msg := v_error_msg || 'ERROR: INVALID COUNTRY|';
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 'ERROR: COUNTRY VALIDATION FAILED - ' || SQLERRM;
    END;

    IF p_bank_name IS NOT NULL THEN
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM lexis_nexis_data
            WHERE upper(name) = upper(p_bank_name);
            IF v_count = 0 THEN
                v_error_msg := v_error_msg || 'ERROR: INVALID BANK_NAME|';
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN 'ERROR: BANK_NAME VALIDATION FAILED - ' || SQLERRM;
        END;
    END IF;

    IF p_bic_code IS NOT NULL THEN
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM lexis_nexis_data
            WHERE bic = p_bic_code;
            IF v_count = 0 THEN
                v_error_msg := v_error_msg || 'ERROR: INVALID BIC_CODE|';
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN 'ERROR: BIC_CODE VALIDATION FAILED - ' || SQLERRM;
        END;
    END IF;

    IF p_routing_number IS NOT NULL THEN
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM lexis_nexis_data
            WHERE natid = p_routing_number;
            IF v_count = 0 THEN
                v_error_msg := v_error_msg || 'ERROR: INVALID ROUTING_NUMBER|';
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN 'ERROR: ROUTING_NUMBER VALIDATION FAILED - ' || SQLERRM;
        END;
    END IF;

    -- Return errors if any
    IF v_error_msg IS NOT NULL THEN
        RETURN RTRIM(v_error_msg, '|');
    END IF;

    -- Build dynamic WHERE clause
    v_where_clause := 'WHERE country = ''' || REPLACE(p_country, '''', '''''') || '''';

    IF p_bank_name IS NOT NULL THEN
        v_where_clause := v_where_clause || ' AND upper(name) = ''' || REPLACE(upper(p_bank_name), '''', '''''') || '''';
    END IF;

    IF p_bic_code IS NOT NULL THEN
        v_where_clause := v_where_clause || ' AND nvl(bic,''X'') = ''' || REPLACE(nvl(p_bic_code,'X'), '''', '''''') || '''';
    END IF;

    IF p_routing_number IS NOT NULL THEN
        v_where_clause := v_where_clause || ' AND nvl(natid,''X'') = ''' || REPLACE(nvl(p_routing_number,'X'), '''', '''''') || '''';
    END IF;

    -- Count matching records
    v_sql := '
        SELECT COUNT(DISTINCT KEYVALUE)
        FROM lexis_nexis_data ' || v_where_clause;

    BEGIN
        EXECUTE IMMEDIATE v_sql INTO v_count;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 'ERROR: COUNT EXECUTION FAILED - ' || SQLERRM;
    END;

    IF v_count = 0 THEN
        RETURN 'ERROR: NOT FOUND';
    ELSIF v_count > 1 THEN
        IF p_bank_name IS NOT NULL AND p_bic_code IS NOT NULL AND p_routing_number IS NOT NULL THEN
            -- Return first match
            v_sql := '
                SELECT KEYVALUE
                FROM (
                    SELECT DISTINCT KEYVALUE
                    FROM lexis_nexis_data ' || v_where_clause || '
                    ORDER BY KEYVALUE
                )
                WHERE ROWNUM = 1';
            BEGIN
                EXECUTE IMMEDIATE v_sql INTO v_branch_name;
                RETURN v_branch_name;
            EXCEPTION
                WHEN TOO_MANY_ROWS THEN
                    RETURN 'ERROR: TOO MANY ROWS RETURNED';
                WHEN NO_DATA_FOUND THEN
                    RETURN 'ERROR: NO DATA FOUND';
                WHEN OTHERS THEN
                    RETURN 'ERROR: FETCH FAILED - ' || SQLERRM;
            END;
        ELSE
            RETURN 'ERROR: TOO MANY VALUES';
        END IF;
    ELSE
        -- Only one match expected
        v_sql := '

                SELECT DISTINCT KEYVALUE
                FROM lexis_nexis_data ' || v_where_clause ;
        BEGIN
            EXECUTE IMMEDIATE v_sql INTO v_branch_name;
            RETURN v_branch_name;
        EXCEPTION
            WHEN TOO_MANY_ROWS THEN
                RETURN 'ERROR: TOO MANY ROWS RETURNED';
            WHEN NO_DATA_FOUND THEN
                RETURN 'ERROR: NO DATA FOUND';
            WHEN OTHERS THEN
                RETURN 'ERROR: FETCH FAILED - ' || SQLERRM;
        END;
    END IF;
END;

/
