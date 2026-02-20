--------------------------------------------------------
--  DDL for Function XXSUP_GEO_VALIDATION
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "XXSUPCNV"."XXSUP_GEO_VALIDATION" (
  p_country_code     IN VARCHAR2,
  p_state_province   IN VARCHAR2,
  p_county           IN VARCHAR2,
  p_city             IN VARCHAR2,
  p_postal_code      IN VARCHAR2
) RETURN VARCHAR2 IS
  v_errors VARCHAR2(4000) := '';
  v_combination_exists NUMBER := 0;
BEGIN
  -- Step 1: Check for missing values
  CASE p_country_code
    WHEN 'IN' THEN
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'DE' THEN
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'IL' THEN
      IF p_county IS NULL OR TRIM(p_county) = '' THEN
        v_errors := v_errors || 'Missing County; ';
      END IF;
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'IE' THEN
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'CA' THEN
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'DK' THEN
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'NL' THEN
      IF p_county IS NULL OR TRIM(p_county) = '' THEN
        v_errors := v_errors || 'Missing County; ';
      END IF;
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'US' THEN
      IF p_county IS NULL OR TRIM(p_county) = '' THEN
        v_errors := v_errors || 'Missing County; ';
      END IF;
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'AU' THEN
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'GB' THEN
      IF p_county IS NULL OR TRIM(p_county) = '' THEN
        v_errors := v_errors || 'Missing County; ';
      END IF;
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'PL' THEN
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'SE' THEN
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'CH' THEN
      IF p_county IS NULL OR TRIM(p_county) = '' THEN
        v_errors := v_errors || 'Missing County; ';
      END IF;
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'SG' THEN
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    WHEN 'NZ' THEN
      IF p_postal_code IS NULL OR TRIM(p_postal_code) = '' THEN
        v_errors := v_errors || 'Missing Postal Code; ';
      END IF;
      IF p_state_province IS NULL OR TRIM(p_state_province) = '' THEN
        v_errors := v_errors || 'Missing State/Province; ';
      END IF;
      IF p_city IS NULL OR TRIM(p_city) = '' THEN
        v_errors := v_errors || 'Missing City; ';
      END IF;
    ELSE
      v_errors := v_errors || 'Unsupported country code; ';
  END CASE;
  -- Step 2: Check if individual field values exist
  IF v_errors IS NULL OR v_errors = '' THEN
    CASE p_country_code
      WHEN 'IN' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND (Upper(STATE) = Upper(p_state_province) or Upper(province) = Upper(p_state_province));
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'DE' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(STATE) = Upper(p_state_province);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'IL' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(COUNTY) = Upper(p_county);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid County; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(STATE) = Upper(p_state_province);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'IE' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(STATE) = Upper(p_state_province);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'CA' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND (Upper(STATE) = Upper(p_state_province) or Upper(province) = Upper(p_state_province));
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'DK' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(STATE) = Upper(p_state_province);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'NL' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND upper(COUNTY) = Upper(p_county);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid County; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND (Upper(STATE) = Upper(p_state_province) or Upper(province) = Upper(p_state_province));
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) =Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'US' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(COUNTY) = Upper(p_county);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid County; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(STATE) = Upper(p_state_province);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'AU' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(STATE ) = Upper(p_state_province);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) =Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'GB' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) =Upper(p_country_code) AND Upper(COUNTY) = Upper(p_county);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid County; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'PL' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(STATE ) = Upper(p_state_province);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'SE' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(STATE ) = Upper(p_state_province);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'CH' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(COUNTY) = Upper(p_county);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid County; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(STATE ) = Upper(p_state_province);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'SG' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(STATE ) = Upper(p_state_province);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
      WHEN 'NZ' THEN
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(POSTAL_CODE) = Upper(p_postal_code);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid Postal Code; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND (Upper(STATE ) = Upper(p_state_province) or Upper(province) = Upper(p_state_province));
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid State/Province; ';
        END IF;
        SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
        WHERE Upper(COUNTRY_CODE) = Upper(p_country_code) AND Upper(CITY) = Upper(p_city);
        IF v_combination_exists = 0 THEN
          v_errors := v_errors || 'Invalid City; ';
        END IF;
    END CASE;
  END IF;
  -- Step 3: Check if full combination exists
  IF v_errors IS NULL OR v_errors = '' THEN
    SELECT COUNT(*) INTO v_combination_exists FROM MV_HZ_GEOGRAPHIES_MAPPED
    WHERE Upper(COUNTRY_CODE) =Upper(p_country_code)
      AND (Upper(STATE ) = Upper(p_state_province) OR STATE IS NULL)
      AND (Upper(province) = Upper(p_state_province) OR PROVINCE IS NULL)
      AND (Upper(COUNTY) = Upper(p_county) OR COUNTY IS NULL)
      AND (Upper(CITY) = Upper(p_city) OR CITY IS NULL)
      AND (Upper(POSTAL_CODE) = Upper(p_postal_code) OR POSTAL_CODE IS NULL);
    IF v_combination_exists = 0 THEN
      v_errors := v_errors || 'Invalid combination of fields; ';
    END IF;
  END IF;
  IF v_errors IS NULL OR v_errors = '' THEN
    RETURN 'Valid';
  ELSE
    RETURN 'Invalid: ' || v_errors;
  END IF;
END;

/
