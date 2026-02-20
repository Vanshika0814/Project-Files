create or replace PACKAGE BODY       XXINT.XXINT_HCM_ADP_PKG AS

  PROCEDURE get_error_data(
    p_days_back   IN  NUMBER,
    p_person_list OUT VARCHAR2
  ) IS
  BEGIN
    SELECT LISTAGG(PERSON_NUMBER, ',') WITHIN GROUP (ORDER BY PERSON_NUMBER)
    INTO p_person_list
    FROM XXINT_HCM_101_ADP_US_DATA
    WHERE UPPER(ACTION_CODE) = 'HIRE'
      AND UPPER(STATUS) = 'ERROR'
      AND CREATION_DATE >= SYSDATE - p_days_back;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      p_person_list := NULL;
    WHEN OTHERS THEN
      p_person_list := 'ERROR: ' || SQLERRM;
  END get_error_data;

END XXINT_HCM_ADP_PKG;
