create or replace PACKAGE       XXINT.XXINT_HCM_ADP_PKG AS
  PROCEDURE get_error_data(
    p_days_back   IN  NUMBER,
    p_person_list OUT VARCHAR2
  );
END XXINT_HCM_ADP_PKG;
