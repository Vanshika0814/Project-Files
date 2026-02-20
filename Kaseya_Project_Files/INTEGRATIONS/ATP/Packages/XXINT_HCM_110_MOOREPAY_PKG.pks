create or replace PACKAGE    XXINT.XXINT_HCM_110_MOOREPAY_PKG AS
    PROCEDURE Get_Error_Person_Numbers (
        p_days           IN  NUMBER,
        p_person_list    OUT VARCHAR2,
        p_status         OUT VARCHAR2,
        p_error_message  OUT VARCHAR2
    );
END XXINT_HCM_110_MOOREPAY_PKG;