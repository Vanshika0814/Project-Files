/**************************************************************
    NAME              :     XXAUD_XX_I024_OCI_AUDIT_LOG_INSERT_PKG
    Type           :     Package Specification
	
	 Developer          Date         Version     Comments and changes made
	 -------------      ------      ----------  -------------------------------------------
	 Vaishnavi	       11/18/2025      1.0		   Intial Development
    **************************************************************/
create or replace PACKAGE XXAUD.XXAUD_XX_I024_OCI_AUDIT_LOG_INSERT_PKG AS
    PROCEDURE load_logs_from_payload (
        p_rows OUT PLS_INTEGER
    );

END XXAUD_XX_I024_OCI_AUDIT_LOG_INSERT_PKG;