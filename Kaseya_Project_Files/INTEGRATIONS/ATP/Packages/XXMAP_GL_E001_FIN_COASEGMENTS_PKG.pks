create or replace PACKAGE xxmap.xxmap_gl_e001_fin_coasegments_pkg AS
 /******************************************************************************************
	  NAME              :     XXMAP.XXMAP_GL_E001_FIN_COASEGMENTS_PKG SPEC				 	
      PURPOSE           :     SPEC Of procedures for managing Chart of Accounts             
							  (COA) segment data Integration between NetSuite and Oracle.	
    Change History																	    
    Developer        Date         Version     Comments and changes made				    
    -------------   ------       ----------  ----------------------------------------------
    Harish.V        21-06-2025      1.0         Initial Development 
    Harish.V        29-08-2025      1.1         LTCI-8034
   ****************************************************************************************/

    TYPE coa_tmp_rec IS RECORD (
            parent_instance_id VARCHAR2(250),
            ns_segment1        VARCHAR2(250),
            ns_segment2        VARCHAR2(250),
            ns_segment3        VARCHAR2(250),
            ns_segment4        VARCHAR2(250),
            ns_segment5        VARCHAR2(250),
            ns_segment6        VARCHAR2(250),
            ns_segment7        VARCHAR2(250),
            ns_segment8        VARCHAR2(250),
            ns_segment9        VARCHAR2(250),
            ns_segment10       VARCHAR2(250),
            status             VARCHAR2(250),
            creation_date      DATE,
            created_by         VARCHAR2(250),
            last_update_date   DATE,
            last_updated_by    VARCHAR2(250),
            ledger             VARCHAR2(250)
    );
    
    TYPE coa_tmp_rec_tbl IS
        TABLE OF coa_tmp_rec;
        
    PROCEDURE update_ns2oracle_coa_data_prc (
        P_InstnaceId IN VARCHAR2,
        p_status        OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    PROCEDURE insert_processed_coa_values_prc (
        P_InstnaceId IN VARCHAR2,
        P_status        OUT VARCHAR2,
        P_error_message OUT VARCHAR2
    );

    PROCEDURE purgedata_coa_tblmapper_prc (
        p_table_name  IN VARCHAR2,
        status        OUT VARCHAR2,
        error_message OUT VARCHAR2
    );
 
 --code modified for chagnes LTCI-8034
     PROCEDURE insert_tmp_coa_data_prc (
        p_coa_tmp_rec_tbl IN coa_tmp_rec_tbl,
        p_status          OUT VARCHAR2,
        P_inserted_recs    OUT NUMBER
    );

END xxmap_gl_e001_fin_coasegments_pkg;