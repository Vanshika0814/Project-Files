create or replace PACKAGE       xxcnv.xxcnv_gl_coa_transformation_pkg IS

    /**************************************************************
    NAME              :     GL_COA_SEGMENT_MAPPING_PKG
    PURPOSE           :     SPEC Of Procedures
    -- Modification History
    -- Developer        Date         Version     Comments and changes made
    -- -------------   ------       ----------  -------------------------------------------
    -- Priyanka Kadam  05-Mar-2025     1.0         Initial Development
    -- Priyanka Kadam  29-Jul-2025     1.1         Added changes for JIRA ID-6261
    **************************************************************/

    PROCEDURE   coa_segment_mapping_prc (

				p_in_segment1 IN VARCHAR2,
				p_in_segment2 IN VARCHAR2,
				p_in_segment3 IN VARCHAR2,
				p_in_segment4 IN VARCHAR2,
				p_in_segment5 IN VARCHAR2,
				p_in_segment6 IN VARCHAR2,
                p_in_segment7 IN VARCHAR2,
											p_in_segment8 IN VARCHAR2,
											p_in_segment9 IN VARCHAR2,
											p_in_segment10 IN VARCHAR2,
											p_out_target_system OUT VARCHAR2,
											p_out_status OUT VARCHAR2,
											p_out_message OUT VARCHAR2,
                                            p_in_pkg_name  IN VARCHAR2

    );

END xxcnv_gl_coa_transformation_pkg;