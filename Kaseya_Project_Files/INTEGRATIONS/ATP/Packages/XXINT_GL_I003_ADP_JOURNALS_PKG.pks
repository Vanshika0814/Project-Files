--------------------------------------------------------
--  File created - Thursday-July-24-2025   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package XXINT_GL_I003_ADP_JOURNALS_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "XXINT"."XXINT_GL_I003_ADP_JOURNALS_PKG" IS
/********************************************************************************************
OBJECT NAME: GL Journals ADP Package
DESCRIPTION: Package specification for GL_I003
Version 	Name              	Date           		Version-Description
---------------------------------------------------------------------------
<1.0>		Priyanka Gupta A   			15-MAY-2025 	    1.0-Initial Draft
**********************************************************************************************/


/*****************************************************************
	OBJECT NAME: Journals Data Validation Procedure
	DESCRIPTION: Procedure to validate the ADP journals source data XXINT_GL_I003_ADP_JOURNALS_STG Records
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Priyanka Gupta A     			15-MAY-2025  	    1.0- Initial Draft
******************************************************************/
    
	PROCEDURE data_validations_prc(
        p_parent_instance_id   IN VARCHAR2,
	    p_current_instance_id IN VARCHAR2,
        p_interface_rice_id   IN VARCHAR2,
        p_integration_name    IN VARCHAR2,
        p_log_flag            IN VARCHAR2,
        x_status              OUT VARCHAR2,
        x_status_message      OUT VARCHAR2
    );
	
/*****************************************************************
	OBJECT NAME: DB Pagination Procedure
	DESCRIPTION: Procedure to paginate XXINT_GL_I003_ADP_JOURNALS_STG Records 
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Priyanka Gupta A     			15-MAY-2025  	    1.0- Initial Draft
******************************************************************/	
	PROCEDURE create_chunk_prc (
        p_current_instance_id  IN   VARCHAR2,
        p_batch_limit     IN   VARCHAR2,
        p_status       OUT  VARCHAR2,
        p_message      OUT  VARCHAR2);


END XXINT_GL_I003_ADP_JOURNALS_PKG;

/

  GRANT EXECUTE ON "XXINT"."XXINT_GL_I003_ADP_JOURNALS_PKG" TO "XXINT_RO";
