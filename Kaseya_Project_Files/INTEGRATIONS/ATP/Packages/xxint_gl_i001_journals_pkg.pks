--------------------------------------------------------
--  DDL for Package XXINT_GL_I001_JOURNALS_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "XXINT"."XXINT_GL_I001_JOURNALS_PKG" IS
/********************************************************************************************
OBJECT NAME: GL Journals Package
DESCRIPTION: Package specification for GL_I001
Version 	Name              	Date           		Version-Description
---------------------------------------------------------------------------
<1.0>		Devishi   			02-MAR-2025 	    1.0-Initial Draft
<1.1>       Devishi   			30-JULY-2025 	    1.1-Removed 'batch_details_prc' procedure
**********************************************************************************************/

	/*****************************************************************
	OBJECT NAME: Get ledger Name Procedure
	DESCRIPTION: Procedure to fetch Ledger Name 
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Devishi   			02-MAR-2025 	    1.0- Initial Draft
	******************************************************************/
    PROCEDURE validation_transformation_prc (
        p_current_instance_id IN VARCHAR2,
        p_batch_limit         IN VARCHAR2,
        x_status              OUT VARCHAR2,
        x_status_message      OUT VARCHAR2
    );


    /*****************************************************************
	OBJECT NAME: Split COA Values Procedure
	DESCRIPTION: Procedure to split COA Values 
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Devishi   			16-APR-2025 	    1.0- Initial Draft
******************************************************************/
    PROCEDURE coa_split_prc (
        p_current_instance_id IN VARCHAR2,
        p_coa_batch_limit     IN VARCHAR2,
        x_status              OUT VARCHAR2,
        x_status_message      OUT VARCHAR2
    );

     /*****************************************************************
	OBJECT NAME: Update COA Values Procedure
	DESCRIPTION: Procedure to update COA Values 
	Version 	Name              	Date           		Version-Description
	----------------------------------------------------------------------------
	<1.0>		Devishi   			16-APR-2025 	    1.0- Initial Draft
******************************************************************/
    PROCEDURE coa_values_prc (
        p_current_instance_id IN VARCHAR2,
        p_batch_limit         IN VARCHAR2,
        x_status              OUT VARCHAR2,
        x_status_message      OUT VARCHAR2
    );

END xxint_gl_i001_journals_pkg;
/
