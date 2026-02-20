--------------------------------------------------------
--  DDL for Package XXINT_GL_I008_CONCUR_ACC_JOURNALS_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "XXINT"."XXINT_GL_I008_CONCUR_ACC_JOURNALS_PKG" AS
/********************************************************************************************
OBJECT NAME: GL Concur Accruals Journals Package
DESCRIPTION: Package specification for GL_I008
Version 	Name              	Date           		Version-Description
---------------------------------------------------------------------------
<1.0>		Devishi   			23-May-2025 	    1.0-Initial Draft
**********************************************************************************************/

/*****************************************************************
OBJECT NAME: Validation Transformation Procedure
DESCRIPTION: Procedure for Validation Transformation
Version 	Name              	Date           		Version-Description
----------------------------------------------------------------------------
<1.0>		Devishi   			23-May-2025 	    1.0- Initial Draft
******************************************************************/

    PROCEDURE validation_transformation_prc (
        p_current_instance_id IN VARCHAR2,
        p_source_file_name    IN VARCHAR2,
        x_status              OUT VARCHAR2,
        x_status_message      OUT VARCHAR2
    );

END xxint_gl_i008_concur_acc_journals_pkg;

/
