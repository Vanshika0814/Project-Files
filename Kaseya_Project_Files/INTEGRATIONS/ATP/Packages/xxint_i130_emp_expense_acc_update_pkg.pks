CREATE OR REPLACE PACKAGE XXINT.xxint_i130_emp_expense_acc_update_pkg IS
/*************************************************************************************************************************************************
OBJECT NAME: XXINT_I130_EMP_EXPENSE_ACC_UPDATE_PKG
DESCRIPTION: Package specification for employee default expense account update on basis of location, department, division and legal entity change
Version 	Name              		Date           		Version-Description
---------------------------------------------------------------------------
<1.0>		Vaishnavi Kattula     	07/22/2025	    	1.0-Initial Draft
***************************************************************************************************************************************************/
    TYPE xxint_i130_employee_default_expense_account_stg_rec IS RECORD (
            "ASSIGNMENT_ID"                NUMBER,
            "PERSON_ID"                    NUMBER,
            "PERSON_NUMBER"                VARCHAR2(30),
            "LAST_NAME"                    VARCHAR2(200),
            "FIRST_NAME"                   VARCHAR2(200),
            "ASSIGNMENT_NUMBER"            VARCHAR2(50),
            "ASSIGNMENT_NAME"              VARCHAR2(80),
            "ASSIGNEMENT_STATUS_TYPE"      VARCHAR2(30),
            "WORK_TERMS_ASSIGNMENT_ID"     NUMBER,
            "PERIOD_OF_SERVICE_ID"         NUMBER,
            "ACTION_CODE"                  VARCHAR2(240),
            "LEGAL_EMPLOYER_NAME"          VARCHAR2(240),
            "EFFECTIVE_START_DATE"         DATE,
            "PRIMARY_FLAG"                 VARCHAR2(10),
            "PRIMARY_ASSIGNMENT_FLAG"      VARCHAR2(10),
            "WORKER_TYPE"                  VARCHAR2(240),
            "WORKER_NUMBER"                VARCHAR2(240),
            "MANAGER_NAME"                 VARCHAR2(240),
            "BUSINESS_UNIT_NAME"           VARCHAR2(240),
            "DEPARTMENT_NAME"              VARCHAR2(240),
            "JOB_CODE"                     VARCHAR2(240),
            "JOB_NAME"                     VARCHAR2(240),
            "LOCATION_CODE"                VARCHAR2(240),
            "LOCATION_NAME"                VARCHAR2(240),
            "GRADE_CODE"                   VARCHAR2(240),
            "GRADE_NAME"                   VARCHAR2(240),
            "Location_Address_Line1"       VARCHAR2(240),
            "Location_Address_Line2"       VARCHAR2(240),
            "Location_Address_Line3"       VARCHAR2(240),
            "Location_Address_Line4"       VARCHAR2(240),
            "Location_REGION1"             VARCHAR2(240),
            "Location_REGION2"             VARCHAR2(240),
            "Location_REGION3"             VARCHAR2(240),
            "Location_REGION4"             VARCHAR2(240),
            "Location_Town_Or_City"        VARCHAR2(240),
            "Location_Postal_Code"         VARCHAR2(240),
            "Location_Country"             VARCHAR2(240),
            "Location_Long_Postal_Code"    VARCHAR2(240),
            "Length_Of_Service_Years"      NUMBER,
            "Length_Of_Service_Months"     NUMBER,
            "Length_Of_Service_Days"       NUMBER,
            "DIVISION"                     VARCHAR2(240),
            "EFFECTIVE_SEQUENCE"           VARCHAR2(20),
            "DEFAULT_EXPENSE_ACCOUNT"      VARCHAR2(250),
            "NEW_DEAFAULT_EXPENSE_ACCOUNT" VARCHAR2(250),
            "COMPANY"                      VARCHAR2(250),
            "LOCATION_SEGMENT"             VARCHAR2(250),
            "DIVISION_SEGMENT"             VARCHAR2(250),
            "COST_CENTER"                  VARCHAR2(250),
            "CREATED_BY"                   VARCHAR2(240),
            "LAST_UPDATED_BY"              VARCHAR2(240),
            "CREATED_DATE"                 DATE,
            "LAST_UPDATE_DATE"             DATE
    );
    TYPE xxint_hcm_i130_employee_default_expense_account_stg_typ IS
        TABLE OF xxint_i130_employee_default_expense_account_stg_rec;
    PROCEDURE validate_emp_expense_account (
        p_current_instance_id  IN VARCHAR2,
        p_batch_limit          IN VARCHAR2,
        p_db_log               IN VARCHAR2,
        p_emp_records          IN xxint_hcm_i130_employee_default_expense_account_stg_typ,
        x_new_expense_record   OUT xxint_hcm_i130_employee_default_expense_account_stg_typ,
        x_return_records_count OUT NUMBER,
        x_status_code          OUT VARCHAR2,
        x_status_message       OUT VARCHAR2
    );

END xxint_i130_emp_expense_acc_update_pkg;