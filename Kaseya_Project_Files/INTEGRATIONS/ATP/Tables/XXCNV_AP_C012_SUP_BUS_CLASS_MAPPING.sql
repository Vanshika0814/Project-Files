/*******************************************************************************************
 NAME              :     XXCNV_AP_C012_SUP_BUS_CLASS_MAPPING TABLE
 -- Modification History
	-- Developer         Date         Version     Comments and changes made
	-- -------------   ------         ----------  -----------------------------------------
	    Phanindra      28-Aug-2024        1.1       Made Changes as per the Jira LTCI-8094
*********************************************************************************************/
DROP TABLE xxcnv.XXCNV_AP_C012_SUP_BUS_CLASS_MAPPING;

CREATE TABLE xxcnv.XXCNV_AP_C012_SUP_BUS_CLASS_MAPPING
( NS_VENDOR_NUM                    VARCHAR2(30),  --Added column for the change jira LTCI-8094
OC_VENDOR_NUM                    VARCHAR2(30),    --Added column for the change jira LTCI-8094
OC_VENDOR_NAME                   VARCHAR2(200), 
CLASSIFICATION_LOOKUP_CODE       VARCHAR2(30) 
);