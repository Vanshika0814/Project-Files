
	/*************************************************************************************
    NAME              :     xxcnv_supplier_branch_mapping
    PURPOSE           :     This table is used for loading bank branch mapping data
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -----------------------------------------
	-- Bhargavi.K	  24-Oct-2025  	    1.0         Initial Development    
	-- Satya Pavani   02-Aug-2025       1.1         LTCI-6584
	****************************************************************************************/


drop table xxcnv.xxcnv_supplier_branch_mapping;

create table xxcnv.xxcnv_supplier_branch_mapping
(
COUNTRY              VARCHAR2(100) ,
BANK_NAME            VARCHAR2(360)  ,
BANK_CITY            VARCHAR2(100)  ,
BIC_CODE             VARCHAR2(100)  ,
ROUTING_NUMBER       VARCHAR2(100)  ,
BRANCH_NAME          VARCHAR2(360)  
);

GRANT select,insert,update,delete ON xxcnv.xxcnv_supplier_branch_mapping to xxcnv_run;