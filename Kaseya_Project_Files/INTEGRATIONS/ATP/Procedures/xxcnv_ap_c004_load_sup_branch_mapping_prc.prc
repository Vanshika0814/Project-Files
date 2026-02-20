/*************************************************************************************
    NAME              :     xxcnv_ap_c004_load_sup_branch_mapping_prc
    PURPOSE           :     This procedure is for loading bank branch data into mapping table
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -----------------------------------------
	-- Bhargavi.K	  24-Oct-2025  	    1.0         Initial Development    
	-- Satya Pavani   02-Aug-2025       1.1         LTCI-6584
	****************************************************************************************/
	
create or replace PROCEDURE xxcnv.xxcnv_ap_c004_load_sup_branch_mapping_prc IS

    lv_table_count NUMBER := 0;
    lv_row_count   NUMBER := 0;
	gv_credential_name        CONSTANT 	  VARCHAR2(30)	:= 'OCI$RESOURCE_PRINCIPAL';                
	gv_status_success         CONSTANT    VARCHAR2(100) := 'Success';
	gv_status_failure         CONSTANT    VARCHAR2(100) := 'Failure';
	 /*
	--gv_oci_file_path          CONSTANT    VARCHAR2(200) := 'https://id8thgcxl2q7.objectstorage.us-ashburn-1.oci.customer-oci.com/n/id8thgcxl2q7/b/ksy-fusion-cnv-bucket-dev/o/SIT2/SupplierBanks';
    --gv_oci_file_name          CONSTANT    VARCHAR2(200) := 'Sup_bank_branch_mapping_UK_3.csv*';
	commented for v1.1 */
	gv_oci_file_path          VARCHAR2(400) := NULL; -- added for v1.1
	gv_oci_file_name          CONSTANT    VARCHAR2(200) := 'Sup_bank_branch_mapping*.csv'; -- added for v1.1

BEGIN	

    BEGIN
		
		/* start added for v1.1 */
		SELECT  obj_storage_path
		INTO    gv_oci_file_path
		FROM    xxcnv_cmn_conversion_metadata
		WHERE   rownum = 1;
		/* end added for v1.1 */
		dbms_output.put_line('Filepath: '||gv_oci_file_path);
        
	    lv_table_count := 0;	  
		
	    SELECT COUNT(*)
        INTO lv_table_count
        FROM all_objects
        WHERE UPPER(object_name) = 'XXCNV_SUPPLIER_BRANCH_MAPPING_EXT'
        AND object_type = 'TABLE';

        IF lv_table_count > 0 THEN
            EXECUTE IMMEDIATE 'DROP TABLE xxcnv_supplier_branch_mapping_ext';
			--EXECUTE IMMEDIATE 'TRUNCATE TABLE xxcnv_supplier_branch_mapping';
            dbms_output.put_line('Table xxcnv_supplier_branch_mapping_ext dropped');
        END IF;
		EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error dropping table xxcnv_supplier_branch_mapping_ext: ' ||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
    END;	

	BEGIN
			dbms_output.put_line('Creating external table xxcnv_supplier_branch_mapping_ext');
			DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
		   table_name => 'xxcnv_supplier_branch_mapping_ext',
           credential_name => gv_credential_name,
		   file_uri_list   =>  gv_oci_file_path||gv_oci_file_name,
			/* commented as per v1.1
           format => json_object('skipheaders' VALUE '1','type' value 'csv','rejectlimit' value 'UNLIMITED','ignoremissingcolumns' value 'true','blankasnull' value 'true','conversionerrors' VALUE 'store_null' ),
           column_list => 
                       ' BANK_NAME            VARCHAR2(80)  
						,BRANCH_NAME          VARCHAR2(80)
						,BANK_CITY            VARCHAR2(80)  
						,BIC_CODE             VARCHAR2(80)  
						,ROUTING_NUMBER       VARCHAR2(80) 
						,COUNTRY              VARCHAR2(100)'
						);
			*/
			/* start added for v1.1 */
           format => json_object('skipheaders' VALUE '1','type' value 'csv','rejectlimit' value 'UNLIMITED','ignoremissingcolumns' value 'true','blankasnull' value 'true','conversionerrors' VALUE 'store_null' ),
           column_list => 
                       ' BANK_NAME            VARCHAR2(360)  
						,BRANCH_NAME          VARCHAR2(360)
						,BANK_CITY            VARCHAR2(100)  
						,BIC_CODE             VARCHAR2(100)  
						,ROUTING_NUMBER       VARCHAR2(100) 
						,COUNTRY              VARCHAR2(100)'
						);
			/* end added for v1.1 */
			dbms_output.put_line('External table is created');
			EXECUTE IMMEDIATE  'INSERT INTO xxcnv_supplier_branch_mapping 
									(	
			                         BANK_NAME            
									,BRANCH_NAME          
									,BANK_CITY            
									,BIC_CODE             
									,ROUTING_NUMBER       
									,COUNTRY   
									) 
									SELECT 
									 BANK_NAME            
									,BRANCH_NAME          
									,BANK_CITY            
									,BIC_CODE             
									,ROUTING_NUMBER       
									,COUNTRY 
									FROM xxcnv_supplier_branch_mapping_ext';

			dbms_output.put_line('Inserted Records in the xxcnv_supplier_branch_mapping');					


	EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error creating external table: ' || SQLERRM);
            RETURN;
    END;

END xxcnv_ap_c004_load_sup_branch_mapping_prc;