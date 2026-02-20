create or replace PACKAGE BODY XXCNV.XXCNV_PO_C008_PROCUREMENT_CONTRACTS_CONVERSION_PKG IS
    /*************************************************************************************
    NAME              :     XXCNV.XXCNV_PO_C008_PROCUREMENT_CONTRACTS_CONVERSION_PKG  BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
    -- Modification History
    -- Developer          Date         Version     Comments and changes made
    -- -------------   ------       ----------  -----------------------------------------
    --  Bhargavi.K   28-May-2025       1.0         Initial Development
    --  Bhargavi.K   27-Jul-2025       1.1         Removed XXCNV. at line798 ****************************************************************************************/

    -- Declaring global Variables
    gv_import_status                    VARCHAR2(256)    := NULL;
    gv_error_message                    VARCHAR2(500)    := NULL;
	gv_file_name            			VARCHAR2(256)   := NULL;
    gv_oci_file_path                    VARCHAR2(256)    := NULL;
    gv_oci_file_name                    VARCHAR2(4000)   := NULL; 
    gv_oci_file_name_contractheader             VARCHAR2(100)    := NULL;
    gv_execution_id                     VARCHAR2(100)    := NULL;
    gv_batch_id                         NUMBER(38)       := NULL;
    gv_credential_name      CONSTANT    VARCHAR2(100)    := 'OCI$RESOURCE_PRINCIPAL';                
    gv_status_success       CONSTANT    VARCHAR2(100)    := 'Success';
    gv_status_failure       CONSTANT    VARCHAR2(100)    := 'Failure';
    gv_conversion_id                    VARCHAR2(100)    := NULL;
	gv_boundary_system	            	VARCHAR2(100)	:=  NULL;
    gv_status_picked            CONSTANT VARCHAR2(100) := 'File_Picked_From_OCI_And_Loaded_To_Stg';
    gv_status_picked_for_tr     CONSTANT VARCHAR2(100) := 'Transformed_Data_From_Ext_To_Stg';

    gv_status_validated     CONSTANT    VARCHAR2(100)    := 'VALIDATED';
	gv_status_failed   	    CONSTANT 	VARCHAR2(100)	:= 'FAILED_AT_VALIDATION';
	gv_status_failed_validation CONSTANT VARCHAR2(100)   := 'NOT_VALIDATED';
    gv_fbdi_export_status   CONSTANT    VARCHAR2(100)    := 'EXPORTED_TO_FBDI';
    gv_status_staged        CONSTANT    VARCHAR2(100)    := 'STAGED_FOR_IMPORT';    
        gv_transformed_folder       CONSTANT VARCHAR2(100) := 'Transformed_FBDI_Files';
    gv_source_folder            CONSTANT VARCHAR2(100) := 'Source_FBDI_Files';
    gv_properties           CONSTANT    VARCHAR2(100)    := 'properties';
    gv_file_picked                      VARCHAR2(100)    := 'File_Picked_From_OCI_Server';
	gv_recon_folder             CONSTANT VARCHAR2(50) := 'ATP_Validation_Error_Files';
	gv_recon_report         CONSTANT    VARCHAR2(100)    := 'Recon_Report_Created';
	gv_file_not_found       CONSTANT    VARCHAR2(100)    := 'File_not_found';

    /*===========================================================================================================
    -- PROCEDURE : MAIN_PRC
    -- PARAMETERS:
    -- COMMENT   : This procedure is used to call all the procedures under a single procedure
    ==============================================================================================================*/
    PROCEDURE MAIN_PRC ( p_RICE_ID 	            IN  		VARCHAR2,
                     p_execution_id 		IN  	    VARCHAR2,
                     p_boundary_system      IN  		VARCHAR2,
			         p_file_name 		    IN  		VARCHAR2)AS
    p_loading_status VARCHAR2(30) := NULL;
    lv_start_pos NUMBER := 1;
    lv_end_pos NUMBER;
    lv_file_name VARCHAR2(4000);
    BEGIN
        gv_conversion_id := p_rice_id;
        gv_execution_id  := p_execution_id ;
        gv_boundary_system := p_boundary_system; 

        dbms_output.put_line('conversion_id: ' || gv_conversion_id);
		dbms_output.put_line('execution_id: ' || gv_execution_id);
        dbms_output.put_line('boundary_system: ' || gv_boundary_system);

        -- Fetch execution details
			BEGIN
				SELECT   
					ce.execution_id, 
					ce.file_path,
					ce.file_name
				INTO    
					gv_execution_id,
					gv_oci_file_path,
					gv_oci_file_name
				FROM    
					xxcnv_cmn_conversion_execution ce
				WHERE
					ce.conversion_id = gv_conversion_id
					AND ce.STATUS = gv_file_picked
					AND ce.last_update_date = (
						SELECT MAX(ce1.last_update_date) 
						FROM xxcnv_cmn_conversion_execution ce1
						WHERE ce1.conversion_id = gv_conversion_id
						AND ce1.STATUS = gv_file_picked 
					)
					AND ROWNUM = 1;


				-- Debugging output
				dbms_output.put_line('Fetched execution details:');
				dbms_output.put_line('Execution ID: ' || gv_execution_id);
				dbms_output.put_line('File Path: ' || gv_oci_file_path);
				dbms_output.put_line('File Name: ' || gv_oci_file_name);

				-- Initialize loop variables
				lv_start_pos := 1;

				-- Split the concatenated file names and assign to global variables
				LOOP
					lv_end_pos := INSTR(gv_oci_file_name, '.csv', lv_start_pos) + 3;
					EXIT WHEN lv_end_pos = 3; -- Exit loop if no more '.csv' found

					lv_file_name := SUBSTR(gv_oci_file_name, lv_start_pos, lv_end_pos - lv_start_pos + 1);
					dbms_output.put_line('Processing file name: ' || lv_file_name); -- Debugging output

					CASE
						WHEN lv_file_name LIKE '%Contract%.csv' THEN gv_oci_file_name_contractheader := lv_file_name;

						ELSE
							dbms_output.put_line('No match found for file name: ' || lv_file_name); -- Debugging output
					END CASE;

					lv_start_pos := lv_end_pos + 1;
				END LOOP;

				-- Output the results for debugging
				dbms_output.put_line('lv_File Name: ' || lv_file_name);
				dbms_output.put_line('Contract File Name: ' || gv_oci_file_name_contractheader);


			EXCEPTION
				WHEN OTHERS THEN
					dbms_output.put_line('Error fetching execution details: ' || SQLERRM);
					--RETURN;
			END;

			-- Call to import data from OCI to external table
			BEGIN
				IMPORT_DATA_FROM_OCI_TO_STG_PRC(p_loading_status);
				IF p_loading_status = gv_status_failure THEN
					dbms_output.put_line('Error in IMPORT_DATA_FROM_OCI_TO_STG_PRC');
					RETURN;
				END IF;
			EXCEPTION
				WHEN OTHERS THEN
					dbms_output.put_line('Error calling IMPORT_DATA_FROM_OCI_TO_STG_PRC: ' || SQLERRM);
					-- RETURN;
			END;


    -- Call to perform data and business validations in interface table

  BEGIN
        DATA_VALIDATIONS_PRC;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling DATA_VALIDATIONS_PRC: ' || SQLERRM);
            --RETURN;
    END;

    -- Call to create a CSV file  after all validations
    BEGIN
        CREATE_FBDI_FILE_PRC;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling CREATE_FBDI_FILE_PRC: ' || SQLERRM);
            --RETURN;
    END;

    --CREATE RECON REPORT 

	BEGIN
        CREATE_RECON_REPORT_PRC;
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error calling CREATE_RECON_REPORT_PRC: ' || SQLERRM);
            -- RETURN;
    END; 	



END MAIN_PRC;

/*=================================================================================================================
-- PROCEDURE : IMPORT_DATA_FROM_OCI_TO_STG_PRC
-- PARAMETERS: p_loading_status
-- COMMENT   : This procedure is used to create an external table and transfer that data from external to stg table.
===================================================================================================================*/
PROCEDURE IMPORT_DATA_FROM_OCI_TO_STG_PRC (p_loading_status OUT VARCHAR2) IS
    lv_table_count NUMBER := 0;
    lv_row_count   NUMBER := 0;
BEGIN

	BEGIN
	lv_table_count := 0;
	SELECT COUNT(*)
            INTO lv_table_count
            FROM all_objects
            WHERE UPPER(object_name) = 'XXCNV_PO_C008_CONTRACT_EXT'
            AND object_type = 'TABLE';

            IF lv_table_count > 0 THEN
			    EXECUTE IMMEDIATE 'TRUNCATE TABLE XXCNV_PO_C008_Contracts_STG';
                EXECUTE IMMEDIATE 'DROP TABLE XXCNV_PO_C008_Contract_EXT';
                dbms_output.put_line('Table XXCNV_PO_C008_Contract_EXT dropped');
            END IF;
			EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error dropping table XXCNV_PO_C008_Contract_EXT: ' ||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
            p_loading_status := gv_status_failure;
			--RETURN;
	END;

    -- Create the external table
    BEGIN

        IF gv_oci_file_name_contractheader LIKE '%Contract%' THEN

            dbms_output.put_line('Creating external table XXCNV_PO_C008_Contract_EXT');
					dbms_output.put_line(' XXCNV_PO_C008_Contract_EXT : '|| gv_oci_file_path||'/'||gv_oci_file_name_contractheader);


	DBMS_CLOUD.CREATE_EXTERNAL_TABLE(

		 table_name => 'XXCNV_PO_C008_Contract_EXT',
		 credential_name => gv_credential_name,
		 file_uri_list   =>  gv_oci_file_path||'/'||gv_oci_file_name_contractheader,
		 format => json_object('skipheaders' VALUE '1','type' VALUE 'csv','rejectlimit' value 'UNLIMITED','ignoremissingcolumns' value 'true','blankasnull' value 'true','dateformat' VALUE 'mm/dd/yyyy','conversionerrors' VALUE 'store_null'), 
		 column_list => 
				'ORGID                           NUMBER         
				,ORGNAME                         VARCHAR2(120)  
				,CONTRACTTYPEID                  NUMBER         
				,CONTRACTTYPENAME                VARCHAR2(120)  
				,CONTRACTNUMBER                  VARCHAR2(120)  
				,CONTRACTNAME                    VARCHAR2(120)  
				,VENDOR_NUM                      VARCHAR2(50)   
				,PARTYNAME                       VARCHAR2(120)  
				,LEGALENTITYID                   NUMBER         
				,LEGALENTITYNAME                 VARCHAR2(240)  
				,STARTDATE                       VARCHAR2(20)   
				,ENDDATE                         VARCHAR2(20)   
				,CURRENCYCODE                    VARCHAR2(15)   
				,INTENT                          VARCHAR2(50)   
				,AMOUNT                          NUMBER         
				,PAYMENTTERM                     VARCHAR2(50)   
				,DOCURL                          VARCHAR2(120)  
				,PRIMARYCATEGORY                 VARCHAR2(1000) 
				,SECONDARYCATEGORY               VARCHAR2(1000) 
				,TERTIARYCATEGORY                VARCHAR2(1000) 
				,ANNUALIZEDCONTRACTVALUE         NUMBER
				,PurchaseDescription             VARCHAR2(1000)'
				);

	dbms_output.put_line(' External table XXCNV_PO_C008_Contract_EXT is created');

			EXECUTE IMMEDIATE  'INSERT INTO XXCNV_PO_C008_Contracts_Stg (
					            ORGID                  ,
								ORGNAME                ,
								CONTRACTTYPEID         ,
								CONTRACTTYPENAME       ,
								CONTRACTNUMBER         ,
								CONTRACTNAME           ,
								VENDOR_NUM             ,
								PARTYNAME              ,
								LEGALENTITYID          ,
								LEGALENTITYNAME        ,
								STARTDATE              ,
								ENDDATE                ,
								CURRENCYCODE           ,
								INTENT                 ,
								AMOUNT                 ,
								PAYMENTTERM            ,
								DOCURL                 ,
								PrimaryCategory         ,
								SecondaryCategory       ,
								TertiaryCategory        ,
								AnnualizedContractValue ,
								PurchaseDescription    ,
								file_name              ,
								import_status          ,
								error_message          ,
								file_reference_identifier,
								execution_id            ,
								source_system
							) SELECT
								    ORGID                   ,
									ORGNAME              ,
									CONTRACTTYPEID       ,
									CONTRACTTYPENAME       ,
									CONTRACTNUMBER         ,
									CONTRACTNAME           ,
									VENDOR_NUM             ,
									PARTYNAME              ,
									LEGALENTITYID          ,
									LEGALENTITYNAME        ,
									STARTDATE              ,
									ENDDATE                ,
									CURRENCYCODE           ,
									INTENT                 ,
									AMOUNT                 ,
									PAYMENTTERM            ,
									DOCURL                 ,
									PrimaryCategory         ,
									SecondaryCategory       ,
									TertiaryCategory        ,
									AnnualizedContractValue ,
								    PurchaseDescription     ,
								 null,
                                 null,
                                 null,
								 null,
					             '||CHR(39)||gv_execution_id||CHR(39)||',
								 null 
								 FROM XXCNV_PO_C008_Contract_EXT';

				p_loading_status := gv_status_success;	

				dbms_output.put_line('Inserted records in XXCNV_PO_C008_Contracts_Stg: '||SQL%ROWCOUNT);
				--commit;
        END IF;


    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error creating external table: ' || SQLERRM);
            p_loading_status := gv_status_failure;
            RETURN;
    END;

    -- Count the number of rows in the external table
    BEGIN
        IF gv_oci_file_name = '%Contract%' THEN
            SELECT COUNT(*)
            INTO lv_row_count
            FROM XXCNV_PO_C008_Contracts_Stg;
            dbms_output.put_line('Inserted Records in the XXCNV_PO_C008_Contracts_Stg from OCI Source Folder: ' || lv_row_count);
		END IF;



    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error counting rows in the external table: ' || SQLERRM);
            p_loading_status := gv_status_failure;
            RETURN;
    END;

    -- Select FEEDER_IMPORT_BATCH_ID from the external table
   BEGIN
        -- Count the number of rows in the external table
        SELECT COUNT(*)
        INTO lv_row_count
        FROM XXCNV_PO_C008_Contracts_Stg;

        dbms_output.put_line('Log:Inserted Records in the XXCNV_PO_C008_Contracts_Stg from OCI Source Folder: ' || lv_row_count);

        -- Use an implicit cursor in the FOR LOOP to iterate over distinct batch_ids

            xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                p_conversion_id    => gv_conversion_id,
                p_execution_id     => gv_execution_id,
                p_execution_step   => gv_status_picked,
                p_boundary_system  => gv_boundary_system,
                p_file_path        => gv_oci_file_path,
                p_file_name        => gv_oci_file_name,
                P_attribute1       => NULL,
                P_attribute2       => lv_row_count,
                p_process_reference => NULL
            );


        p_loading_status := gv_status_success;

    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error counting rows in XXCNV_PO_C008_Contracts_Stg: ' || SQLERRM);
            p_loading_status := gv_status_failure;
            RETURN;

END;

END IMPORT_DATA_FROM_OCI_TO_STG_PRC;
/*=================================================================================================================
-- PROCEDURE : DATA_VALIDATIONS_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for the validating the mandatory columns and business validations as per lean spec
===================================================================================================================*/
PROCEDURE DATA_VALIDATIONS_PRC IS

  -- Declaring Local Variables for validation.     
  lv_row_count     NUMBER;
  lv_error_count   NUMBER;

BEGIN 
-- Table 1 contract headers stage validations--
	BEGIN

		  BEGIN 
          UPDATE XXCNV_PO_C008_Contracts_Stg
          SET execution_id = gv_execution_id
		  WHERE file_reference_identifier is null;
          END;

		SELECT COUNT(*) INTO lv_row_count 
		FROM XXCNV_PO_C008_Contracts_Stg
		WHERE EXECUTION_ID = gv_execution_id ;

		IF lv_row_count <> 0 then 

		 -- Initialize ERROR_MESSAGE to an empty string if it is NULL
          BEGIN
              UPDATE XXCNV_PO_C008_Contracts_Stg
              SET ERROR_MESSAGE = ''
              WHERE ERROR_MESSAGE IS NULL
			  --and execution_id = gv_execution_id 
			  ;
          END;

-- Orgname Transformation
BEGIN
UPDATE XXCNV_PO_C008_Contracts_Stg
SET ORGNAME = (SELECT oc_business_unit_name FROM xxcnv_gl_le_bu_mapping WHERE ns_legal_entity_name = ORGNAME)
	WHERE ORGNAME is NOT NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('BUSINESS_UNIT is updated');
  END;

   BEGIN
        UPDATE XXCNV_PO_C008_Contracts_Stg
        SET ERROR_MESSAGE = ERROR_MESSAGE || '|Corresponding ORGNAME is not found'
        WHERE ORGNAME IS NULL;
        dbms_output.put_line('ORGNAME is validated');
    END;


 --Validate Legal entity
 BEGIN

      UPDATE XXCNV_PO_C008_Contracts_Stg
      SET LEGALENTITYNAME = (SELECT oc_legal_entity_name FROM xxcnv_gl_le_bu_mapping WHERE ns_legal_entity_name = LEGALENTITYNAME)
      WHERE LEGALENTITYNAME IS NOT NULL
	  AND   file_reference_identifier is NULL;
    DBMS_OUTPUT.PUT_LINE('Legal Entity is updated'); 
END;

 BEGIN
        UPDATE XXCNV_PO_C008_Contracts_Stg
        SET ERROR_MESSAGE = ERROR_MESSAGE || '|Corresponding LEGALENTITYNAME is not found'
        WHERE LEGALENTITYNAME IS NULL;
        dbms_output.put_line('LEGALENTITYNAME is validated');
    END;


--PartyName update
	BEGIN
    UPDATE XXCNV_PO_C008_Contracts_Stg 
    SET PARTYNAME = (SELECT oc_vendor_name FROM xxcnv_ap_c008_contract_supplier_mapping WHERE ns_vendor_num = VENDOR_NUM)
	where VENDOR_NUM is NOT NULL
	and file_reference_identifier is NUll;
    dbms_output.put_line('PartyName is Updated');
  END;

     BEGIN
        UPDATE XXCNV_PO_C008_Contracts_Stg
        SET ERROR_MESSAGE = ERROR_MESSAGE || '|Corresponding Party Name is not found'
        WHERE PARTYNAME IS NULL;
        dbms_output.put_line('PartyName is validated');
    END;

 --  update PAYMENT_TERMS
  BEGIN
    UPDATE XXCNV_PO_C008_Contracts_Stg 
    SET PAYMENTTERM = (SELECT oc_value FROM xxcnv_ap_payment_terms_mapping WHERE ns_value = PAYMENTTERM)
	where PAYMENTTERM IS NOT NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('PAYMENTTERM is updated');
  END;

-- Validate ContractTypeName
	 BEGIN
    UPDATE XXCNV_PO_C008_Contracts_Stg 
    SET CONTRACTTYPENAME = (SELECT oc_CONTRACTTYPENAME FROM xxcnv_PO_CONTRACT_TYPE_NAME_mapping WHERE ns_CONTRACTTYPENAME = CONTRACTTYPENAME)
	where CONTRACTTYPENAME IS NOT NULL
	and file_reference_identifier is NULL;
    dbms_output.put_line('CONTRACTTYPENAME is updated');
  END;

 BEGIN
        UPDATE XXCNV_PO_C008_Contracts_Stg
        SET ERROR_MESSAGE = ERROR_MESSAGE || '|Corresponding CONTRACTTYPENAME is not found'
        WHERE CONTRACTTYPENAME IS NULL;
        dbms_output.put_line('CONTRACTTYPENAME is validated');
    END;  

           -- Update import_status based on error_message
          BEGIN
              UPDATE XXCNV_PO_C008_Contracts_Stg
              SET import_status = CASE WHEN error_message IS NOT NULL THEN 'ERROR' ELSE 'PROCESSED' END;
			  --where execution_id = gv_execution_id ;
              dbms_output.put_line('import_status is validated');
          END;


          -- Final update to set error_message and import_status
          BEGIN 
              UPDATE XXCNV_PO_C008_Contracts_Stg
              SET 
                  error_message = LTRIM(error_message, ','), 
                  import_status = CASE WHEN error_message IS NOT NULL THEN 'ERROR' ELSE 'PROCESSED' END
				  where  execution_id = gv_execution_id
				 AND file_reference_identifier IS NULL; 
              dbms_output.put_line('import_status column is updated');
          END;



		    BEGIN 
            UPDATE XXCNV_PO_C008_Contracts_Stg
            SET SOURCE_SYSTEM = gv_boundary_system
			WHERE file_reference_identifier is null
			and execution_id = gv_execution_id ;
            dbms_output.put_line('source_system is updated');
          END;

		  BEGIN
			UPDATE XXCNV_PO_C008_Contracts_Stg 
			SET FILE_NAME = gv_oci_file_name_contractheader
			where file_reference_identifier is null 
			and execution_id = gv_execution_id;
			dbms_output.put_line('file_name column is updated');
		  END;


	  -- Check if there are any error messages
	  SELECT COUNT(*) INTO lv_error_count 
	  FROM XXCNV_PO_C008_Contracts_Stg 
	  WHERE error_message is not null
	  ;

	   -- Check if there are any error messages



	   UPDATE XXCNV_PO_C008_Contracts_Stg
       SET file_reference_identifier = gv_execution_id||'_'||gv_status_failure
	   where error_message is not null
	   and file_reference_identifier is null
	   and execution_id = gv_execution_id 
	   ;
       dbms_output.put_line('file_reference_identifier column is updated');


		UPDATE XXCNV_PO_C008_Contracts_Stg

		SET file_reference_identifier = gv_execution_id||'_'||gv_status_success
		where error_message is null
		and file_reference_identifier is null
		and execution_id = gv_execution_id 
		;
		dbms_output.put_line('file_reference_identifier column is updated');

	  IF lv_error_count > 0 THEN
			 -- Logging the message If data is not validated
	  xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
					p_conversion_id 		=> gv_conversion_id,
					p_execution_id		    => gv_execution_id,
					p_execution_step 		=> gv_status_failed,
					p_boundary_system 		=> gv_boundary_system,
					p_file_path				=> gv_oci_file_path,
					p_file_name 			=> gv_oci_file_name_contractheader,
					P_attribute1            => gv_batch_id,
					P_attribute2            => NULL,
					p_process_reference 	=> NULL
	  );

	   END IF;



	  IF lv_error_count = 0 AND  gv_oci_file_name_contractheader is NOT NULL THEN

		xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
				p_conversion_id 		=> gv_conversion_id,
				p_execution_id		    => gv_execution_id,
				p_execution_step 		=> gv_status_validated,
				p_boundary_system 		=> gv_boundary_system,
				p_file_path				=> gv_oci_file_path,
				p_file_name 			=> gv_oci_file_name_contractheader,
				P_attribute1            => NULL,
				P_attribute2            => NULL,
				p_process_reference 	=> NULL );

	  END IF;
	  --COMMIT;
	   IF gv_oci_file_name_contractheader is null THEN


		xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
				p_conversion_id 		=> gv_conversion_id,
				p_execution_id		    => gv_execution_id,
				p_execution_step 		=> gv_file_not_found,
				p_boundary_system 		=> gv_boundary_system,
				p_file_path				=> gv_oci_file_path,
				p_file_name 			=> gv_oci_file_name_contractheader,
				P_attribute1            => NULL,
				P_attribute2            => NULL,
				p_process_reference 	=> NULL );


	  END IF;
	   else 
	 dbms_output.put_line('No Data is found in interface tables. Data is not loaded from ext to stg ');

	 end if;
	  END;


	END DATA_VALIDATIONS_PRC;

/*==============================================================================================================================
-- PROCEDURE : CREATE_FBDI_FILE_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating the FBDI CSV file after all validations.
================================================================================================================================= */
PROCEDURE CREATE_FBDI_FILE_PRC IS

    lv_success_count INTEGER;

BEGIN
    BEGIN
        

        dbms_output.put_line('In create FBDI Processing for XXCNV_PO_C008_Contracts_Stg ' );

            BEGIN
                -- Count the number of rows with non-null, non-empty error_message for the current batch_id
                SELECT COUNT(*)
                INTO lv_success_count
                FROM XXCNV_PO_C008_Contracts_Stg
                WHERE file_reference_identifier = gv_execution_id||'_'||gv_status_success;
               

                dbms_output.put_line('Error count for XXCNV_PO_C008_Contracts_Stg: ' || lv_success_count);

            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    dbms_output.put_line('No data found for XXCNV_PO_C008_Contracts_Stg ');
                    RETURN; --
                WHEN OTHERS THEN
                    dbms_output.put_line('Error checking error_message column for XXCNV_PO_C008_Contracts_Stg  batch_id : ' || SQLERRM);
                    RETURN; --
            END;

            IF lv_success_count > 0 THEN
                BEGIN
                    DBMS_CLOUD.EXPORT_DATA (
                        CREDENTIAL_NAME => gv_credential_name,
						FILE_URI_LIST   => REPLACE(gv_oci_file_path, gv_source_folder, gv_transformed_folder) || '/' || 'ACTIVE' || gv_oci_file_name_contractheader,
                        FORMAT          => JSON_OBJECT('type' VALUE 'csv', 'trimspaces' VALUE 'rtrim','header' value true),
                        QUERY           => 'SELECT 
                                              ORGID                   ,
ORGNAME              ,
CONTRACTTYPEID       ,
CONTRACTTYPENAME       ,
CONTRACTNUMBER         ,
CONTRACTNAME           ,
PARTYID                ,
PARTYNAME              ,
LEGALENTITYID          ,
LEGALENTITYNAME        ,
STARTDATE              ,
ENDDATE                ,
CURRENCYCODE           ,
INTENT                 ,
AMOUNT                 ,
PAYMENTTERM            ,
DOCURL                 ,
PrimaryCategory         ,
SecondaryCategory         ,
TertiaryCategory        ,
AnnualizedContractValue ,
PurchaseDescription     ,
ATTRIBUTE1             ,
ATTRIBUTE2             ,
ATTRIBUTE3             ,
ATTRIBUTE4             ,
ATTRIBUTE5             
                                            FROM XXCNV_PO_C008_Contracts_Stg
										  WHERE import_status = '''||'PROCESSED'||'''
											AND file_reference_identifier= '''|| gv_execution_id|| '_' || gv_status_success||''''
                    );

                    dbms_output.put_line('CSV file for XXCNV_PO_C008_Contracts_Stg exported successfully to OCI Object Storage.');

                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id       => gv_conversion_id,
                        p_execution_id        => gv_execution_id,
                        p_execution_step      => gv_fbdi_export_status,
                        p_boundary_system     => gv_boundary_system,
                        p_file_path           => REPLACE(gv_oci_file_path, gv_source_folder, gv_transformed_folder),
                        p_file_name           => gv_oci_file_name_contractheader,
                        P_attribute1          => NULL,
                        P_attribute2          => NULL,
                        p_process_reference   => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV for  XXCNV_PO_C008_Contracts_Stg BATCH_ID : ' || SQLERRM);
				
                        RETURN;
                END;
           ELSE
                dbms_output.put_line('Process Stopped for XXCNV_PO_C008_Contracts_Stg: Error message columns contain data.');
			RETURN;
            END IF;


         EXCEPTION
          WHEN OTHERS THEN
		   dbms_output.put_line('An error occurred: ' ||  '->'|| SUBSTR (SQLERRM, 1, 3000)|| '->'|| DBMS_UTILITY.format_error_backtrace);
				RETURN;
		 END;




END CREATE_FBDI_FILE_PRC;


/*==============================================================================================================================
-- PROCEDURE : CREATE_RECON_REPORT_PRC
-- PARAMETERS: 
-- COMMENT   : This procedure is used for creating properties file.
================================================================================================================================= */

PROCEDURE CREATE_RECON_REPORT_PRC IS


BEGIN
		BEGIN
		 dbms_output.put_line('Processing RECON REPORT' || gv_oci_file_path || '_' || gv_source_folder || '_' || gv_recon_folder );

                    BEGIN

                    DBMS_CLOUD.EXPORT_DATA (
                        CREDENTIAL_NAME => gv_credential_name,
						file_uri_list   => REPLACE(gv_oci_file_path, gv_source_folder, gv_recon_folder)||'/'||'ATP_Recon_Contract'|| sysdate,
                        FORMAT          => JSON_OBJECT('type' VALUE 'csv', 'trimspaces' VALUE 'rtrim','header' value true),
                        QUERY           => 'SELECT 
                                                ORGID                   ,
ORGNAME              ,
CONTRACTTYPEID       ,
CONTRACTTYPENAME       ,
CONTRACTNUMBER         ,
CONTRACTNAME           ,
PARTYID                ,
PARTYNAME              ,
LEGALENTITYID          ,
LEGALENTITYNAME        ,
STARTDATE              ,
ENDDATE                ,
CURRENCYCODE           ,
INTENT                 ,
AMOUNT                 ,
PAYMENTTERM            ,
DOCURL                 ,
PrimaryCategory         ,
SecondaryCategory         ,
TertiaryCategory        ,
AnnualizedContractValue ,
PurchaseDescription     ,
ATTRIBUTE1             ,
ATTRIBUTE2             ,
ATTRIBUTE3             ,
ATTRIBUTE4             ,
ATTRIBUTE5             ,
                                                file_name,
                                                error_message,
                                                import_status,
                                                source_system
                                            FROM XXCNV_PO_C008_Contracts_Stg 
											 where import_status = '''||'ERROR'||'''
                                            and execution_id  =  '''||gv_execution_id||''''
                    );

                    dbms_output.put_line('CSV file for XXCNV_PO_C008_Contracts_Stg exported successfully to OCI Object Storage.');

                    xxcnv_cmn_conversion_log_message_pkg.write_log_prc(
                        p_conversion_id       => gv_conversion_id,
                        p_execution_id        => gv_execution_id,
                        p_execution_step      => gv_recon_report,
                        p_boundary_system     => gv_boundary_system,
                        p_file_path           => REPLACE(gv_oci_file_path, gv_source_folder, gv_recon_folder),
                        p_file_name           => gv_oci_file_name_contractheader,
                        P_attribute1          => NULL,
                        P_attribute2          => NULL,
                        p_process_reference   => NULL
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Error exporting data to CSV : ' || SQLERRM);
                        -- RETURN;
                END;
    END;

END CREATE_RECON_REPORT_PRC;

END XXCNV_PO_C008_PROCUREMENT_CONTRACTS_CONVERSION_PKG;
