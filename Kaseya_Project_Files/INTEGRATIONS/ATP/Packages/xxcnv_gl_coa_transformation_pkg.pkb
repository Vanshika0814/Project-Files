create or replace PACKAGE BODY       xxcnv.xxcnv_gl_coa_transformation_pkg IS

	/*************************************************************************************
    NAME              :     xxcnv_gl_coa_transformation_pkg BODY
    PURPOSE           :     This package is the detailed body of all the procedures.
	-- Modification History
	-- Developer          Date         Version     Comments and changes made
	-- -------------   ------       ----------  -----------------------------------------
	-- Priyanka Kadam  27-Feb-2024     1.0         Initial Development
        -- Priyanka Kadam  29-Jul-2025     1.1         Added changes for JIRA ID-6261
	****************************************************************************************/


    PROCEDURE coa_segment_mapping_prc (
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
											p_out_status  OUT VARCHAR2,
											p_out_message OUT VARCHAR2,
                                            p_in_pkg_name  IN VARCHAR2
										) IS
        lv_segment1 	VARCHAR2(50);
        lv_segment2 	VARCHAR2(50);
        lv_segment3 	VARCHAR2(50);
        lv_segment4 	VARCHAR2(50);
        lv_segment5 	VARCHAR2(50);
        lv_segment6 	VARCHAR2(50);
        lv_segment7 	VARCHAR2(50);
        lv_segment8 	VARCHAR2(50):= '9999';
        lv_segment9 	VARCHAR2(50):= '9999';
        lv_segment10 	VARCHAR2(50):= '999999';
		lv_segment   	VARCHAR2(300);
		lv_status 		VARCHAR2(100):= NULL;
		lv_message      VARCHAR2(2000):= NULL;
        lv_rec_cnt      NUMBER:=0;

BEGIN

    -- Segment1 - Company --
        BEGIN
            SELECT erp_coa_value
            INTO lv_segment1
            FROM xxmap.xxmap_gl_e001_kaseya_ns_company
            WHERE ns_company_attribute_1 = p_in_segment1
            AND   last_update_date = (select max(last_update_date)
                                      FROM xxmap.xxmap_gl_e001_kaseya_ns_company
            				     WHERE ns_company_attribute_1 = p_in_segment1
						    );

        EXCEPTION
			WHEN NO_DATA_FOUND THEN
				lv_status  := 'ERROR'; 
				lv_message := '|No data found for coa segment1';
				dbms_output.put_line('No data found for coa segment1');
            WHEN OTHERS THEN
				lv_status := 'ERROR'; 
				lv_message := '|Error while fetching coa segment1';
                dbms_output.put_line('Error while fetching coa segment1: ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);
        END;

    -- Segment2 - Division --
		IF p_in_segment2 IS NULL THEN 
		lv_segment2:= '999';
		ELSIF p_in_segment2 = '999' THEN
		lv_segment2:= '999';
		ELSE
        BEGIN
            SELECT erp_coa_value
            INTO lv_segment2
            FROM xxmap.xxmap_gl_e001_kaseya_ns_divison
            WHERE ns_divison_attribute_1 = p_in_segment2
			AND   last_update_date =  ( SELECT max(last_update_date)
										FROM xxmap.xxmap_gl_e001_kaseya_ns_divison
										WHERE ns_divison_attribute_1 = p_in_segment2
										);
        EXCEPTION
			WHEN NO_DATA_FOUND THEN
				lv_status := 'ERROR'; 
				lv_message := lv_message || '|No data found for coa segment2';
				dbms_output.put_line('No data found for coa segment2');
            WHEN OTHERS THEN
				lv_status := 'ERROR'; 
				lv_message := lv_message || '|Error while fetching coa segment2';
                dbms_output.put_line('Error while fetching coa segment2: ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);
        END;
		END IF;

    -- Segment3 - Cost Center --

        BEGIN
            SELECT distinct erp_coa_value
            INTO lv_segment3
            FROM xxmap.xxmap_gl_e001_kaseya_ns_acctcc 
            WHERE ns_costcenter_attribute_1 = NVL(p_in_segment3,'99999')
			AND   ns_account_attribute_1    = p_in_segment4
			AND   last_update_date =  ( SELECT max(last_update_date)
										FROM xxmap.xxmap_gl_e001_kaseya_ns_acctcc 
										WHERE ns_costcenter_attribute_1 = NVL(p_in_segment3,'99999')
										AND   ns_account_attribute_1    = p_in_segment4
										);
        EXCEPTION
			WHEN NO_DATA_FOUND THEN
				        BEGIN
							IF p_in_segment3 = '99999' THEN 
							lv_segment3:= '99999';
							ELSE
							SELECT distinct erp_coa_value
							INTO lv_segment3
							FROM xxmap.xxmap_gl_e001_kaseya_ns_costcenter
							WHERE ns_costcenter_attribute_1 = NVL(p_in_segment3,'99999')
							AND   last_update_date =  ( SELECT max(last_update_date)
														FROM xxmap.xxmap_gl_e001_kaseya_ns_costcenter
														WHERE ns_costcenter_attribute_1 = NVL(p_in_segment3,'99999')
														)
							AND lv_segment3 IS NULL;
							END IF;
						EXCEPTION
							WHEN NO_DATA_FOUND THEN
								lv_status := 'ERROR'; 
								lv_message := lv_message || '|No data found for coa segment3 in case 2';
								dbms_output.put_line('No data found for coa segment3 in case 2');
							WHEN OTHERS THEN
								lv_status := 'ERROR'; 
								lv_message := lv_message || '|Error while fetching coa segment3 in case 2';
								dbms_output.put_line('Error while fetching coa segment3 in case 2: ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);
						END;
            WHEN OTHERS THEN
				lv_status := 'ERROR'; 
				lv_message := lv_message || '|Error while fetching coa segment3 in case 1';
                dbms_output.put_line('Error while fetching coa segment3 in case 1: ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);

        END;

    -- Segment4 - Natural Account --
        BEGIN
            SELECT erp_coa_value
            INTO lv_segment4
            FROM xxmap.xxmap_gl_e001_kaseya_ns_account
            WHERE ns_account_attribute_1 = p_in_segment4
			AND   last_update_date =  ( SELECT max(last_update_date)
										FROM xxmap.xxmap_gl_e001_kaseya_ns_account
										WHERE ns_account_attribute_1 = p_in_segment4
										);
        EXCEPTION
			WHEN NO_DATA_FOUND THEN
				lv_status := 'ERROR'; 
				lv_message := lv_message || '|No data found for coa segment4';
				dbms_output.put_line('No data found for coa segment4');
            WHEN OTHERS THEN
				lv_status := 'ERROR'; 
				lv_message := lv_message || '|Error while fetching coa segment4';
                dbms_output.put_line('Error while fetching coa segment4: ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);
        END;

    -- Segment5 - Product Line --
		IF p_in_segment5 IS NULL THEN 
		lv_segment5:= '9999';
		ELSIF p_in_segment5 = '9999' THEN
		lv_segment5:= '9999';
		ELSE
        BEGIN
            SELECT erp_coa_value
            INTO lv_segment5
            FROM xxmap.xxmap_gl_e001_kaseya_ns_productline 
            WHERE ns_productline_attribute_1 = p_in_segment5
			AND   last_update_date =  ( SELECT max(last_update_date)
										FROM xxmap.xxmap_gl_e001_kaseya_ns_productline
										WHERE ns_productline_attribute_1 = p_in_segment5
										);
        EXCEPTION
			WHEN NO_DATA_FOUND THEN
				lv_status := 'ERROR'; 
				lv_message := lv_message || '|No data found for coa segment5';
				dbms_output.put_line('No data found for coa segment5');
            WHEN OTHERS THEN
				lv_status := 'ERROR'; 
				lv_message := lv_message || '|Error while fetching coa segment5';
                dbms_output.put_line('Error while fetching coa segment5: ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);
        END;
		END IF;

    -- Segment6 - Location --
		IF p_in_segment6 IS NULL THEN 
		lv_segment6:= '999999';
		ELSIF p_in_segment6 = '999999' THEN
		lv_segment6:= '999999';
		ELSE
        BEGIN
            SELECT erp_coa_value
            INTO lv_segment6
            FROM xxmap.xxmap_gl_e001_kaseya_ns_location
            WHERE ns_location_attribute_1 = p_in_segment6
			AND   last_update_date =  ( SELECT max(last_update_date)
										FROM xxmap.xxmap_gl_e001_kaseya_ns_location
										WHERE ns_location_attribute_1 = p_in_segment6
										);
        EXCEPTION
			WHEN NO_DATA_FOUND THEN
				lv_status := 'ERROR'; 
				lv_message := lv_message || '|No data found for coa segment6';
				dbms_output.put_line('No data found for coa segment6');
            WHEN OTHERS THEN
				lv_status := 'ERROR'; 
				lv_message := lv_message || '|Error while fetching coa segment6';
                dbms_output.put_line('Error while fetching coa segment6: ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);
        END;
		END IF;

    -- Segment7 - Inter Company --
		IF p_in_segment7 IS NULL THEN 
		lv_segment7:= '9999';
		ELSIF p_in_segment7 = '9999' THEN
		lv_segment7:= '9999';
		ELSE
			BEGIN
				SELECT erp_coa_value
				INTO lv_segment7
				FROM xxmap.xxmap_gl_e001_kaseya_ns_intercompany
				WHERE ns_intercompany_attribute_1 = p_in_segment7
				AND   last_update_date =  ( SELECT max(last_update_date)
										    FROM xxmap.xxmap_gl_e001_kaseya_ns_intercompany
										    WHERE ns_intercompany_attribute_1 = p_in_segment7
										   );
			EXCEPTION
				WHEN NO_DATA_FOUND THEN
					lv_status := 'ERROR'; 
					lv_message := lv_message || '|No data found for coa segment7';
					dbms_output.put_line('No data found for coa segment7');
				WHEN OTHERS THEN
					lv_status := 'ERROR'; 
					lv_message := lv_message || '|Error while fetching coa segment7';
					dbms_output.put_line('Error while fetching coa segment7: ' || '->' || SUBSTR(SQLERRM, 1, 3000) || '->' || DBMS_UTILITY.format_error_backtrace);
			END;
		END IF;


		BEGIN
        lv_rec_cnt:=0;

        SELECT count(1)
        INTO   lv_rec_cnt
        FROM   xxmap.xxmap_gl_e001_exclude_accounts
        WHERE  account_no = lv_segment4;


		IF lv_segment4 < 400000 THEN
		lv_segment2:= '999';
		END IF;

		IF lv_segment4 < 400000 AND p_in_pkg_name <> 'GL' AND lv_rec_cnt=0 
 		THEN 
		lv_segment3:= '99999';
		END IF;

		IF lv_segment4 < 400000 AND p_in_pkg_name = 'GL'
 		THEN 
		lv_segment3:= '99999';
		END IF;

		lv_segment:= lv_segment1||'|'||lv_segment2||'|'||lv_segment3||'|'||lv_segment4||'|'||lv_segment5||'|'||lv_segment6||'|'||lv_segment7||'|'||lv_segment8
					 ||'|'||lv_segment9||'|'||lv_segment10;

		p_out_target_system := lv_segment;
		IF lv_status = 'ERROR' THEN
		p_out_status:= 'ERROR';
		p_out_message:= lv_message;
		ELSIF lv_status IS NULL THEN
		p_out_status:= 'SUCCESS';
		p_out_message:= 'Target COA segments fetched successfully';
		ELSE NULL;
		END IF;
        END;

    END coa_segment_mapping_prc;

END xxcnv_gl_coa_transformation_pkg;