CREATE OR REPLACE PACKAGE BODY xxsupcnv.xxksy_supplier_data_conversion_pkg IS

---------------------------------------------------------------------------------------------------------------
--                                                                                                           --
--                                               Main                                                        --
--                                                                                                           --
---------------------------------------------------------------------------------------------------------------

    PROCEDURE main AS
    BEGIN
        purge_ns_tables;
        move_to_enrich_data;
        insert_missing_data1;
       -- xxksy_zero_prepend_us_zip;
        xxksy_merge_vendors;
        xxksy_procurement_bu_map;
--        xxksy_update_address_error;

--        xxksy_update_us_state;
        revalidate_transformed_data;
    END;
---------------------------------------------------------------------------------------------------------------
--                                                                                                           --
--                             Procedure to Validate data in Staging table                                  --
--                                                                                                           --
---------------------------------------------------------------------------------------------------------------

    PROCEDURE validate_data AS
        v_error_message  VARCHAR2(4000) := '';
        v_supplier_count NUMBER := 0;
    BEGIN
---------------------------------------------------------------------------------------------------------------
-------------------------------------------- Validate Supplier file--------------------------------------------
---------------------------------------------------------------------------------------------------------------
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.stg_poz_suppliers
        ) LOOP
            v_error_message := '';
            IF
                rec.federal_reportable = 'Y'
                AND rec.federal_income_tax_type IS NULL
            THEN
                v_error_message := v_error_message || 'Federal Income Tax Type should be populated if Federal reportable is Y | ';
            END IF;

            IF rec.payment_method IS NULL THEN
                v_error_message := v_error_message || 'Payment Method should not be NULL | ';
            END IF;
            IF rec.supplier_name IS NULL THEN
                v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
            END IF;
            IF rec.supplier_number IS NULL THEN
                v_error_message := v_error_message || 'Supplier Number should not be NULL | ';
            END IF;
            IF rec.taxpayer_country IS NULL THEN
                v_error_message := v_error_message || 'Taxpayer Country should not be NULL | ';
            END IF;
            IF
                rec.taxpayer_id IS NULL
                AND rec.duns_number IS NULL
                AND rec.tax_registration_number IS NULL
            THEN
                v_error_message := v_error_message || 'At least one of the Taxpayer ID, DUNS Number, Tax Registration Number should be populated | '
                ;
            END IF;

            -- Check for duplicate supplier names
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.stg_poz_suppliers s
            WHERE
                s.supplier_name = rec.supplier_name;

            IF v_supplier_count > 1 THEN
                v_error_message := v_error_message || 'Duplicate Supplier Names | ';
            END IF;

                -- Check for duplicate supplier numbers
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.stg_poz_suppliers s
            WHERE
                s.supplier_number = rec.supplier_number;

            IF v_supplier_count > 1 THEN
                v_error_message := v_error_message || 'Duplicate Supplier Numbers | ';
            END IF;

                -- Check for duplicate taxpayer IDs
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.stg_poz_suppliers s
            WHERE
                s.taxpayer_id = rec.taxpayer_id;

            IF v_supplier_count > 1 THEN
                v_error_message := v_error_message || 'Duplicate Taxpayer IDs | ';
            END IF;
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.stg_poz_suppliers s
            WHERE
                s.tax_registration_number = rec.tax_registration_number;

            IF v_supplier_count > 1 THEN
                v_error_message := v_error_message || 'Duplicate Tax Registration Number | ';
            END IF;
            IF v_error_message IS NOT NULL THEN
                UPDATE xxsupcnv.stg_poz_suppliers
                SET
                    error_flag = 'Y',
                    error_message = rtrim(v_error_message, ' | ')
                WHERE
                    supplier_number = rec.supplier_number;

                COMMIT;
            END IF;
            -- Add more validation checks for Supplier file
        END LOOP;
---------------------------------------------------------------------------------------------------------------
--------------------------------------- Validate Supplier Address file ----------------------------------------
---------------------------------------------------------------------------------------------------------------    
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.stg_poz_supplier_addresses
        ) LOOP
            v_error_message := '';
            BEGIN
                IF rec.address_1 IS NULL THEN
                    v_error_message := v_error_message || 'Address line1 should not be NULL | ';
                END IF;
                IF rec.address_name IS NULL THEN
                    v_error_message := v_error_message || 'Address Name should not be NULL | ';
                END IF;
                IF rec.city IS NULL THEN
                    v_error_message := v_error_message || 'City should not be NULL | ';
                END IF;
                IF rec.country_code IS NULL
                   OR length(rec.country_code) != 2 THEN
                    v_error_message := v_error_message || 'COUNTRY_Code should not be NULL and be of 2 characters | ';
                END IF;

                IF rec.postal_code IS NULL THEN
                    v_error_message := v_error_message || 'Postal Code should not be NULL | ';
                END IF;
                IF
                    rec.rfq_or_bidding IS NULL
                    AND rec.ordering IS NULL
                    AND rec.pay IS NULL
                THEN
                    v_error_message := v_error_message || 'At least one of the RFQ, Ordering, Pay flags should be Y | ';
                END IF;

                IF rec.supplier_name IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_suppliers s
                WHERE
                    s.supplier_name = rec.supplier_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
                END IF;
                IF v_error_message IS NOT NULL THEN
                    UPDATE xxsupcnv.stg_poz_supplier_addresses
                    SET
                        error_flag = 'Y',
                        error_message = rtrim(v_error_message, ' | ')
                    WHERE
                        address_name = rec.address_name;

                    COMMIT;
                END IF;

            END;

        END LOOP;
---------------------------------------------------------------------------------------------------------------
-------------------------------------  Validate Supplier Assignments file  ------------------------------------
---------------------------------------------------------------------------------------------------------------    
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.stg_poz_site_assignments
        ) LOOP
            v_error_message := '';
            BEGIN
                IF rec.client_bu IS NULL THEN
                    v_error_message := v_error_message || 'Client BU should not be NULL | ';
                END IF;
                IF rec.procurement_bu IS NULL THEN
                    v_error_message := v_error_message || 'Procurement BU should not be NULL | ';
                END IF;
                IF rec.supplier_name IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_suppliers s
                WHERE
                    s.supplier_name = rec.supplier_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
                END IF;
                IF rec.supplier_site IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Site should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_supplier_sites ss
                WHERE
                    ss.supplier_site = rec.supplier_site;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Site not found in Supplier sites table | ';
                END IF;
                IF v_error_message IS NOT NULL THEN
                    UPDATE xxsupcnv.stg_poz_site_assignments
                    SET
                        error_flag = 'Y',
                        error_message = rtrim(v_error_message, ' | ')
                    WHERE
                            supplier_name = rec.supplier_name
                        AND supplier_site = rec.supplier_site
                        AND procurement_bu = rec.procurement_bu;

                END IF;

                COMMIT;
            END;

        END LOOP;
---------------------------------------------------------------------------------------------------------------
-------------------------------  Validate Supplier Business Classification file -------------------------------
---------------------------------------------------------------------------------------------------------------        
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.stg_poz_sup_bus_class
        ) LOOP
            v_error_message := '';
            IF rec.supplier_name IS NULL THEN
                v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
            END IF;
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.stg_poz_suppliers s
            WHERE
                s.supplier_name = rec.supplier_name;

            IF v_supplier_count = 0 THEN
                v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
            END IF;
            IF rec.classification IS NULL THEN
                v_error_message := v_error_message || 'Classification should not be NULL | ';
            END IF;
            IF v_error_message IS NOT NULL THEN
                UPDATE xxsupcnv.stg_poz_sup_bus_class
                SET
                    error_flag = 'Y',
                    error_message = rtrim(v_error_message, ' | ')
                WHERE
                        classification = rec.classification
                    AND supplier_name = rec.supplier_name;

            END IF;

            COMMIT;
            -- Add more validation checks for Supplier Business Classification file
        END LOOP;
---------------------------------------------------------------------------------------------------------------
---------------------------------------- Validate Supplier Contact file ---------------------------------------
---------------------------------------------------------------------------------------------------------------        
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.stg_poz_sup_contacts
        ) LOOP
            v_error_message := '';
            BEGIN
                IF rec.administrative_contact NOT IN ( 'Y', 'N', '' ) THEN
                    v_error_message := v_error_message || 'Administrative Contact should not be other values than Y, N, Blank | ';
                END IF;

                IF rec.email IS NULL THEN
                    v_error_message := v_error_message || 'E-Mail should not be NULL | ';
                END IF;
                IF rec.email LIKE '%;%' THEN
                    v_error_message := v_error_message || 'E-Mail should not contain semicolon ; | ';
                END IF;
                IF NOT regexp_like(rec.email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN
                    v_error_message := v_error_message || 'E-Mail is in incorrect format | ';
                END IF;

                IF rec.first_name IS NULL THEN
                    v_error_message := v_error_message || 'First Name should not be NULL | ';
                END IF;
                IF rec.last_name IS NULL THEN
                    v_error_message := v_error_message || 'Last Name should not be NULL | ';
                END IF;
                IF rec.supplier_name IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_suppliers s
                WHERE
                    s.supplier_name = rec.supplier_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
                END IF;

                -- Check for duplicate E-Mail values
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_sup_contacts sc
                WHERE
                    sc.email = rec.email;

                IF v_supplier_count > 1 THEN
                    v_error_message := v_error_message || 'Duplicate E-Mail values | ';
                END IF;
                IF v_error_message IS NOT NULL THEN
                    UPDATE xxsupcnv.stg_poz_sup_contacts
                    SET
                        error_flag = 'Y',
                        error_message = rtrim(v_error_message, ' | ')
                    WHERE
                            supplier_name = rec.supplier_name
                        AND first_name = rec.first_name;

                    COMMIT;
                END IF;

            END;

        END LOOP;
---------------------------------------------------------------------------------------------------------------
----------------------------------- Validate Supplier Contact Addresses file ----------------------------------
---------------------------------------------------------------------------------------------------------------        
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.stg_poz_supp_contact_addresses
        ) LOOP
            v_error_message := '';
            BEGIN
                IF rec.address_name IS NULL THEN
                    v_error_message := v_error_message || 'Address Name should not be NULL | ';
                END IF;
                IF rec.first_name IS NULL THEN
                    v_error_message := v_error_message || 'First Name should not be NULL | ';
                END IF;
                IF rec.last_name IS NULL THEN
                    v_error_message := v_error_message || 'Last Name should not be NULL | ';
                END IF;
                IF rec.supplier_name IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_suppliers s
                WHERE
                    s.supplier_name = rec.supplier_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_supplier_addresses sa
                WHERE
                    sa.address_name = rec.address_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Contact address not found in Supplier addresses table | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_sup_contacts sc
                WHERE
                        sc.first_name = rec.first_name
                    AND last_name = rec.last_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Contact details not found in Supplier contacts table | ';
                END IF;
                IF v_error_message IS NOT NULL THEN
                    UPDATE xxsupcnv.stg_poz_supp_contact_addresses
                    SET
                        error_flag = 'Y',
                        error_message = rtrim(v_error_message, ' | ')
                    WHERE
                            address_name = rec.address_name
                        AND supplier_name = rec.supplier_name;

                    COMMIT;
                END IF;

            END;
            -- Add more validation checks for Supplier Contact Addresses file
        END LOOP;
---------------------------------------------------------------------------------------------------------------
----------------------------------------- Validate Supplier Sites file ----------------------------------------
---------------------------------------------------------------------------------------------------------------        
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.stg_poz_supplier_sites
        ) LOOP
            v_error_message := '';
            BEGIN
                IF rec.address_name IS NULL THEN
                    v_error_message := v_error_message || 'Address Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_supplier_addresses sa
                WHERE
                    sa.address_name = rec.address_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Address Name not found in Supplier address table | ';
                END IF;
                IF rec.attribute1 IS NULL THEN
                    v_error_message := v_error_message || 'Attribute1 should be populated with NetSuite Vendor ID | ';
                END IF;
--                IF NOT regexp_like(rec.email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN
--                    v_error_message := v_error_message || 'Email address is in incorrect format | ';
--                END IF;

                IF
                    rec.email IS NULL
                    AND rec.communication_method = 'Email'
                THEN
                    v_error_message := v_error_message || 'E-Mail should not be NULL when Communication Method is populated as Email | '
                    ;
                END IF;

                IF rec.pay IS NULL THEN
                    v_error_message := v_error_message || 'Pay flag should not be NULL | ';
                END IF;
                IF rec.payment_method IS NULL THEN
                    v_error_message := v_error_message || 'Payment Method should not be NULL | ';
                END IF;
                IF rec.payment_terms IS NULL THEN
                    v_error_message := v_error_message || 'Payment Terms should not be NULL | ';
                END IF;
                IF rec.procurement_bu IS NULL THEN
                    v_error_message := v_error_message || 'Procurement BU should not be NULL | ';
                END IF;
                IF rec.purchasing IS NULL THEN
                    v_error_message := v_error_message || 'Purchasing flag should not be NULL | ';
                END IF;
                IF rec.supplier_name IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_suppliers s
                WHERE
                    s.supplier_name = rec.supplier_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
                END IF;
                IF rec.supplier_site IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Site should not be NULL | ';
                END IF;
                IF v_error_message IS NOT NULL THEN
                    UPDATE xxsupcnv.stg_poz_supplier_sites
                    SET
                        error_flag = 'Y',
                        error_message = rtrim(v_error_message, ' | ')
                    WHERE
                            supplier_site = rec.supplier_site
                        AND supplier_name = rec.supplier_name;

                    COMMIT;
                END IF;

            END;

        END LOOP;

        COMMIT;
    END validate_data;

    PROCEDURE move_to_enrich_data IS
    BEGIN
        BEGIN
            INSERT INTO xxsupcnv.poz_suppliers_ns (
                batch_id,
                import_action,
                supplier_name,
                supplier_name_new,
                supplier_number,
                alternate_name,
                tax_organization_type,
                supplier_type,
                inactive_date,
                business_relationship,
                parent_supplier,
                alias,
                duns_number,
                corporate_web_site,
                taxpayer_country,
                taxpayer_id,
                federal_reportable,
                federal_income_tax_type,
                state_reportable,
                tax_reporting_name,
                name_control,
                tax_verification_date,
                use_withholding_tax,
                tax_registration_number,
                payment_method,
                remittance_e_mail,
                critical_pay
            )
                SELECT
                    batch_id,
                    import_action,
                    supplier_name,
                    supplier_name_new,
                    supplier_number,
                    alternate_name,
                    tax_organization_type,
                    supplier_type,
                    inactive_date,
                    business_relationship,
                    parent_supplier,
                    alias,
                    '' duns_number,
                    corporate_web_site,
                    taxpayer_country,
                    '' taxpayer_id,
                    federal_reportable,
                    federal_income_tax_type,
                    state_reportable,
                    tax_reporting_name,
                    name_control,
                    tax_verification_date,
                    use_withholding_tax,
                    '' tax_registration_number,
                    payment_method,
                    remittance_e_mail,
                    critical_pay
                FROM
                    xxsupcnv.stg_poz_suppliers sps
                WHERE
                    supplier_number IN (
                        SELECT
                            id
                        FROM
                            xxsupcnv.supplier_conversion_cleaned_data
                           -- where merge_vendor is null
                    )
                    AND supplier_number NOT IN (
                        SELECT
                            supplier_number
                        FROM
                            employee_suppliers
                    );

            COMMIT;
        END;

        BEGIN
            INSERT INTO xxsupcnv.poz_supplier_addresses_ns (
                id,
                batch_id,
                import_action,
                supplier_name,
                address_name,
                address_name_new,
                country_code,
                address_1,
                address_2,
                address_3,
                city,
                state,
                province,
                county,
                postal_code,
                postal_plus_4_code,
                addressee,
                global_location_number,
                language,
                inactive_date,
                phone_country_code,
                phone_area_code,
                phone,
                phone_extension,
                fax_country_code,
                fax_area_code,
                fax,
                rfq_or_bidding,
                ordering,
                pay,
                email,
                delivery_channel,
                remittance_email
            )
                SELECT
                    id,
                    batch_id,
                    import_action,
                    supplier_name,
                    address_name,
                    address_name_new,
                    country_code,
                    address_1,
                    address_2,
                    address_3,
                    city,
                    state,
                    province,
                    county,
                    postal_code,
                    postal_plus_4_code,
                    addressee,
                    global_location_number,
                    language,
                    inactive_date,
                    phone_country_code,
                    phone_area_code,
                    phone,
                    phone_extension,
                    fax_country_code,
                    fax_area_code,
                    fax,
                    rfq_or_bidding,
                    ordering,
                    pay,
                    email,
                    delivery_channel,
                    remittance_email
                FROM
                    xxsupcnv.stg_poz_supplier_addresses
                WHERE
                    id IN (
                        SELECT
                            supplier_number
                        FROM
                            xxsupcnv.poz_suppliers_ns sps
                    );

            COMMIT;
        END;

        BEGIN
            INSERT INTO xxsupcnv.poz_sup_contacts_ns (
                id,
                batch_id,
                import_action,
                supplier_name,
                prefix,
                first_name,
                first_name_new,
                middle_name,
                last_name,
                last_name_new,
                job_title,
                administrative_contact,
                email,
                e_mail_new,
                phone_country_code,
                phone_area_code,
                phone,
                phone_extension,
                fax_country_code,
                fax_area_code,
                fax,
                mobile_country_code,
                mobile_area_code,
                mobile,
                inactive_date,
                user_account_action
            )
                SELECT
                    id,
                    batch_id,
                    import_action,
                    supplier_name,
                    prefix,
                    first_name,
                    first_name_new,
                    middle_name,
                    last_name,
                    last_name_new,
                    job_title,
                    administrative_contact,
                    email,
                    e_mail_new,
                    phone_country_code,
                    phone_area_code,
                    phone,
                    phone_extension,
                    fax_country_code,
                    fax_area_code,
                    fax,
                    mobile_country_code,
                    mobile_area_code,
                    mobile,
                    inactive_date,
                    user_account_action
                FROM
                    xxsupcnv.stg_poz_sup_contacts
                WHERE
                    id IN (
                        SELECT
                            supplier_number
                        FROM
                            xxsupcnv.poz_suppliers_ns sps
                    );

            COMMIT;
        END;

        BEGIN
            INSERT INTO xxsupcnv.poz_supp_contact_addresses_ns (
                id,
                batch_id,
                import_action,
                supplier_name,
                address_name,
                first_name,
                last_name,
                email
            )
                SELECT
                    id,
                    batch_id,
                    import_action,
                    supplier_name,
                    address_name,
                    first_name,
                    last_name,
                    email
                FROM
                    xxsupcnv.stg_poz_supp_contact_addresses
                WHERE
                    id IN (
                        SELECT
                            supplier_number
                        FROM
                            xxsupcnv.poz_suppliers_ns sps
                    );
--                    AND address_name IN (
--                        SELECT
--                            address_name
--                        FROM
--                            xxsupcnv.poz_supplier_addresses_ns
--                    )
--                    AND ( first_name, nvl(last_name, '.') ) IN (
--                        SELECT
--                            first_name, nvl(last_name, '.')
--                        FROM
--                            xxsupcnv.poz_sup_contacts_ns
--                    );

            COMMIT;
        END;

        BEGIN
            INSERT INTO xxsupcnv.poz_supplier_sites_ns (
                id,
                batch_id,
                import_action,
                supplier_name,
                procurement_bu,
                address_name,
                supplier_site,
                supplier_site_new,
                inactive_date,
                sourcing_only,
                purchasing,
                procurement_card,
                pay,
                primary_pay,
                income_tax_reporting_site,
                alternate_site_name,
                customer_number,
                b2b_communication_method,
                communication_method,
                email,
                fax_country_code,
                fax_area_code,
                fax,
                hold_all_new_purchasing_documents,
                purchasing_hold_reason,
                carrier,
                mode_of_transport,
                service_level,
                freight_terms,
                pay_on_receipt,
                fob,
                country_of_origin,
                buyer_managed_transportation,
                pay_on_use,
                aging_onset_point,
                aging_period_days,
                consumption_advice_frequency,
                consumption_advice_summary,
                alternate_pay_site,
                invoice_summary_level,
                gapless_invoice_numbering,
                selling_company_identifier,
                create_debit_memo_from_return,
                ship_to_exception_action,
                receipt_routing,
                over_receipt_tolerance,
                over_receipt_action,
                payment_currency,
                payment_priority,
                payment_terms,
                payment_method,
                remittance_email
            )
                SELECT
                    id,
                    batch_id,
                    import_action,
                    supplier_name,
                    procurement_bu,
                    address_name,
                    supplier_site,
                    supplier_site_new,
                    inactive_date,
                    sourcing_only,
                    purchasing,
                    procurement_card,
                    pay,
                    primary_pay,
                    income_tax_reporting_site,
                    alternate_site_name,
                    customer_number,
                    b2b_communication_method,
                    communication_method,
                    email,
                    fax_country_code,
                    fax_area_code,
                    fax,
                    hold_all_new_purchasing_documents,
                    purchasing_hold_reason,
                    carrier,
                    mode_of_transport,
                    service_level,
                    freight_terms,
                    pay_on_receipt,
                    fob,
                    country_of_origin,
                    buyer_managed_transportation,
                    pay_on_use,
                    aging_onset_point,
                    aging_period_days,
                    consumption_advice_frequency,
                    consumption_advice_summary,
                    alternate_pay_site,
                    invoice_summary_level,
                    gapless_invoice_numbering,
                    selling_company_identifier,
                    create_debit_memo_from_return,
                    ship_to_exception_action,
                    receipt_routing,
                    over_receipt_tolerance,
                    over_receipt_action,
                    payment_currency,
                    payment_priority,
                    payment_terms,
                    payment_method,
                    remittance_email
                FROM
                    xxsupcnv.stg_poz_supplier_sites
                WHERE
                    id IN (
                        SELECT
                            supplier_number
                        FROM
                            xxsupcnv.poz_suppliers_ns sps
                    );
--                    AND address_name IN (
--                        SELECT
--                            address_name
--                        FROM
--                            xxsupcnv.poz_supplier_addresses_ns
--                    )
--                    ;

            COMMIT;
        END;

        BEGIN
            INSERT INTO xxsupcnv.poz_sup_bus_class_ns (
                id,
                batch_id,
                import_action,
                supplier_name,
                classification,
                classification_new,
                subclassification,
                certifying_agency,
                certifying_agency_new,
                create_certifying_agency,
                certificate_number,
                certificate_number_new,
                start_date,
                expiration_date,
                notes,
                provided_by_first_name,
                provided_by_last_name,
                provided_by_e_mail
            )
                SELECT
                    id,
                    batch_id,
                    import_action,
                    supplier_name,
                    classification,
                    classification_new,
                    subclassification,
                    certifying_agency,
                    certifying_agency_new,
                    create_certifying_agency,
                    certificate_number,
                    certificate_number_new,
                    start_date,
                    expiration_date,
                    notes,
                    provided_by_first_name,
                    provided_by_last_name,
                    provided_by_e_mail
                FROM
                    xxsupcnv.stg_poz_sup_bus_class
                WHERE
                    id IN (
                        SELECT
                            supplier_number
                        FROM
                            xxsupcnv.poz_suppliers_ns sps
                    );

            COMMIT;
        END;

        BEGIN
            INSERT INTO xxsupcnv.poz_site_assignments_ns (
                id,
                batch_id,
                import_action,
                supplier_name,
                supplier_site,
                procurement_bu,
                client_bu,
                bill_to_bu,
                ship_to_location,
                bill_to_location,
                use_withholding_tax,
                withholding_tax_group,
                liability_distribution,
                prepayment_distribution,
                bills_payable_distribution,
                distribution_set
            )
                SELECT
                    id,
                    batch_id,
                    import_action,
                    supplier_name,
                    supplier_site,
                    procurement_bu,
                    client_bu,
                    bill_to_bu,
                    ship_to_location,
                    bill_to_location,
                    use_withholding_tax,
                    withholding_tax_group,
                    liability_distribution,
                    prepayment_distribution,
                    bills_payable_distribution,
                    distribution_set
                FROM
                    xxsupcnv.stg_poz_site_assignments
                WHERE
                    id IN (
                        SELECT
                            supplier_number
                        FROM
                            xxsupcnv.poz_suppliers_ns sps
                    )
                    AND supplier_site IN (
                        SELECT
                            supplier_site
                        FROM
                            xxsupcnv.poz_supplier_sites_ns
                    );

            COMMIT;
        END;

    END;

    PROCEDURE insert_missing_data1 IS
        v_country_code VARCHAR(50) := '';
        v_last_four    VARCHAR(10) := '';
    BEGIN
        BEGIN
            UPDATE xxsupcnv.poz_sup_contacts_ns
            SET
                fax_country_code = '',
                fax_area_code = '',
                fax = '';

            UPDATE xxsupcnv.poz_supplier_sites_ns
            SET
                fax_country_code = '',
                fax_area_code = '',
                fax = '';

            UPDATE xxsupcnv.poz_supplier_addresses_ns
            SET
                fax_country_code = '',
                fax_area_code = '',
                fax = '';

            COMMIT;
        END;
---------------------------------------------------------------------------------------------------------------
--------------------------------------------  Enrich Supplier Data --------------------------------------------
---------------------------------------------------------------------------------------------------------------
        BEGIN
            FOR rec IN (
                SELECT
                    *
                FROM
                    xxsupcnv.supplier_conversion_cleaned_data
            ) LOOP
                UPDATE xxsupcnv.poz_suppliers_ns psn
                SET
--                    psn.supplier_name = nvl(rec.name, psn.supplier_name),
--                    psn.alternate_name = nvl(rec.name, psn.alternate_name),
                    psn.supplier_type = 'Supplier',
                    taxpayer_country = nvl(rec.country_code, taxpayer_country),
                    taxpayer_id = nvl(rec.tax_id, taxpayer_id),
                    tax_registration_number = rec.tax_number
--                        case
--                            when xxksy_format_tax_registration_number(
--                                nvl(rec.tax_number, tax_registration_number),
--                                rec.country_code
--                            ) = 'CountryLogicNotFound' then
--                                nvl(rec.tax_number, tax_registration_number)
--                            else
--                                xxksy_format_tax_registration_number(
--                                    nvl(rec.tax_number, tax_registration_number),
--                                    rec.country_code
--                                )
--                        end
                    ,
                    payment_method = nvl(payment_method, 'EFT'),
                    remittance_e_mail = nvl(rec.remittance_email, remittance_e_mail),
                    critical_pay = nvl(critical_pay, 'N'),
                    federal_income_tax_type =
                        CASE
                            WHEN upper(rec.c1099_eligible) = 'YES'
                                 AND rec.c1099_misc_category IS NOT NULL THEN
                                    CASE
                                        WHEN instr(rec.c1099_misc_category, '-') = '0' THEN
                                            rec.c1099_misc_category
                                        ELSE
                                            substr(rec.c1099_misc_category,
                                                   1,
                                                   instr(rec.c1099_misc_category, '-') - 1)
                                    END
                            ELSE
                                NULL
                        END,
                    federal_reportable =
                        CASE
                            WHEN upper(rec.c1099_eligible) = 'YES'
                                 AND rec.c1099_misc_category IS NOT NULL THEN
                                'Y'
                            ELSE
                                NULL
                        END
                WHERE
                    supplier_number = rec.id;

                COMMIT;
            END LOOP;

        END;
---------------------------------------------------------------------------------------------------------------
---------------------------------------  Enrich Supplier Address Data  ----------------------------------------
---------------------------------------------------------------------------------------------------------------   
        BEGIN
            FOR rec IN (
                SELECT
                    *
                FROM
                    xxsupcnv.supplier_conversion_cleaned_data
            ) LOOP
                UPDATE xxsupcnv.poz_supplier_addresses_ns
                SET
--                    supplier_name = nvl(rec.name, supplier_name),
                    address_name = rec.address,
                    country_code = nvl(rec.country_code, country_code),
                    address_1 = nvl(rec.address1, address_1),
                    address_2 = nvl(rec.address2, address_2),
                    address_3 = nvl(rec.address3, address_3),
                    city = nvl(rec.city, city),
                    state =
                        CASE
                            WHEN rec.country_code IN ( 'CN', 'CA' ) THEN
                                NULL
                            ELSE
                                rec.state_province
                        END,
                    province =
                        CASE
                            WHEN rec.country_code IN ( 'CN', 'CA' ) THEN
                                rec.state_province
                            ELSE
                                NULL
                        END,
                    postal_code = nvl(rec.zip_code, postal_code),
                    county = nvl(rec.county, county),
                    phone_country_code = rec.phone_country_code,
                    phone_area_code = rec.phone_area_code,
                    phone = rec.phone1,
                    phone_extension = rec.phone_extension,
                    fax_country_code = '',
                    fax_area_code = '',
                    fax = '',
                    pay = 'Y',
                    ordering = 'Y',
                    email = nvl(rec.email, email),
                    remittance_email = nvl(rec.remittance_email, remittance_email),
                    delivery_channel =
                        CASE
                            WHEN nvl(rec.remittance_email, remittance_email) IS NOT NULL THEN
                                'EMAIL'
                            ELSE
                                NULL
                        END
                WHERE
                    id = rec.id;

                COMMIT;
            END LOOP;
        END;
---------------------------------------------------------------------------------------------------------------
----------------------------------------  Enrich Supplier Contact Data  ---------------------------------------
---------------------------------------------------------------------------------------------------------------         

        BEGIN
            FOR rec IN (
                SELECT
                    *
                FROM
                    xxsupcnv.supplier_conversion_cleaned_data
            ) LOOP
                UPDATE xxsupcnv.poz_sup_contacts_ns
                SET
--                    supplier_name = nvl(rec.name, supplier_name),
                    first_name = substr(rec.email,
                                        1,
                                        instr(rec.email, '@') - 1),
                    last_name = nvl(last_name, '.'),
                    administrative_contact =
                        CASE
                            WHEN administrative_contact NOT IN ( 'Y', 'N', '' ) THEN
                                NULL
                        END,
                    phone_country_code = rec.phone_country_code,
                    phone_area_code = rec.phone_area_code,
                    phone = rec.phone1,
                    phone_extension = rec.phone_extension,
                    email = nvl(rec.email, email),
                    user_account_action = 'Y',
                    fax_country_code = '',
                    fax_area_code = '',
                    fax = ''
                WHERE
                    id = rec.id;

                COMMIT;
            END LOOP;
        END;
---------------------------------------------------------------------------------------------------------------
----------------------------------- Enrich Supplier Contact Addresses Data ------------------------------------
---------------------------------------------------------------------------------------------------------------
        BEGIN
            FOR rec IN (
                SELECT
                    *
                FROM
                    xxsupcnv.supplier_conversion_cleaned_data
            ) LOOP
                UPDATE xxsupcnv.poz_supp_contact_addresses_ns
                SET
--                    supplier_name = nvl(rec.name, supplier_name),
                    address_name = (
                        SELECT
                            address_name
                        FROM
                            xxsupcnv.poz_supplier_addresses_ns b
                        WHERE
                            b.id = rec.id
                    ),
                    first_name = (
                        SELECT
                            first_name
                        FROM
                            xxsupcnv.poz_sup_contacts_ns b
                        WHERE
                            b.id = rec.id
                    ),
                    last_name = (
                        SELECT
                            last_name
                        FROM
                            xxsupcnv.poz_sup_contacts_ns b
                        WHERE
                            b.id = rec.id
                    ),
                    email = (
                        SELECT
                            email
                        FROM
                            xxsupcnv.poz_sup_contacts_ns b
                        WHERE
                            b.id = rec.id
                    )
                WHERE
                    id = rec.id;

                COMMIT;
            END LOOP;
        END;
---------------------------------------------------------------------------------------------------------------
-------------------------------   Enrich Supplier Business Classification Data  -------------------------------
---------------------------------------------------------------------------------------------------------------           

        BEGIN
            FOR rec IN (
                SELECT
                    *
                FROM
                    xxsupcnv.supplier_conversion_cleaned_data
            ) LOOP
                UPDATE xxsupcnv.poz_sup_bus_class_ns
                SET
--                    supplier_name = nvl(rec.name, supplier_name),
                    classification = 'Others',
                    provided_by_first_name = (
                        SELECT
                            first_name
                        FROM
                            xxsupcnv.poz_sup_contacts_ns b
                        WHERE
                            b.id = rec.id
                    ),
                    provided_by_last_name = (
                        SELECT
                            last_name
                        FROM
                            xxsupcnv.poz_sup_contacts_ns b
                        WHERE
                            b.id = rec.id
                    ),
                    provided_by_e_mail = (
                        SELECT
                            email
                        FROM
                            xxsupcnv.poz_sup_contacts_ns b
                        WHERE
                            b.id = rec.id
                    )
                WHERE
                    id = rec.id;

                COMMIT;
            END LOOP;
        END;
---------------------------------------------------------------------------------------------------------------
-----------------------------------------  Enrich Supplier Sites Data  ----------------------------------------
---------------------------------------------------------------------------------------------------------------          
        BEGIN
            FOR rec IN (
                SELECT
                    *
                FROM
                    xxsupcnv.supplier_conversion_cleaned_data
            ) LOOP
                BEGIN
                    v_country_code := '';
                    v_last_four := '';
--                dbms_output.put_line('Vendor id :'||rec.id);
                    SELECT DISTINCT
                        country_code,
                        substr(account_number,
                               length(account_number) - 3)
                    INTO
                        v_country_code,
                        v_last_four
                    FROM
                        supplier_conversion_cleaned_data
                    WHERE
                        id = rec.id;

                    UPDATE xxsupcnv.poz_supplier_sites_ns
                    SET
--                    supplier_name = nvl(rec.name, supplier_name),
                        supplier_site = v_country_code
                                        || '_'
                                        || nvl(payment_method, 'EFT')
                                        || '_'
                                        || v_last_four,
                        procurement_bu = 'US USD BU',
                        address_name = (
                            SELECT
                                address_name
                            FROM
                                xxsupcnv.poz_supplier_addresses_ns b
                            WHERE
                                b.id = rec.id
                        ),
                        purchasing = 'Y',
                        pay = 'Y',
                        income_tax_reporting_site =
                            CASE
                                WHEN (
                                    SELECT
                                        federal_reportable
                                    FROM
                                        xxsupcnv.poz_suppliers_ns
                                    WHERE
                                        supplier_number = rec.id
                                ) = 'Y' THEN
                                    'Y'
                                ELSE
                                    NULL
                            END,
                        email = nvl(rec.email, email),
                        payment_method = nvl(payment_method, 'EFT'),
                        remittance_email = nvl(rec.remittance_email, remittance_email),
                        attribute1 = rec.id,
                        payment_terms = nvl(payment_terms, 'Net 30'),
                        over_receipt_action = ''
                    WHERE
                        id = rec.id;

                    COMMIT;
                EXCEPTION
                    WHEN OTHERS THEN
                        dbms_output.put_line('Vendor id :' || rec.id);
                END;
            END LOOP;
        END;
---------------------------------------------------------------------------------------------------------------
-------------------------------------     Enrich Site Assignments Data     ------------------------------------
---------------------------------------------------------------------------------------------------------------  
        BEGIN
            FOR rec IN (
                SELECT
                    *
                FROM
                    xxsupcnv.supplier_conversion_cleaned_data
            ) LOOP
                UPDATE xxsupcnv.poz_site_assignments_ns
                SET
--                    supplier_name = nvl(rec.name, supplier_name),
                    procurement_bu = 'US USD BU',
                    supplier_site = (
                        SELECT
                            supplier_site
                        FROM
                            poz_supplier_sites_ns
                        WHERE
                            id = rec.id
                    )
                WHERE
                    id = rec.id;

                COMMIT;
            END LOOP;
        END;

    END insert_missing_data1;
---------------------------------------------------------------------------------------------------------------
--                                                                                                           --
--                             Procedure to Data Enrichment                                                  --
--                                                                                                           --
---------------------------------------------------------------------------------------------------------------
--    PROCEDURE insert_missing_data IS
--
--        v_taxpayer_id    VARCHAR2(50) := '';
--        v_tax_regi_num   VARCHAR2(50) := '';
--        v_error_message  VARCHAR2(4000) := '';
--        v_supplier_count NUMBER := 0;
--    BEGIN
--        BEGIN
--            UPDATE xxsupcnv.poz_sup_contacts_ns
--            SET
--                fax_country_code = '',
--                fax_area_code = '',
--                fax = '';
--
--            UPDATE xxsupcnv.poz_supplier_sites_ns
--            SET
--                fax_country_code = '',
--                fax_area_code = '',
--                fax = '';
--
--            UPDATE xxsupcnv.poz_supplier_addresses_ns
--            SET
--                fax_country_code = '',
--                fax_area_code = '',
--                fax = '';
--
--            COMMIT;
--        END;
--
--        dbms_output.put_line('Update Federal Reportable Type');
--        BEGIN
--            UPDATE xxsupcnv.poz_suppliers_ns a
--            SET
--                federal_income_tax_type = (
--                    SELECT
--                        substr(c1099_misc_category,
--                               1,
--                               instr(c1099_misc_category, '-') - 1)
--                    FROM
--                        xxsupcnv.supplier_conversion_cleaned_data b
--                    WHERE
--                            b.id = a.supplier_number
--                        AND upper(b.c1099_eligible) = 'YES'
--                        AND c1099_misc_category IS NOT NULL
--                ),
--                federal_reportable = (
--                    SELECT
--                        'Y'
--                    FROM
--                        xxsupcnv.supplier_conversion_cleaned_data b
--                    WHERE
--                            b.id = a.supplier_number
--                        AND upper(b.c1099_eligible) = 'YES'
--                        AND c1099_misc_category IS NOT NULL
--                );
--
--        EXCEPTION
--            WHEN OTHERS THEN
--                dbms_output.put_line('Error Federal Reportable Type - ' || sqlerrm);
--        END;
-----------------------------------------------------------------------------------------------------------------
----------------------------------------------  Enrich Supplier Data --------------------------------------------
-----------------------------------------------------------------------------------------------------------------
--        dbms_output.put_line('Update Supplier Data');
--        FOR rec IN (
--            SELECT
--                *
--            FROM
--                xxsupcnv.poz_suppliers_ns
--        ) LOOP
--            BEGIN
--                dbms_output.put_line('Update Supplier Data Record SUPPLIER_NUMBER - ' || rec.supplier_number);
--               ------Temp---------------
--                IF rec.payment_method IS NULL THEN
--                    UPDATE xxsupcnv.poz_suppliers_ns a
--                    SET
--                        payment_method = 'EFT'
--                    WHERE
--                        a.supplier_number = rec.supplier_number;
--
--                END IF;
--
--                IF rec.supplier_name IS NULL THEN
--                    UPDATE xxsupcnv.poz_suppliers_ns a
--                    SET
--                        supplier_name = (
--                            SELECT DISTINCT
--                                name
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data b
--                            WHERE
--                                b.id = a.supplier_number
--                        )
--                    WHERE
--                        a.supplier_number = rec.supplier_number;
--
--                END IF;
--
--                IF
--                    rec.taxpayer_id IS NULL
--                    AND rec.duns_number IS NULL
--                    AND rec.tax_registration_number IS NULL
--                THEN
--                v_taxpayer_id := null;
--                v_tax_regi_num := null;
--                    SELECT DISTINCT
--                        tax_id,
--                        tax_number
--                    INTO
--                        v_taxpayer_id,
--                        v_tax_regi_num
--                    FROM
--                        xxsupcnv.supplier_conversion_cleaned_data b
--                    WHERE
--                        b.id = rec.supplier_number;
--
--                    IF v_taxpayer_id IS NOT NULL THEN
--                        UPDATE xxsupcnv.poz_suppliers_ns a
--                        SET
--                            taxpayer_id = v_taxpayer_id
--                        WHERE
--                            a.supplier_number = rec.supplier_number;
--
--                    ELSIF v_tax_regi_num IS NOT NULL THEN
--                        UPDATE xxsupcnv.poz_suppliers_ns a
--                        SET
--                            taxpayer_id = v_taxpayer_id
--                        WHERE
--                            a.supplier_number = rec.supplier_number;
--
--                    END IF;
--
--                END IF;
--
--                IF rec.supplier_number IS NULL THEN
--                    UPDATE xxsupcnv.poz_suppliers_ns a
--                    SET
--                        supplier_name = (
--                            SELECT DISTINCT
--                                id
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data b
--                            WHERE
--                                b.name = a.supplier_name
--                        )
--                    WHERE
--                        a.supplier_name = rec.supplier_name;
--
--                END IF;
--
--                IF rec.taxpayer_country IS NULL THEN
--                    UPDATE xxsupcnv.poz_suppliers_ns a
--                    SET
--                        taxpayer_country = (
--                            SELECT DISTINCT
--                                country_code
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data b
--                            WHERE
--                                b.id = a.supplier_number
--                        )
--                    WHERE
--                        a.supplier_number = rec.supplier_number;
--
--                END IF;
--
--            EXCEPTION
--                WHEN OTHERS THEN
--                    dbms_output.put_line('Error Supplier Data - ' || sqlerrm);
--            END;
--
--            COMMIT;
--            -- Add more validation checks for Supplier file
--
--        END LOOP;
--
--        ------temp-----
--
----        UPDATE xxsupcnv.poz_suppliers_ns
----        SET
----            taxpayer_id = (
----                SELECT
----                    ceil(dbms_random.value(10, 99))
----                    || '-'
----                    || substr(supplier_number, 2)
----                FROM
----                    dual
----            )
----        WHERE
----            taxpayer_id IS NULL;
----
----        COMMIT;
--
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------  Enrich Supplier Address Data  ----------------------------------------
-----------------------------------------------------------------------------------------------------------------    
--        dbms_output.put_line('Update Supplier Address Data');
--        FOR rec IN (
--            SELECT
--                *
--            FROM
--                xxsupcnv.poz_supplier_addresses_ns
--            WHERE
--                id NOT IN (
--                    SELECT
--                        id
--                    FROM
--                        xxsupcnv.poz_supplier_addresses_ns
--                    HAVING
--                        COUNT(1) > 1
--                    GROUP BY
--                        id, supplier_name
--                )
--        ) LOOP
--            v_error_message := '';
--            BEGIN
--                IF rec.address_1 IS NULL THEN
--                    UPDATE xxsupcnv.poz_supplier_addresses_ns
--                    SET
--                        address_1 = (
--                            SELECT
--                                address1
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                    id = rec.id
--                                AND zip_code IS NOT NULL
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.address_name IS NULL THEN
--                    UPDATE xxsupcnv.poz_supplier_addresses_ns
--                    SET
--                        address_name = (
--                            SELECT
--                                address1
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                    id = rec.id
--                                AND zip_code IS NOT NULL
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.city IS NULL THEN
--                    UPDATE xxsupcnv.poz_supplier_addresses_ns
--                    SET
--                        city = (
--                            SELECT
--                                city
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                    id = rec.id
--                                AND zip_code IS NOT NULL
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.country_code IS NULL
--                   OR length(rec.country_code) != 2 THEN
--                    UPDATE xxsupcnv.poz_supplier_addresses_ns
--                    SET
--                        country_code = (
--                            SELECT
--                                country_code
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                id = rec.id
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.postal_code IS NULL THEN
--                    UPDATE xxsupcnv.poz_supplier_addresses_ns
--                    SET
--                        postal_code = (
--                            SELECT
--                                zip_code
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                    id = rec.id
--                                AND zip_code IS NOT NULL
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--                ---Temp -- 
--                IF
--                    rec.rfq_or_bidding IS NULL
--                    AND rec.ordering IS NULL
--                    AND rec.pay IS NULL
--                THEN
--                    UPDATE xxsupcnv.poz_supplier_addresses_ns
--                    SET
--                        pay = 'Y',
--                        ordering = 'Y'
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.supplier_name IS NULL THEN
--                    UPDATE xxsupcnv.poz_supplier_addresses_ns
--                    SET
--                        supplier_name = (
--                            SELECT
--                                name
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                id = rec.id
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                COMMIT;
--            END;
--
--        END LOOP;
-----------------------------------------------------------------------------------------------------------------
------------------------------------------  Enrich Supplier Contact Data  ---------------------------------------
-----------------------------------------------------------------------------------------------------------------        
--        dbms_output.put_line('Update Supplier Contact Data');
--        FOR rec IN (
--            SELECT
--                *
--            FROM
--                xxsupcnv.poz_sup_contacts_ns
--            WHERE
--                id NOT IN (
--                    SELECT
--                        id
--                    FROM
--                        xxsupcnv.poz_sup_contacts_ns
--                    HAVING
--                        COUNT(1) > 1
--                    GROUP BY
--                        id
--                )
--        ) LOOP
--            BEGIN
--                IF rec.administrative_contact NOT IN ( 'Y', 'N', '' ) THEN
--                    UPDATE xxsupcnv.poz_sup_contacts_ns
--                    SET
--                        administrative_contact = ''
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.phone_area_code IS NULL
--                   OR rec.phone_country_code IS NULL THEN
--                    UPDATE xxsupcnv.poz_sup_contacts_ns
--                    SET
--                        phone_country_code = (
--                            SELECT
--                                phone_country_code
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                id = rec.id
--                        ),
--                        phone_area_code = (
--                            SELECT
--                                phone_area_code
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                id = rec.id
--                        ),
--                        phone = (
--                            SELECT
--                                phone1
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                id = rec.id
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.email IS NULL THEN
--                    UPDATE xxsupcnv.poz_sup_contacts_ns
--                    SET
--                        email = (
--                            SELECT
--                                email
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                id = rec.id
--                        )
--                    WHERE
--                        id = rec.id;
--
--                    UPDATE xxsupcnv.poz_sup_contacts_ns
--                    SET
--                        first_name = (
--                            SELECT
--                                substr(email,
--                                       1,
--                                       instr(email, '@') - 1)
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                id = rec.id
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.last_name IS NULL THEN
--                    UPDATE xxsupcnv.poz_sup_contacts_ns
--                    SET
--                        last_name = '.'
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.supplier_name IS NULL THEN
--                    UPDATE xxsupcnv.poz_sup_contacts_ns
--                    SET
--                        supplier_name = (
--                            SELECT
--                                name
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                id = rec.id
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                COMMIT;
--            END;
--        END LOOP;
-----------------------------------------------------------------------------------------------------------------
---------------------------------   Enrich Supplier Business Classification Data  -------------------------------
-----------------------------------------------------------------------------------------------------------------   
--        dbms_output.put_line('Update Supplier Business Classification Data');
--        FOR rec IN (
--            SELECT
--                *
--            FROM
--                xxsupcnv.poz_sup_bus_class_ns
--        ) LOOP
--            BEGIN
--                IF rec.supplier_name IS NULL THEN
--                    UPDATE xxsupcnv.poz_sup_bus_class_ns
--                    SET
--                        supplier_name = (
--                            SELECT
--                                name
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                id = rec.id
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--          --temp--
--                IF rec.classification IS NULL THEN
--                    UPDATE xxsupcnv.poz_sup_bus_class_ns
--                    SET
--                        classification = 'Others'
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--            -- Add more validation checks for Supplier Business Classification file
--                COMMIT;
--            END;
--        END LOOP;
--        ----Update provided by last name as . period -----
--        BEGIN
--            UPDATE xxsupcnv.poz_sup_bus_class_ns a
--            SET
--                provided_by_last_name = (
--                    SELECT
--                        last_name
--                    FROM
--                        xxsupcnv.poz_sup_contacts_ns b
--                    WHERE
--                            a.id = b.id
--            --    and nvl(a.provided_by_first_name,'X') = nvl(b.first_name,'X')
--                        AND b.last_name IS NOT NULL
--                ),
--                provided_by_first_name = (
--                    SELECT
--                        first_name
--                    FROM
--                        xxsupcnv.poz_sup_contacts_ns b
--                    WHERE
--                            a.id = b.id
--            --    and a.provided_by_first_name = b.first_name
--                        AND b.first_name IS NOT NULL
--                ),
--                provided_by_e_mail = (
--                    SELECT
--                        email
--                    FROM
--                        xxsupcnv.poz_sup_contacts_ns b
--                    WHERE
--                            a.id = b.id
--            --    and a.provided_by_first_name = b.first_name
--                        AND b.email IS NOT NULL
--                );
--
--            COMMIT;
--        END;
-----------------------------------------------------------------------------------------------------------------
------------------------------------- Enrich Supplier Contact Addresses Data ----------------------------------
-----------------------------------------------------------------------------------------------------------------       
--        dbms_output.put_line('Update Supplier Contact Addresses Data');
--        FOR rec IN (
--            SELECT
--                *
--            FROM
--                xxsupcnv.poz_supp_contact_addresses_ns
--            WHERE
--                id NOT IN (
--                    SELECT
--                        id
--                    FROM
--                        xxsupcnv.poz_supp_contact_addresses_ns
--                    HAVING
--                        COUNT(1) > 1
--                    GROUP BY
--                        id
--                )
--        ) LOOP
--            v_error_message := '';
--            BEGIN
--                IF rec.address_name IS NULL THEN
--                    UPDATE xxsupcnv.poz_supp_contact_addresses_ns
--                    SET
--                        address_name = (
--                            SELECT
--                                address1
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                    id = rec.id
--                                AND zip_code IS NOT NULL
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.last_name IS NULL THEN
--                    UPDATE xxsupcnv.poz_supp_contact_addresses_ns
--                    SET
--                        last_name = '.'
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.supplier_name IS NULL THEN
--                    UPDATE xxsupcnv.poz_supp_contact_addresses_ns
--                    SET
--                        supplier_name = (
--                            SELECT
--                                name
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                id = rec.id
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                COMMIT;
--            END;
--            -- Add more validation checks for Supplier Contact Addresses file
--        END LOOP;
-----------------------------------------------------------------------------------------------------------------
-------------------------------------------  Enrich Supplier Sites Data  ----------------------------------------
-----------------------------------------------------------------------------------------------------------------        
--        dbms_output.put_line('Update Supplier Sites Data');
--        FOR rec IN (
--            SELECT
--                *
--            FROM
--                xxsupcnv.poz_supplier_sites_ns
--        ) LOOP
--            BEGIN
--                IF rec.attribute1 IS NULL THEN
--                    UPDATE xxsupcnv.poz_supplier_sites_ns
--                    SET
--                        attribute1 = rec.id
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.address_name IS NULL THEN
--                    UPDATE xxsupcnv.poz_supp_contact_addresses_ns
--                    SET
--                        address_name = (
--                            SELECT
--                                address1
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                    id = rec.id
--                                AND zip_code IS NOT NULL
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.payment_method IS NULL THEN
--                    UPDATE xxsupcnv.poz_supplier_sites_ns
--                    SET
--                        payment_method = 'EFT'
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--                ---temp--
--                IF rec.payment_terms IS NULL THEN
--                    UPDATE xxsupcnv.poz_supplier_sites_ns
--                    SET
--                        payment_terms = 'NET 30'
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--
--                IF rec.supplier_name IS NULL THEN
--                    UPDATE xxsupcnv.poz_supplier_sites_ns
--                    SET
--                        supplier_name = (
--                            SELECT
--                                name
--                            FROM
--                                xxsupcnv.supplier_conversion_cleaned_data
--                            WHERE
--                                id = rec.id
--                        )
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--                --temp--
--                IF
--                    rec.pay IS NULL
--                    AND rec.purchasing IS NULL
--                THEN
--                    UPDATE xxsupcnv.poz_supplier_sites_ns
--                    SET
--                        pay = 'Y',
--                        purchasing = 'Y'
--                    WHERE
--                        id = rec.id;
--
--                END IF;
--                
--                
--
--                COMMIT;
--            END;
--        END LOOP;
--        ----Update INCOME_TAX_REPORTING_SITE on Supplier Site record----
--        FOR rec IN (
--            SELECT
--                *
--            FROM
--                xxsupcnv.poz_suppliers_ns
--            WHERE
--                federal_reportable = 'Y'
--        ) LOOP
--            BEGIN
--                UPDATE xxsupcnv.poz_supplier_sites_ns
--                SET
--                    income_tax_reporting_site = 'Y'
--                WHERE
--                        id = rec.supplier_number
--                    AND ROWNUM < 2;
--
--            END;
--        END LOOP;
--        ---Update procurement bu to US USD BU----
--        UPDATE xxsupcnv.poz_supplier_sites_ns
--        SET
--            procurement_bu = 'US USD BU';
--
-----------------------------------------------------------------------------------------------------------------
---------------------------------------     Enrich Site Assignments Data     ------------------------------------
-----------------------------------------------------------------------------------------------------------------    
--        dbms_output.put_line('Update Supplier Site Assignments Data');
--        FOR rec IN (
--            SELECT
--                *
--            FROM
--                xxsupcnv.poz_site_assignments_ns
--        ) LOOP
--            BEGIN               
--
--                -- IF REC.SUPPLIER_SITE IS NULL THEN
--                --     V_ERROR_MESSAGE := V_ERROR_MESSAGE || 'Supplier Site should not be NULL | ';
--                -- END IF;
--                UPDATE xxsupcnv.poz_site_assignments_ns
--                SET
--                    procurement_bu = (
--                        SELECT
--                            business_unit_name
--                        FROM
--                            xxsupcnv.enterprise_structure_mapping
--                        WHERE
--                            subsidiary = rec.procurement_bu
--                    )
--                WHERE
--                    id = rec.id;
--
--            END;
--        END LOOP;
--
--    END insert_missing_data;

    PROCEDURE generate_error_report IS
        v_x NUMBER := 0;
    BEGIN
        v_x := 1;
    END generate_error_report;
---------------------------------------------------------------------------------------------------------------
--                                                                                                           --
--                             Procedure to Re-Validate Cleaned Data                                         --
--                                                                                                           --
---------------------------------------------------------------------------------------------------------------
    PROCEDURE revalidate_transformed_data IS
        v_error_message  VARCHAR2(4000) := '';
        v_supplier_count NUMBER := 0;
    BEGIN
---------------------------------------------------------------------------------------------------------------
-------------------------------------------- Validate Supplier file--------------------------------------------
---------------------------------------------------------------------------------------------------------------
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.poz_suppliers_ns
        ) LOOP
            v_error_message := '';
            IF
                rec.federal_reportable = 'Y'
                AND rec.federal_income_tax_type IS NULL
            THEN
                v_error_message := v_error_message || 'Federal Income Tax Type should be populated if Federal reportable is Y | ';
            END IF;

            IF rec.payment_method IS NULL THEN
                v_error_message := v_error_message || 'Payment Method should not be NULL | ';
            END IF;
            IF rec.supplier_name IS NULL THEN
                v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
            END IF;
            IF rec.supplier_number IS NULL THEN
                v_error_message := v_error_message || 'Supplier Number should not be NULL | ';
            END IF;
            IF rec.taxpayer_country IS NULL THEN
                v_error_message := v_error_message || 'Taxpayer Country should not be NULL | ';
            END IF;
            IF
                rec.taxpayer_id IS NULL
                AND rec.duns_number IS NULL
                AND rec.tax_registration_number IS NULL
            THEN
                v_error_message := v_error_message || 'At least one of the Taxpayer ID, DUNS Number, Tax Registration Number should be populated | '
                ;
            END IF;

            -- Check for duplicate supplier names
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.poz_suppliers_ns s
            WHERE
                s.supplier_name = rec.supplier_name;

            IF v_supplier_count > 1 THEN
                v_error_message := v_error_message || 'Duplicate Supplier Names | ';
            END IF;

                -- Check for duplicate supplier numbers
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.poz_suppliers_ns s
            WHERE
                s.supplier_number = rec.supplier_number;

            IF v_supplier_count > 1 THEN
                v_error_message := v_error_message || 'Duplicate Supplier Numbers | ';
            END IF;

                -- Check for duplicate taxpayer IDs
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.poz_suppliers_ns s
            WHERE
                s.taxpayer_id = rec.taxpayer_id;

            IF v_supplier_count > 1 THEN
                v_error_message := v_error_message || 'Duplicate Taxpayer IDs | ';
            END IF;
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.poz_suppliers_ns s
            WHERE
                s.tax_registration_number = rec.tax_registration_number;

            IF v_supplier_count > 1 THEN
                v_error_message := v_error_message || 'Duplicate Tax Registration Number | ';
            END IF;
            IF v_error_message IS NOT NULL THEN
                UPDATE xxsupcnv.poz_suppliers_ns
                SET
                    error_flag = 'Y',
                    error_message = rtrim(v_error_message, ' | ')
                WHERE
                    supplier_number = rec.supplier_number;

                COMMIT;
            END IF;
            -- Add more validation checks for Supplier file

        END LOOP;
---------------------------------------------------------------------------------------------------------------
--------------------------------------- Validate Supplier Address file ----------------------------------------
---------------------------------------------------------------------------------------------------------------    
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.poz_supplier_addresses_ns
        ) LOOP
            v_error_message := '';
            BEGIN
                IF rec.address_1 IS NULL THEN
                    v_error_message := v_error_message || 'Address line1 should not be NULL | ';
                END IF;
                IF rec.address_name IS NULL THEN
                    v_error_message := v_error_message || 'Address Name should not be NULL | ';
                END IF;
                IF rec.city IS NULL THEN
                    v_error_message := v_error_message || 'City should not be NULL | ';
                END IF;
                IF rec.country_code IS NULL
                   OR length(rec.country_code) != 2 THEN
                    v_error_message := v_error_message || 'COUNTRY_Code should not be NULL and be of 2 characters | ';
                END IF;

                IF rec.postal_code IS NULL THEN
                    v_error_message := v_error_message || 'Postal Code should not be NULL | ';
                END IF;
                IF
                    rec.state IS NULL
                    AND rec.country_code IN ( 'IN', 'US' )
                THEN
                    v_error_message := v_error_message || 'State should not be NULL for US, IN | ';
                END IF;

                IF
                    length(rec.postal_code) NOT IN ( 5, 10 )
                    AND rec.country_code IN ( 'US' )
                THEN
                    v_error_message := v_error_message || 'Postal Code length for US should be 5 or 10 | ';
                END IF;

                IF
                    rec.province IS NULL
                    AND rec.country_code IN ( 'CN', 'CA' )
                THEN
                    v_error_message := v_error_message || 'Province should not be NULL for CA, CN | ';
                END IF;

                IF
                    rec.rfq_or_bidding IS NULL
                    AND rec.ordering IS NULL
                    AND rec.pay IS NULL
                THEN
                    v_error_message := v_error_message || 'At least one of the RFQ, Ordering, Pay flags should be Y | ';
                END IF;

                IF rec.supplier_name IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_suppliers s
                WHERE
                    s.supplier_name = rec.supplier_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_suppliers_ns s
                WHERE
                        s.error_flag = 'Y'
                    AND s.supplier_name = rec.supplier_name;

                IF v_supplier_count > 0 THEN
                    v_error_message := v_error_message || 'Supplier Record is in Error Status | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_supplier_addresses_ns s
                WHERE
                    s.address_name = rec.address_name;

                IF v_supplier_count > 1 THEN
                    v_error_message := v_error_message || 'Duplicate Address Name | ';
                END IF;
                IF v_error_message IS NOT NULL THEN
                    UPDATE xxsupcnv.poz_supplier_addresses_ns
                    SET
                        error_flag = 'Y',
                        error_message = rtrim(v_error_message, ' | ')
                    WHERE
                        id = rec.id;

                    COMMIT;
                END IF;

            END;

        END LOOP;

---------------------------------------------------------------------------------------------------------------
-------------------------------  Validate Supplier Business Classification file -------------------------------
---------------------------------------------------------------------------------------------------------------        
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.poz_sup_bus_class_ns
        ) LOOP
            v_error_message := '';
            IF rec.supplier_name IS NULL THEN
                v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
            END IF;
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.stg_poz_suppliers s
            WHERE
                s.supplier_name = rec.supplier_name;

            IF v_supplier_count = 0 THEN
                v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
            END IF;
            IF rec.classification IS NULL THEN
                v_error_message := v_error_message || 'Classification should not be NULL | ';
            END IF;
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.poz_sup_bus_class_ns s,
                xxsupcnv.poz_sup_contacts_ns  c
            WHERE
                    s.id = rec.id
                AND c.id = s.id
                AND c.first_name = s.provided_by_first_name;

            IF v_supplier_count = 0 THEN
                v_error_message := v_error_message || 'First_Name in Business Classification should match with Supplier Contacts | ';
            END IF;
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.poz_sup_bus_class_ns s,
                xxsupcnv.poz_sup_contacts_ns  c
            WHERE
                    s.id = rec.id
                AND c.id = s.id
                AND c.last_name = s.provided_by_last_name;

            IF v_supplier_count = 0 THEN
                v_error_message := v_error_message || 'Last_Name in Business Classification should match with Supplier Contacts | ';
            END IF;
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.poz_sup_bus_class_ns s,
                xxsupcnv.poz_sup_contacts_ns  c
            WHERE
                    s.id = rec.id
                AND c.id = s.id
                AND c.email = s.provided_by_e_mail;

            IF v_supplier_count = 0 THEN
                v_error_message := v_error_message || 'Email in Business Classification should match with Supplier Contacts | ';
            END IF;
            v_supplier_count := 0;
            SELECT
                COUNT(*)
            INTO v_supplier_count
            FROM
                xxsupcnv.poz_suppliers_ns s
            WHERE
                    s.error_flag = 'Y'
                AND s.supplier_name = rec.supplier_name;

            IF v_supplier_count > 0 THEN
                v_error_message := v_error_message || 'Supplier Record is in Error Status | ';
            END IF;
            IF v_error_message IS NOT NULL THEN
                UPDATE xxsupcnv.poz_sup_bus_class_ns
                SET
                    error_flag = 'Y',
                    error_message = rtrim(v_error_message, ' | ')
                WHERE
                        nvl(classification, 'x') = nvl(rec.classification, 'x')
                    AND supplier_name = rec.supplier_name;

                COMMIT;
            END IF;
            -- Add more validation checks for Supplier Business Classification file
        END LOOP;
---------------------------------------------------------------------------------------------------------------
---------------------------------------- Validate Supplier Contact file ---------------------------------------
---------------------------------------------------------------------------------------------------------------        
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.poz_sup_contacts_ns
        ) LOOP
            v_error_message := '';
            BEGIN
                IF rec.administrative_contact NOT IN ( 'Y', 'N', '' ) THEN
                    v_error_message := v_error_message || 'Administrative Contact should not be other values than Y, N, Blank | ';
                END IF;

                IF rec.email IS NULL THEN
                    v_error_message := v_error_message || 'E-Mail should not be NULL | ';
                END IF;
                IF rec.email LIKE '%;%' THEN
                    v_error_message := v_error_message || 'E-Mail should not contain semicolon ; | ';
                END IF;
                IF NOT regexp_like(rec.email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN
                    v_error_message := v_error_message || 'E-Mail is in incorrect format | ';
                    dbms_output.put_line('Vendor id :'
                                         || rec.id
                                         || ' Email: '
                                         || rec.email);

                END IF;

                IF rec.first_name IS NULL THEN
                    v_error_message := v_error_message || 'First Name should not be NULL | ';
                END IF;
                IF rec.last_name IS NULL THEN
                    v_error_message := v_error_message || 'Last Name should not be NULL | ';
                END IF;
                IF rec.supplier_name IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_suppliers s
                WHERE
                    s.supplier_name = rec.supplier_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
                END IF;

                -- Check for duplicate E-Mail values
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_sup_contacts_ns sc
                WHERE
                        sc.email = rec.email
                    AND sc.id = rec.id;

                IF v_supplier_count > 1 THEN
                    v_error_message := v_error_message || 'Duplicate E-Mail values | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_suppliers_ns s
                WHERE
                        s.error_flag = 'Y'
                    AND s.supplier_name = rec.supplier_name;

                IF v_supplier_count > 0 THEN
                    v_error_message := v_error_message || 'Supplier Record is in Error Status | ';
                END IF;
                IF v_error_message IS NOT NULL THEN
                    UPDATE xxsupcnv.poz_sup_contacts_ns
                    SET
                        error_flag = 'Y',
                        error_message = rtrim(v_error_message, ' | ')
                    WHERE
                            supplier_name = rec.supplier_name
                        AND nvl(email, 'X') = nvl(rec.email, 'X');

                    v_error_message := '';
                    COMMIT;
                END IF;

            END;

        END LOOP;
---------------------------------------------------------------------------------------------------------------
----------------------------------- Validate Supplier Contact Addresses file ----------------------------------
---------------------------------------------------------------------------------------------------------------        
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.poz_supp_contact_addresses_ns
        ) LOOP
            v_error_message := '';
            BEGIN
                IF rec.address_name IS NULL THEN
                    v_error_message := v_error_message || 'Address Name should not be NULL | ';
                END IF;
                IF rec.first_name IS NULL THEN
                    v_error_message := v_error_message || 'First Name should not be NULL | ';
                END IF;
                IF rec.last_name IS NULL THEN
                    v_error_message := v_error_message || 'Last Name should not be NULL | ';
                END IF;
                IF rec.supplier_name IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_suppliers s
                WHERE
                    s.supplier_name = rec.supplier_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_supplier_addresses_ns sa
                WHERE
                    sa.address_name = rec.address_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Contact address not found in Supplier addresses table | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_supplier_addresses_ns sa
                WHERE
                        error_flag = 'Y'
                    AND sa.address_name = rec.address_name;

                IF v_supplier_count > 0 THEN
                    v_error_message := v_error_message || 'Address Record is in Error Status| ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_sup_contacts_ns sc
                WHERE
                        sc.first_name = rec.first_name
                    AND last_name = rec.last_name
                    AND sc.email = rec.email;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Contact details not found in Supplier contacts table | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_sup_contacts_ns sc
                WHERE
                        error_flag = 'Y'
                    AND sc.first_name = rec.first_name
                    AND last_name = rec.last_name
                    AND sc.email = rec.email;

                IF v_supplier_count > 0 THEN
                    v_error_message := v_error_message || 'Contact Record is in Error Status| ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_suppliers_ns s
                WHERE
                        s.error_flag = 'Y'
                    AND s.supplier_name = rec.supplier_name;

                IF v_supplier_count > 0 THEN
                    v_error_message := v_error_message || 'Supplier Record is in Error Status | ';
                END IF;
                IF v_error_message IS NOT NULL THEN
                    UPDATE xxsupcnv.poz_supp_contact_addresses_ns
                    SET
                        error_flag = 'Y',
                        error_message = rtrim(v_error_message, ' | ')
                    WHERE
                            address_name = rec.address_name
                        AND supplier_name = rec.supplier_name;

                    COMMIT;
                END IF;

            END;
            -- Add more validation checks for Supplier Contact Addresses file
        END LOOP;
---------------------------------------------------------------------------------------------------------------
----------------------------------------- Validate Supplier Sites file ----------------------------------------
---------------------------------------------------------------------------------------------------------------        
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.poz_supplier_sites_ns
        ) LOOP
            v_error_message := '';
            BEGIN
                IF rec.address_name IS NULL THEN
                    v_error_message := v_error_message || 'Address Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_supplier_addresses_ns sa
                WHERE
                    sa.address_name = rec.address_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Address Name not found in Supplier address table | ';
                END IF;
                IF rec.attribute1 IS NULL THEN
                    v_error_message := v_error_message || 'Attribute1 should be populated with NetSuite Vendor ID | ';
                END IF;
--                IF NOT regexp_like(rec.email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN
--                    v_error_message := v_error_message || 'Email address is in incorrect format | ';
--                END IF;

                IF
                    rec.email IS NULL
                    AND rec.communication_method = 'Email'
                THEN
                    v_error_message := v_error_message || 'E-Mail should not be NULL when Communication Method is populated as Email | '
                    ;
                END IF;

                IF
                    rec.pay IS NULL
                    AND rec.purchasing IS NULL
                THEN
                    v_error_message := v_error_message || 'One of Purchasing flag , Pay flag should not be NULL | ';
                END IF;

                IF rec.payment_method IS NULL THEN
                    v_error_message := v_error_message || 'Payment Method should not be NULL | ';
                END IF;
                IF rec.payment_terms IS NULL THEN
                    v_error_message := v_error_message || 'Payment Terms should not be NULL | ';
                END IF;
                IF rec.procurement_bu IS NULL THEN
                    v_error_message := v_error_message || 'Procurement BU should not be NULL | ';
                END IF;
                -- IF REC.PURCHASING IS NULL THEN
                --     V_ERROR_MESSAGE := V_ERROR_MESSAGE || 'Purchasing flag should not be NULL | ';
                -- END IF;
                IF rec.supplier_name IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_suppliers s
                WHERE
                    s.supplier_name = rec.supplier_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
                END IF;
                IF rec.supplier_site IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Site should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_suppliers_ns s
                WHERE
                        s.error_flag = 'Y'
                    AND s.supplier_name = rec.supplier_name;

                IF v_supplier_count > 0 THEN
                    v_error_message := v_error_message || 'Supplier Record is in Error Status | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_supplier_addresses_ns sa
                WHERE
                        error_flag = 'Y'
                    AND sa.address_name = rec.address_name;

                IF v_supplier_count > 0 THEN
                    v_error_message := v_error_message || 'Address Record is in Error Status | ';
                END IF;
                IF v_error_message IS NOT NULL THEN
                    UPDATE xxsupcnv.poz_supplier_sites_ns
                    SET
                        error_flag = 'Y',
                        error_message = rtrim(v_error_message, ' | ')
                    WHERE
                            supplier_site = rec.supplier_site
                        AND supplier_name = rec.supplier_name;

                    COMMIT;
                END IF;

            END;

        END LOOP;
---------------------------------------------------------------------------------------------------------------
-------------------------------------  Validate Supplier Assignments file  ------------------------------------
---------------------------------------------------------------------------------------------------------------    
        FOR rec IN (
            SELECT
                *
            FROM
                xxsupcnv.poz_site_assignments_ns
        ) LOOP
            v_error_message := '';
            BEGIN
                IF rec.client_bu IS NULL THEN
                    v_error_message := v_error_message || 'Client BU should not be NULL | ';
                END IF;
                IF rec.procurement_bu IS NULL THEN
                    v_error_message := v_error_message || 'Procurement BU should not be NULL | ';
                END IF;
                IF rec.supplier_name IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Name should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.stg_poz_suppliers s
                WHERE
                    s.supplier_name = rec.supplier_name;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Name not found in Supplier header table | ';
                END IF;
                IF rec.supplier_site IS NULL THEN
                    v_error_message := v_error_message || 'Supplier Site should not be NULL | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_supplier_sites_ns ss
                WHERE
                        ss.supplier_site = rec.supplier_site
                    AND ss.id = rec.id;

                IF v_supplier_count = 0 THEN
                    v_error_message := v_error_message || 'Supplier Site not found in Supplier sites table | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_suppliers_ns s
                WHERE
                        s.error_flag = 'Y'
                    AND s.supplier_name = rec.supplier_name;

                IF v_supplier_count > 0 THEN
                    v_error_message := v_error_message || 'Supplier Record is in Error Status | ';
                END IF;
                v_supplier_count := 0;
                SELECT
                    COUNT(*)
                INTO v_supplier_count
                FROM
                    xxsupcnv.poz_supplier_sites_ns ss
                WHERE
                        error_flag = 'Y'
                    AND ss.supplier_site = rec.supplier_site
                    AND ss.id = rec.id;

                IF v_supplier_count > 0 THEN
                    v_error_message := v_error_message || 'Supplier Site is in Error Status | ';
                END IF;
                IF v_error_message IS NOT NULL THEN
                    UPDATE xxsupcnv.poz_site_assignments_ns
                    SET
                        error_flag = 'Y',
                        error_message = rtrim(v_error_message, ' | ')
                    WHERE
                            supplier_name = rec.supplier_name
                        AND id = rec.id
                        AND supplier_site = rec.supplier_site
                        AND procurement_bu = rec.procurement_bu;

                    COMMIT;
                END IF;

            END;

        END LOOP;

    END revalidate_transformed_data;

    PROCEDURE purge_stg_tables IS
    BEGIN
        DELETE FROM xxsupcnv.stg_poz_suppliers;

        DELETE FROM xxsupcnv.stg_poz_supplier_addresses;

        DELETE FROM xxsupcnv.stg_poz_supplier_sites;

        DELETE FROM xxsupcnv.stg_poz_supp_contact_addresses;

        DELETE FROM xxsupcnv.stg_poz_sup_bus_class;

        DELETE FROM xxsupcnv.stg_poz_sup_contacts;

        DELETE FROM xxsupcnv.stg_poz_site_assignments;

        COMMIT;
    END;

    PROCEDURE purge_ns_tables IS
    BEGIN
        DELETE FROM xxsupcnv.poz_suppliers_ns;

        DELETE FROM xxsupcnv.poz_supplier_addresses_ns;

        DELETE FROM xxsupcnv.poz_supplier_sites_ns;

        DELETE FROM xxsupcnv.poz_supp_contact_addresses_ns;

        DELETE FROM xxsupcnv.poz_sup_bus_class_ns;

        DELETE FROM xxsupcnv.poz_sup_contacts_ns;

        DELETE FROM xxsupcnv.poz_site_assignments_ns;

    END;

END xxksy_supplier_data_conversion_pkg;
/
