CREATE OR REPLACE PROCEDURE "XXSUPCNV"."XXKSY_PROCUREMENT_BU_MAP" is
begin
    for rec in (
        select
            *
        from
            subsidiary_procurement_bu_map
    ) loop
        begin
            insert into poz_supplier_sites_ns (
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
                remittance_email,
                error_flag,
                error_message,
                attribute1
            )
                select
                    id,
                    batch_id,
                    import_action,
                    supplier_name,
                    rec.prc_bu                  procurement_bu,
                    address_name,
                    rec.country_code
                    || substr(supplier_site, 3) supplier_site,
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
                    remittance_email,
                    error_flag,
                    error_message,
                    attribute1
                from
                    poz_supplier_sites_ns
                where
                    id = rec.id;

            commit;
        end;

        begin
            insert into poz_site_assignments_ns (
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
                select
                    id,
                    batch_id,
                    import_action,
                    supplier_name,
                    rec.country_code
                    || substr(supplier_site, 3) supplier_site,
                    rec.prc_bu,
                    rec.prc_bu,
                    rec.prc_bu,
                    ship_to_location,
                    bill_to_location,
                    use_withholding_tax,
                    withholding_tax_group,
                    liability_distribution,
                    prepayment_distribution,
                    bills_payable_distribution,
                    distribution_set
                from
                    poz_site_assignments_ns
                where
                    id = rec.id;

        end;

--        UPDATE poz_site_assignments_ns
--        SET
--            procurement_bu = rec.prc_bu
--        WHERE
--            id = rec.id;

    end loop;

    commit;

--    begin
--    
--    end;

    ----LTCI-4180 - for AU and UK suppliers - Use first two char of Procurement BU to modify site's first 2 char to make it unique----
    update poz_supplier_sites_ns
    set
        supplier_site = substr(procurement_bu, 1, 2)
                        || substr(supplier_site, 3)
    where
        id in (
            select
                id
            from
                subsidiary_procurement_bu_map
        );

    ----LTCI-4180 - Client BU and Bill to BU should be same as Procurement BU for AU and UK suppliers----
    update poz_site_assignments_ns
    set
        client_bu = procurement_bu,
        bill_to_bu = procurement_bu,
        supplier_site = substr(procurement_bu, 1, 2)
                        || substr(supplier_site, 3)
    where
        id in (
            select
                id
            from
                subsidiary_procurement_bu_map
        );

    commit;
end;

/
