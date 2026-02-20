CREATE OR REPLACE PROCEDURE "XXSUPCNV"."XXKSY_MERGE_VENDORS" is
begin
    begin
----delete Merge Vendors from Supplier file ----
        delete from poz_suppliers_ns
        where
            supplier_number in (
                select
                    id
                from
                    supplier_conversion_cleaned_data
                where
                        merge_vendor != id
                    and merge_vendor is not null
            );

        commit;
    end;

    for rec in (
        select
            id,
            name,
            merge_vendor,
            (
                select
                    supplier_name
                from
                    stg_poz_suppliers
                where
                    supplier_number = a.merge_vendor
            ) merge_supplier_name
        from
            supplier_conversion_cleaned_data a
        where
                merge_vendor != id
            and merge_vendor is not null
    ) loop
        begin
----update merge vendor number on addresses-----
            update poz_supplier_addresses_ns
            set
                id = rec.merge_vendor,
                supplier_name = rec.merge_supplier_name,
                address_name = rec.id
                               || '-'
                               || trim(replace(address_name, rec.id, '')),
                address_1 = rec.id
                            || '-'
                            || trim(replace(address_1, rec.id, '')),
                remittance_email = rec.id
                                   || '-'
                                   || trim(replace(remittance_email, rec.id, 'xxx')),
                email = rec.id
                        || '-'
                        || trim(replace(email, rec.id, 'xxx'))
            where
                id = rec.id;

----update merge vendor number on Sites-----
            update poz_supplier_sites_ns
            set
                id = rec.merge_vendor,
                supplier_name = rec.merge_supplier_name,
                supplier_site = rec.id
                                || '-'
                                || supplier_site,
                address_name = rec.id
                               || '-'
                               || trim(replace(address_name, rec.id, '')),
                remittance_email = rec.id
                                   || '-'
                                   || trim(replace(remittance_email, rec.id, 'xxx')),
                email = rec.id
                        || '-'
                        || trim(replace(email, rec.id, 'xxx')),
                income_tax_reporting_site = null
            where
                id = rec.id;

----update merge vendor number on Contacts-----
            update poz_sup_contacts_ns
            set
                id = rec.merge_vendor,
                supplier_name = rec.merge_supplier_name,
                email = rec.id
                        || '-'
                        || trim(replace(email, rec.id, 'xxx')),
                first_name = rec.id
                             || '-'
                             || trim(replace(first_name, rec.id, ''))
            where
                id = rec.id;

----update merge vendor number on Site Assignments-----
            update poz_site_assignments_ns
            set
                id = rec.merge_vendor,
                supplier_name = rec.merge_supplier_name,
                supplier_site = rec.id
                                || '-'
                                || supplier_site
            where
                id = rec.id;

----update merge vendor number on Contact Addresses-----
            update poz_supp_contact_addresses_ns
            set
                id = rec.merge_vendor,
                supplier_name = rec.merge_supplier_name,
                email = rec.id
                        || '-'
                        || trim(replace(email, rec.id, 'xxx')),
                first_name = rec.id
                             || '-'
                             || trim(replace(first_name, rec.id, '')),
                address_name = rec.id
                               || '-'
                               || trim(replace(address_name, rec.id, ''))
            where
                id = rec.id;

----update merge vendor number on Business Classification-----PROVIDED_BY_FIRST_NAME
            delete from poz_sup_bus_class_ns
            where
                id = rec.id;
--            UPDATE poz_sup_bus_class_ns
--            SET
--                id = rec.merge_vendor,
--                supplier_name = rec.merge_supplier_name,
--                PROVIDED_BY_E_MAIL = rec.id
--                               || '-'
--                               || TRIM(replace(PROVIDED_BY_E_MAIL, rec.id, '')),
--                PROVIDED_BY_First_name = rec.id
--                               || '-'
--                               || TRIM(replace(PROVIDED_BY_First_name, rec.id, ''))
--            WHERE
--                id = rec.id;

        end;

        commit;
    end loop;

end;

/
