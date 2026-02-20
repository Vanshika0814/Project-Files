CREATE OR REPLACE package          xxsupcnv.xxksy_supplier_data_conversion_pkg as
    procedure main;

    procedure validate_data;

    procedure move_to_enrich_data;

--    PROCEDURE INSERT_MISSING_DATA;
    procedure insert_missing_data1;

    procedure revalidate_transformed_data;

    procedure generate_error_report;

    procedure purge_stg_tables;

    procedure purge_ns_tables;

end xxksy_supplier_data_conversion_pkg;

/