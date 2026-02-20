--------------------------------------------------------
--  DDL for View NL_GEOGRAPHY
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."NL_GEOGRAPHY" ("COUNTRY", "COUNTRY_CODE", "PROVINCE", "MUNICIPALITY", "TOWN", "POSTAL_CODE") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  select
        geography_element1      country,
        geography_element1_code country_code,
        geography_element2      province,
        geography_element3      municipality,
        geography_element4      town,
        geography_element5      postal_code
    from
        hz_geographies
    where
        geography_element1_code = 'NL'
;
/