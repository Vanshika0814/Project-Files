--------------------------------------------------------
--  DDL for View CA_GEOGRAPHY
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."CA_GEOGRAPHY" ("COUNTRY", "COUNTRY_CODE", "PROVINCE", "CITY", "POSTAL_CODE") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  select
        geography_element1      country,
        geography_element1_code country_code,
        geography_element2      province,
        geography_element3      city,
        geography_element4      postal_code
    from
        hz_geographies
    where
        geography_element1_code = 'CA'
;
/