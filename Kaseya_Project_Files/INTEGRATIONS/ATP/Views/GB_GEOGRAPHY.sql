--------------------------------------------------------
--  DDL for View GB_GEOGRAPHY
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."GB_GEOGRAPHY" ("COUNTRY", "COUNTRY_CODE", "COUNTY", "TOWNSHIP", "POSTAL_CODE") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  select distinct
        geography_element1      country,
        geography_element1_code country_code,
        geography_element2      county,
        geography_element3      township,
        geography_element4      postal_code
--    ,    geography_element5 POSTAL_CODE
    from
        hz_geographies
    where
        geography_element1_code = 'GB'
;
/