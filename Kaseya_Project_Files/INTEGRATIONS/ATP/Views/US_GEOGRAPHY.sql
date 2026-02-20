--------------------------------------------------------
--  DDL for View US_GEOGRAPHY
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XXSUPCNV"."US_GEOGRAPHY" ("COUNTRY", "COUNTRY_CODE", "STATE", "COUNTY", "CITY", "POSTAL_CODE") DEFAULT COLLATION "USING_NLS_COMP"  AS 
  select distinct
        geography_element1      country,
        geography_element1_code country_code,
        geography_element2      state,
        geography_element3      county,
        geography_element4      city,
        geography_element5      postal_code
    from
        hz_geographies
    where
        geography_element1_code = 'US'
;
/